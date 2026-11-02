package db

import (
	"encoding/binary"
	"errors"
	"slices"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// Columns of the header and range tombstone rows that are not table
// columns. A partition or range deletion is stored as a live cell (never
// purged, reconciled by timestamp then by value, the local deletion time)
// rather than as a row deletion, because what it shadows lives under other
// keys: purging it could resurrect them.
const (
	deletionColumn = 0xFFFFFFFF
	rtStartColumn  = 0xFFFFFFFE
	rtEndColumn    = 0xFFFFFFFD
)

type modKind uint8

const (
	insertKind modKind = iota
	updateKind
	deleteKind
)

// modification is a prepared INSERT, UPDATE or DELETE
// (ModificationStatement).
type modification struct {
	t         *Table
	kind      modKind
	where     *where
	ops       []setOp
	timestamp term
	ttl       term
	pkIndices []uint16
	// staticOnly: every operation is on static columns, so the clustering
	// may be left unrestricted.
	staticOnly bool
	cond       *condition
}

type opKind uint8

const (
	opSet         opKind = iota // c = v
	opAppend                    // c = c + v (sets, maps, lists)
	opRemove                    // c = c - v (sets, maps, lists)
	opPrepend                   // l = v + l
	opSetKey                    // m[k] = v, l[i] = v
	opSetField                  // u.f = v
	opCounter                   // c = c + n, c = c - n
	opDelete                    // DELETE c
	opDeleteKey                 // DELETE m[k], DELETE l[i]
	opDeleteField               // DELETE u.f
)

// setOp is an operation on a column (Operation and its subclasses in
// Lists, Sets, Maps, UserTypes, Constants).
type setOp struct {
	col   *Column
	kind  opKind
	t     term // the value (opSet, opAppend, ...) or the counter delta
	key   term // opSetKey, opDeleteKey
	field int  // opSetField, opDeleteField
	neg   bool // counter decrement
}

// readsBefore reports whether the operation reads the current value
// (list index and removal operations, counters).
func (o *setOp) readsBefore() bool {
	switch o.kind {
	case opCounter:
		return true
	case opSetKey, opDeleteKey, opRemove:
		return o.col.Type.Kind == types.List
	}
	return false
}

func (d *DB) prepareModification(s *Schema, session string, raw cql.Statement, vs *vars) (*modification, error) {
	var ksName, tName string
	var using cql.Using
	switch st := raw.(type) {
	case *cql.Insert:
		ksName, tName, using = st.Keyspace, st.Table, st.Using
	case *cql.Update:
		ksName, tName, using = st.Keyspace, st.Table, st.Using
	case *cql.Delete:
		ksName, tName, using = st.Keyspace, st.Table, st.Using
	}
	ks, err := keyspaceOf(ksName, session)
	if err != nil {
		return nil, err
	}
	t, err := s.table(ks, tName)
	if err != nil {
		return nil, err
	}
	if t.virtual != nil {
		return nil, invalidf("Modification is not supported on table %s.%s", ks, tName)
	}
	vs.ks, vs.table = ks, tName
	m := &modification{t: t}
	if using.Timestamp != nil {
		if m.timestamp, err = vs.prepareTerm(using.Timestamp, "[timestamp]", types.BigintType); err != nil {
			return nil, err
		}
	}
	if using.TTL != nil {
		if m.ttl, err = vs.prepareTerm(using.TTL, "[ttl]", types.IntType); err != nil {
			return nil, err
		}
	}
	var rels []cql.Relation
	switch st := raw.(type) {
	case *cql.Insert:
		m.kind = insertKind
		if st.JSON != nil {
			if err := m.prepareJSON(st, vs); err != nil {
				return nil, err
			}
			if st.IfNotExists {
				m.cond = &condition{ifNotExists: true}
			}
			break
		}
		if len(st.Columns) != len(st.Values) {
			return nil, invalidf("Unmatched column names/values")
		}
		seen := map[string]bool{}
		for i, name := range st.Columns {
			c := t.byName[name]
			if c == nil {
				return nil, invalidf("Undefined column name %s in table %s.%s", name, ks, tName)
			}
			if seen[name] {
				return nil, invalidf("Multiple definitions found for column %s", name)
			}
			seen[name] = true
			if c.Kind == PartitionKey || c.Kind == Clustering {
				rels = append(rels, cql.Relation{Columns: []string{name}, Op: "=", Value: st.Values[i]})
				continue
			}
			tt, err := vs.prepareTerm(st.Values[i], c.Name, c.Type)
			if err != nil {
				return nil, err
			}
			m.ops = append(m.ops, setOp{col: c, kind: opSet, t: tt})
		}
		if t.IsCounter() {
			return nil, invalidf("INSERT statements are not allowed on counter tables, use UPDATE instead")
		}
		for _, c := range append(slices.Clone(t.PartitionKey), t.Clustering...) {
			if !seen[c.Name] {
				what := "Some partition key parts are missing: "
				if c.Kind == Clustering {
					what = "Some clustering keys are missing: "
				}
				return nil, invalidf("%s%s", what, c.Name)
			}
		}
		if st.IfNotExists {
			m.cond = &condition{ifNotExists: true}
		}
	case *cql.Update:
		m.kind = updateKind
		for _, op := range st.Operations {
			c := t.byName[op.Column]
			if c == nil {
				return nil, invalidf("Undefined column name %s in table %s.%s", op.Column, ks, tName)
			}
			if c.Kind == PartitionKey || c.Kind == Clustering {
				return nil, invalidf("PRIMARY KEY part %s found in SET part", c.Name)
			}
			o, err := prepareOperation(c, op, vs)
			if err != nil {
				return nil, err
			}
			m.ops = append(m.ops, o)
		}
		if t.IsCounter() && m.ttl != nil {
			return nil, invalidf("Cannot provide custom TTL for counter updates")
		}
		rels = st.Where
		if st.IfExists || len(st.Conditions) > 0 {
			if m.cond, err = prepareConditions(t, st.IfExists, st.Conditions, vs); err != nil {
				return nil, err
			}
		}
	case *cql.Delete:
		m.kind = deleteKind
		for _, del := range st.Columns {
			c := t.byName[del.Column]
			if c == nil {
				return nil, invalidf("Undefined column name %s in table %s.%s", del.Column, ks, tName)
			}
			if c.Kind == PartitionKey || c.Kind == Clustering {
				return nil, invalidf("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", c.Name)
			}
			o, err := prepareDeletion(c, del, vs)
			if err != nil {
				return nil, err
			}
			m.ops = append(m.ops, o)
		}
		rels = st.Where
		if m.ttl != nil {
			return nil, invalidf("Cannot provide custom TTL for DELETE")
		}
		if st.IfExists || len(st.Conditions) > 0 {
			if m.cond, err = prepareConditions(t, st.IfExists, st.Conditions, vs); err != nil {
				return nil, err
			}
		}
	}
	if m.where == nil {
		if m.where, err = analyzeWhere(t, rels, vs); err != nil {
			return nil, err
		}
	}
	w := m.where
	if w.token != nil {
		return nil, invalidf("Token relations are not allowed in modification statements")
	}
	if len(w.filters) > 0 && w.filters[0].tuple != nil {
		return nil, invalidf("Multi-column slices on clustering columns of mixed order are not supported in modification statements")
	}
	if len(w.filters) > 0 {
		var names []string
		for _, f := range w.filters {
			names = append(names, f.col.Name)
		}
		if w.filters[0].col.Kind == Clustering {
			return nil, invalidf("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted",
				w.filters[0].col.Name, t.Clustering[len(w.ck)].Name)
		}
		return nil, invalidf("Non PRIMARY KEY columns found in where clause: %s ", strings.Join(names, ", "))
	}
	for _, c := range t.PartitionKey {
		if w.pk[c.Position] == nil {
			return nil, invalidf("Some partition key parts are missing: %s", c.Name)
		}
	}
	m.staticOnly = len(m.ops) > 0
	for _, op := range m.ops {
		m.staticOnly = m.staticOnly && op.col.Kind == Static
	}
	fullClustering := len(w.ck) == len(t.Clustering)
	switch {
	case m.kind != deleteKind && w.slice != nil:
		return nil, invalidf("Slice restrictions are not supported on the clustering columns in UPDATE statements")
	case m.kind == deleteKind && len(m.ops) == 0:
		// whole rows, a range, or the whole partition
	case !fullClustering && !(m.staticOnly && len(w.ck) == 0 && w.slice == nil):
		if m.kind == deleteKind && w.slice != nil {
			return nil, invalidf("Range deletions are not supported for specific columns")
		}
		missing := t.Clustering[len(w.ck)].Name
		return nil, invalidf("Some clustering keys are missing: %s", missing)
	}
	if m.staticOnly && len(w.ck) > 0 && m.kind != insertKind {
		return nil, invalidf("Invalid restrictions on clustering columns since the %s statement modifies only static columns", map[modKind]string{updateKind: "UPDATE", deleteKind: "DELETE"}[m.kind])
	}
	if m.cond != nil && m.cond.ifNotExists && m.kind != insertKind {
		return nil, invalidf("IF NOT EXISTS is only supported for INSERT")
	}
	if m.cond != nil {
		m.cond.read = &selectStmt{t: t, where: m.where}
		for _, col := range t.AllColumns() {
			m.cond.read.sels = append(m.cond.read.sels, selector{kind: selColumn, name: col.Name, typ: col.Type, col: col})
		}
	}
	m.pkIndices = pkIndices(t, vs)
	return m, nil
}

func operationString(op cql.Operation) string {
	switch op.Op {
	case "+", "-":
		return op.Column + " = " + op.Column + " " + op.Op + " <value>"
	case "prepend":
		return op.Column + " = <value> + " + op.Column
	case "+=", "-=":
		return op.Column + " " + op.Op + " <value>"
	}
	if op.Key != nil {
		return op.Column + "[<key>] = <value>"
	}
	return op.Column + "." + op.Field + " = <value>"
}

// pkIndices returns the bind markers of the partition key columns, if each
// is bound by one marker (the PK indices of the PREPARED result, which
// drivers use for token-aware routing).
func pkIndices(t *Table, vs *vars) []uint16 {
	var out []uint16
	for _, c := range t.PartitionKey {
		found := false
		for i, sp := range vs.specs {
			if sp.name == c.Name && sp.typ != nil && sp.typ.Equal(c.Type) {
				out = append(out, uint16(i))
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return out
}

func (m *modification) execute(e *execCtx) (message.Message, error) {
	if m.cond != nil {
		return m.executeConditional(e)
	}
	if err := e.d.applyModifications([]boundModification{{m: m, e: e}}, e); err != nil {
		return nil, err
	}
	return &message.VoidResult{}, nil
}

type boundModification struct {
	m *modification
	e *execCtx
}

// applyModifications applies statements' mutations in one atomic batch.
func (d *DB) applyModifications(mods []boundModification, batchCtx *execCtx) error {
	for _, bm := range mods {
		if slices.ContainsFunc(bm.m.ops, func(o setOp) bool { return o.kind == opCounter }) {
			d.counterMu.Lock()
			defer d.counterMu.Unlock()
			if len(mods) > 1 {
				// Counter batches are not atomic (they are UNLOGGED): apply
				// each statement in turn, so that each reads the counters the
				// previous ones updated.
				batchCtx.timestamp()
				for _, bm := range mods {
					bm.e.ts = batchCtx.ts
					if err := d.applyLocked([]boundModification{bm}); err != nil {
						return err
					}
				}
				return nil
			}
			break
		}
	}
	return d.applyLocked(mods)
}

// applyLocked builds and applies mutations (holding counterMu if they
// update counters).
func (d *DB) applyLocked(mods []boundModification) error {
	batchCtx := mods[0].e
	b := lsm.NewBatch()
	if len(mods) > 1 {
		batchCtx.timestamp() // one timestamp for the whole batch
		for i := range mods {
			mods[i].e.ts = batchCtx.ts
		}
	}
	for _, bm := range mods {
		if err := bm.m.addMutations(bm.e, b); err != nil {
			return err
		}
	}
	if b.Count() == 0 {
		return nil
	}
	d.tableMu.RLock()
	defer d.tableMu.RUnlock()
	if err := d.engine.Apply(b, d.wo); err != nil {
		if strings.Contains(err.Error(), lsm.ErrUnknownTable.Error()) {
			return invalidf("table %s.%s was dropped", mods[0].m.t.Keyspace, mods[0].m.t.Name)
		}
		return err
	}
	return nil
}

// writeParams binds USING TIMESTAMP and TTL.
func (m *modification) writeParams(e *execCtx) (ts int64, ttl int64, err error) {
	ts, ok, err := bindInt(e, m.timestamp, "timestamp")
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		ts = e.timestamp()
	} else if ts == -1<<63 {
		return 0, 0, invalidf("Invalid timestamp value")
	}
	ttl, ok, err = bindInt(e, m.ttl, "TTL")
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		ttl = int64(m.t.Params.DefaultTTL)
	}
	if ttl < 0 {
		return 0, 0, invalidf("A TTL must be greater or equal to 0, but was %d", ttl)
	}
	if ttl > maxTTL {
		return 0, 0, invalidf("ttl is too large. requested (%d) maximum (%d)", ttl, maxTTL)
	}
	return ts, ttl, nil
}

func (m *modification) addMutations(e *execCtx, b *lsm.Batch) error {
	t := m.t
	ts, ttl, err := m.writeParams(e)
	if err != nil {
		return err
	}
	pks, err := m.where.partitionKeys(e, t)
	if err != nil {
		return err
	}
	needRead := false
	for i := range m.ops {
		needRead = needRead || m.ops[i].readsBefore()
	}
	var cells, statics []row.Cell
	if !needRead {
		if cells, statics, err = m.buildCells(e, ts, ttl, nil); err != nil {
			return err
		}
	}

	var ranges []keyRange
	if len(t.Clustering) > 0 && !(m.staticOnly && len(m.where.ck) == 0) {
		if ranges, err = m.where.clusteringRanges(e, t); err != nil {
			return err
		}
	}
	deletion := row.Deletion{MarkedForDeleteAt: row.NoTimestamp}
	wholeRows := m.kind == deleteKind && len(m.ops) == 0
	if wholeRows {
		deletion = row.Deletion{MarkedForDeleteAt: ts, LocalDeletionTime: e.nowSec}
	}
	size := 16
	for _, c := range cells {
		size += len(c.Value) + len(c.Path) + 24
	}
	for _, c := range statics {
		size += len(c.Value) + len(c.Path) + 24
	}
	buf := make([]byte, 0, size)
	// rowCells builds the cells of one row (and the partition's statics),
	// reading the current values first if an operation needs them.
	rowCells := func(p, key []byte) ([]row.Cell, []row.Cell, error) {
		if !needRead {
			return cells, statics, nil
		}
		cur, err := e.d.readCurrent(e, t, p, key)
		if err != nil {
			return nil, nil, err
		}
		return m.buildCells(e, ts, ttl, cur)
	}
	put := func(key []byte, r *row.Row) {
		buf = row.Append(buf[:0], r)
		b.Put(t.Storage, key, buf)
		if len(t.Indexes) > 0 && len(r.Cells) > 0 && (len(t.Clustering) == 0 || key[len(key)-1] == types.Terminator) {
			if err := t.addEntries(b, key, r.Cells, e.nowSec); err != nil {
				panic(err) // values were validated
			}
		}
	}
	for _, pk := range pks {
		p := partitionPrefix(pk)
		if needRead {
			key := p
			if len(ranges) > 0 {
				key = append(slices.Clip(p), ranges[0].start...)
			} else if len(t.Clustering) > 0 {
				key = append(slices.Clip(p), headerSuffix)
			}
			if cells, statics, err = rowCells(p, key); err != nil {
				return err
			}
		}
		if len(statics) > 0 {
			put(append(slices.Clip(p), headerSuffix), &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: statics})
		}
		switch {
		case len(t.Clustering) == 0:
			if len(cells) > 0 || wholeRows {
				put(p, &row.Row{Deletion: deletion, Cells: cells})
			}
		case wholeRows && len(m.where.ck) == 0 && m.where.slice == nil:
			// Partition deletion.
			put(append(slices.Clip(p), headerSuffix), &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
				Cells: []row.Cell{deletionCell(ts, e.nowSec)}})
		case wholeRows:
			for _, r := range ranges {
				if r.point {
					put(append(slices.Clip(p), r.start...), &row.Row{Deletion: deletion})
					continue
				}
				// Range tombstone: the bounds are both in the key (unique
				// per range) and in cells, to read back.
				key := append(slices.Clip(p), tombstoneSuffix)
				key = binary.AppendUvarint(key, uint64(len(r.start)))
				key = append(key, r.start...)
				key = append(key, r.end...)
				put(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{
					{Column: rtEndColumn, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: r.end},
					{Column: rtStartColumn, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: r.start},
					deletionCell(ts, e.nowSec),
				}})
			}
		default:
			for i, r := range ranges {
				key := append(slices.Clip(p), r.start...)
				if needRead && i > 0 {
					if cells, _, err = rowCells(p, key); err != nil {
						return err
					}
				}
				if len(cells) > 0 {
					put(key, &row.Row{Deletion: deletion, Cells: cells})
				}
			}
		}
	}
	return nil
}

// current is a row's live cells before a write, for operations that read
// before writing.
type current struct {
	cells, statics []row.Cell
}

func (c *current) of(col *Column) []row.Cell {
	if c == nil {
		return nil
	}
	if col.Kind == Static {
		return columnCells(c.statics, col.ID)
	}
	return columnCells(c.cells, col.ID)
}

// readCurrent reads a row's live cells and its partition's static cells,
// applying partition and range deletions.
func (d *DB) readCurrent(e *execCtx, t *Table, p, key []byte) (*current, error) {
	d.tableMu.RLock()
	defer d.tableMu.RUnlock()
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return nil, invalidf("table %s.%s was dropped", t.Keyspace, t.Name)
	}
	cur := &current{}
	dels := deletions{partition: row.NoTimestamp}
	if len(t.Clustering) > 0 {
		it, err := lt.NewIter(append(slices.Clip(p), headerSuffix), append(slices.Clip(p), tombstoneSuffix+1))
		if err != nil {
			return nil, err
		}
		for ok := it.First(); ok; ok = it.Next() {
			r, err := row.Decode(it.Value())
			if err != nil {
				it.Close()
				return nil, err
			}
			suffix := it.Key()[len(p):]
			if suffix[0] == headerSuffix {
				for _, c := range r.Cells {
					if c.Column == deletionColumn {
						dels.partition = max(dels.partition, c.Timestamp)
					} else if c.Column < rtEndColumn {
						cur.statics = append(cur.statics, c)
					}
				}
				continue
			}
			var rt rangeTombstone
			for _, c := range r.Cells {
				switch c.Column {
				case rtStartColumn:
					rt.start = slices.Clone(c.Value)
				case rtEndColumn:
					rt.end = slices.Clone(c.Value)
				case deletionColumn:
					rt.ts = c.Timestamp
				}
			}
			dels.ranges = append(dels.ranges, rt)
		}
		err = it.Error()
		it.Close()
		if err != nil {
			return nil, err
		}
		cur.statics = liveCells(cloneCells(cur.statics), dels.partition, e.nowSec)
	}
	if len(key) > len(p) && key[len(p)] == headerSuffix {
		return cur, nil
	}
	v, err := lt.Get(key)
	if errors.Is(err, lsm.ErrNotFound) {
		return cur, nil
	}
	if err != nil {
		return nil, err
	}
	r, err := row.Decode(v)
	if err != nil {
		return nil, err
	}
	delTS := r.Deletion.MarkedForDeleteAt
	if len(t.Clustering) > 0 {
		delTS = max(delTS, dels.deletedAt(key[len(p):]))
	}
	cur.cells = liveCells(cloneCells(r.Cells), delTS, e.nowSec)
	return cur, nil
}

func cloneCells(cells []row.Cell) []row.Cell {
	out := make([]row.Cell, len(cells))
	for i, c := range cells {
		c.Value, c.Path = slices.Clone(c.Value), slices.Clone(c.Path)
		out[i] = c
	}
	return out
}

// buildCells returns the cells the operations write, split into regular
// and static ones (each Operation.execute).
func (m *modification) buildCells(e *execCtx, ts, ttl int64, cur *current) (cells, statics []row.Cell, err error) {
	add := func(c *Column, out row.Cell) {
		if c.Kind == Static {
			statics = append(statics, out)
		} else {
			cells = append(cells, out)
		}
	}
	live := func(c *Column, path, v []byte) row.Cell {
		out := row.Cell{Column: c.ID, Path: path, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: v}
		if ttl > 0 {
			out.TTL, out.LocalDeletionTime = uint32(ttl), e.nowSec+ttl
		}
		return out
	}
	tomb := func(c *Column, path []byte) row.Cell {
		return row.Cell{Column: c.ID, Path: path, Timestamp: ts, LocalDeletionTime: e.nowSec}
	}
	complexDeletion := func(c *Column, at int64) row.Cell {
		return row.Cell{Column: c.ID, ComplexDeletion: true, Timestamp: at, LocalDeletionTime: e.nowSec}
	}
	listCell := func(c *Column, idx int64) (*row.Cell, error) {
		elems := cur.of(c)
		if idx < 0 || idx >= int64(len(elems)) {
			return nil, invalidf("List index %d out of bound, list has size %d", idx, len(elems))
		}
		return &elems[idx], nil
	}
	for i := range m.ops {
		op := &m.ops[i]
		c := op.col
		var v []byte
		var st valState
		if op.t != nil {
			if v, st, err = op.t.bind(e); err != nil {
				return nil, nil, err
			}
		}
		multi := c.Type.IsMultiCell()
		switch op.kind {
		case opSet:
			switch {
			case st == vUnset:
			case !multi && st == vNull:
				add(c, tomb(c, nil))
			case !multi:
				add(c, live(c, nil, v))
			default:
				add(c, complexDeletion(c, ts-1))
				if st == vSet {
					els, err := e.d.elementCells(c, v, ts, false, func(p, x []byte) row.Cell { return live(c, p, x) })
					if err != nil {
						return nil, nil, err
					}
					for _, el := range els {
						add(c, el)
					}
				}
			}
		case opAppend, opPrepend:
			if st != vSet {
				continue
			}
			els, err := e.d.elementCells(c, v, ts, op.kind == opPrepend, func(p, x []byte) row.Cell { return live(c, p, x) })
			if err != nil {
				return nil, nil, err
			}
			for _, el := range els {
				add(c, el)
			}
		case opRemove:
			if st != vSet {
				continue
			}
			elems, err := types.SplitCollection(v, 1)
			if err != nil {
				return nil, nil, invalidf("Invalid value for %s", c.Name)
			}
			switch c.Type.Kind {
			case types.List:
				for _, cell := range cur.of(c) {
					for _, x := range elems {
						if types.Compare(c.Type.Elems[0], cell.Value, x) == 0 {
							add(c, tomb(c, cell.Path))
							break
						}
					}
				}
			default: // set elements, or map keys
				for _, x := range elems {
					p, err := elementPath(c.Type.Elems[0], x)
					if err != nil {
						return nil, nil, err
					}
					add(c, tomb(c, p))
				}
			}
		case opSetKey, opDeleteKey:
			k, kst, err := op.key.bind(e)
			if err != nil {
				return nil, nil, err
			}
			if kst != vSet {
				return nil, nil, invalidf("Invalid null value for %s", map[types.Kind]string{types.Map: "map key", types.List: "list index"}[c.Type.Kind])
			}
			var path []byte
			if c.Type.Kind == types.List {
				lc, err := listCell(c, int64(int32(binary.BigEndian.Uint32(k))))
				if err != nil {
					return nil, nil, err
				}
				path = lc.Path
			} else if path, err = elementPath(c.Type.Elems[0], k); err != nil {
				return nil, nil, err
			}
			switch {
			case op.kind == opDeleteKey || st == vNull:
				add(c, tomb(c, path))
			case st == vSet:
				add(c, live(c, path, v))
			}
		case opSetField:
			switch st {
			case vNull:
				add(c, tomb(c, fieldPath(op.field)))
			case vSet:
				add(c, live(c, fieldPath(op.field), v))
			}
		case opDeleteField:
			add(c, tomb(c, fieldPath(op.field)))
		case opDelete:
			if multi {
				add(c, complexDeletion(c, ts))
			} else {
				add(c, tomb(c, nil))
			}
		case opCounter:
			if st != vSet {
				return nil, nil, invalidf("Invalid null value for counter increment")
			}
			delta := int64(binary.BigEndian.Uint64(v))
			if op.neg {
				delta = -delta
			}
			var clock, count int64
			if cc := cur.of(c); len(cc) > 0 {
				clock, count, _ = row.CounterShard(cc[0].Value, e.d.hostID)
			}
			add(c, row.Cell{Column: c.ID, Counter: true, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime,
				Value: row.CounterContext(e.d.hostID, clock+1, count+delta)})
		}
	}
	if m.kind == insertKind {
		marker := row.Cell{Column: 0, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime}
		if ttl > 0 {
			marker.TTL, marker.LocalDeletionTime = uint32(ttl), e.nowSec+ttl
		}
		cells = append(cells, marker)
	}
	sortCells(cells)
	sortCells(statics)
	return cells, statics, nil
}

// prepareOperation prepares a SET item of an UPDATE (Operation.Raw.prepare).
func prepareOperation(c *Column, op cql.Operation, vs *vars) (setOp, error) {
	t := c.Type
	o := setOp{col: c}
	invalid := func(what string) (setOp, error) {
		return o, invalidf("Invalid operation (%s) for %s column %s", operationString(op), what, c.Name)
	}
	frozenOrSimple := !t.IsMultiCell()
	var err error
	switch {
	case op.Key != nil:
		if frozenOrSimple || (t.Kind != types.Map && t.Kind != types.List) {
			if t.IsCollection() {
				return invalid("frozen collection")
			}
			return invalid("non list/map")
		}
		o.kind = opSetKey
		kt, vt := types.IntType, t.Elems[0]
		if t.Kind == types.Map {
			kt, vt = t.Elems[0], t.Elems[1]
		}
		if o.key, err = vs.prepareTerm(op.Key, "key("+c.Name+")", kt); err != nil {
			return o, err
		}
		o.t, err = vs.prepareTerm(op.Value, "value("+c.Name+")", vt)
		return o, err
	case op.Field != "":
		if t.Kind != types.UDT {
			return invalid("non-UDT")
		}
		if frozenOrSimple {
			return o, invalidf("Invalid operation (%s) for frozen UDT column %s", operationString(op), c.Name)
		}
		i := slices.Index(t.Fields, op.Field)
		if i < 0 {
			return o, invalidf("UDT column %s does not have a field named %s", c.Name, op.Field)
		}
		o.kind, o.field = opSetField, i
		o.t, err = vs.prepareTerm(op.Value, op.Field, t.Elems[i])
		return o, err
	}
	switch op.Op {
	case "=":
		if t.Kind == types.Counter {
			return o, invalidf("Cannot set the value of counter column %s (counters can only be incremented/decremented, not set)", c.Name)
		}
		o.kind = opSet
		o.t, err = vs.prepareTerm(op.Value, c.Name, t)
		return o, err
	case "+", "+=", "-", "-=":
		add := op.Op[0] == '+'
		if t.Kind == types.Counter {
			o.kind, o.neg = opCounter, !add
			o.t, err = vs.prepareTerm(op.Value, c.Name, types.BigintType)
			return o, err
		}
		if frozenOrSimple || t.Kind == types.UDT {
			if t.IsCollection() {
				return invalid("frozen collection")
			}
			return invalid("non counter")
		}
		vt := t
		if !add && t.Kind == types.Map {
			vt = types.SetOf(t.Elems[0], true)
		}
		o.kind = opAppend
		if !add {
			o.kind = opRemove
			if t.Kind == types.Map && op.Value != nil {
				// m = m - {k1, k2} takes a set of keys.
				vt = types.SetOf(t.Elems[0], true)
			}
		}
		o.t, err = vs.prepareTerm(op.Value, c.Name, vt)
		return o, err
	case "prepend":
		if t.Kind != types.List || frozenOrSimple {
			return invalid("non list")
		}
		o.kind = opPrepend
		o.t, err = vs.prepareTerm(op.Value, c.Name, t)
		return o, err
	}
	return invalid("this")
}

// prepareDeletion prepares one column of a DELETE.
func prepareDeletion(c *Column, del cql.Deletion, vs *vars) (setOp, error) {
	t := c.Type
	o := setOp{col: c, kind: opDelete}
	var err error
	switch {
	case del.Key != nil:
		if !t.IsMultiCell() || (t.Kind != types.Map && t.Kind != types.List) {
			return o, invalidf("Invalid deletion operation for non list/map or frozen column %s", c.Name)
		}
		kt := types.IntType
		if t.Kind == types.Map {
			kt = t.Elems[0]
		}
		o.kind = opDeleteKey
		o.key, err = vs.prepareTerm(del.Key, "key("+c.Name+")", kt)
	case del.Field != "":
		if t.Kind != types.UDT || !t.IsMultiCell() {
			return o, invalidf("Invalid deletion operation for non-UDT or frozen column %s", c.Name)
		}
		o.kind, o.field = opDeleteField, slices.Index(t.Fields, del.Field)
		if o.field < 0 {
			return o, invalidf("UDT column %s does not have a field named %s", c.Name, del.Field)
		}
	}
	return o, err
}

func deletionCell(ts, nowSec int64) row.Cell {
	return row.Cell{Column: deletionColumn, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime,
		Value: binary.BigEndian.AppendUint64(nil, uint64(nowSec))}
}

// batchStmt is a prepared BATCH (BatchStatement).
type batchStmt struct {
	mods        []*modification
	conditional bool
}

func (d *DB) prepareBatch(s *Schema, session string, st *cql.Batch, vs *vars) (*batchStmt, error) {
	b := &batchStmt{}
	for _, raw := range st.Statements {
		m, err := d.prepareModification(s, session, raw, vs)
		if err != nil {
			return nil, err
		}
		if m.cond != nil {
			b.conditional = true
		}
		switch {
		case m.t.IsCounter() && st.Type == "LOGGED":
			return nil, invalidf("Cannot include a counter statement in a logged batch")
		case !m.t.IsCounter() && st.Type == "COUNTER":
			return nil, invalidf("Cannot include non-counter statement in a counter batch")
		}
		if st.Using.Timestamp != nil {
			if m.timestamp != nil {
				return nil, invalidf("Timestamp must be set either on BATCH or individual statements")
			}
			if m.timestamp, err = vs.prepareTerm(st.Using.Timestamp, "[timestamp]", types.BigintType); err != nil {
				return nil, err
			}
		}
		if st.Using.TTL != nil {
			return nil, invalidf("Global TTL on the BATCH statement is not supported.")
		}
		b.mods = append(b.mods, m)
	}
	return b, nil
}

func (b *batchStmt) execute(e *execCtx) (message.Message, error) {
	mods := make([]boundModification, len(b.mods))
	for i, m := range b.mods {
		child := *e
		mods[i] = boundModification{m: m, e: &child}
	}
	if b.conditional {
		return e.d.executeCAS(mods, e)
	}
	if err := e.d.applyModifications(mods, e); err != nil {
		return nil, err
	}
	return &message.VoidResult{}, nil
}

// prepareJSON prepares INSERT JSON (Json.Raw): every column is bound from
// the JSON object at execution; columns it omits are null (DEFAULT NULL)
// or left unchanged (DEFAULT UNSET).
func (m *modification) prepareJSON(st *cql.Insert, vs *vars) error {
	t := m.t
	src := &jsonSource{table: t, unset: st.DefaultUnset}
	var err error
	if src.t, err = vs.prepareTerm(st.JSON, "[json]", types.TextType); err != nil {
		return err
	}
	m.where = &where{pk: make([]*valuesTerm, len(t.PartitionKey))}
	for i, c := range t.PartitionKey {
		m.where.pk[i] = &valuesTerm{col: c, eq: &jsonField{src: src, col: c}}
	}
	for _, c := range t.Clustering {
		m.where.ck = append(m.where.ck, &valuesTerm{col: c, eq: &jsonField{src: src, col: c}})
	}
	for _, c := range t.Columns {
		m.ops = append(m.ops, setOp{col: c, kind: opSet, t: &jsonField{src: src, col: c}})
	}
	return nil
}

// jsonSource is the JSON object of an INSERT JSON, decoded once per
// execution.
type jsonSource struct {
	table *Table
	t     term
	unset bool
}

func (j *jsonSource) object(e *execCtx) (map[string]any, error) {
	if obj, ok := e.json[j]; ok {
		return obj, nil
	}
	v, st, err := j.t.bind(e)
	if err != nil {
		return nil, err
	}
	if st != vSet {
		return nil, invalidf("Got null for INSERT JSON values")
	}
	parsed, err := types.ParseJSON(string(v))
	if err != nil {
		return nil, invalidf("%v", err)
	}
	raw, ok := parsed.(map[string]any)
	if !ok {
		return nil, invalidf("Could not decode JSON string as a map: %s", v)
	}
	obj := map[string]any{}
	for k, x := range raw {
		name := types.UnquoteJSONName(k)
		if j.table.byName[name] == nil {
			return nil, invalidf("JSON values map contains unrecognized column: %s", k)
		}
		obj[name] = x
	}
	if e.json == nil {
		e.json = map[*jsonSource]map[string]any{}
	}
	e.json[j] = obj
	return obj, nil
}

type jsonField struct {
	src *jsonSource
	col *Column
}

func (f *jsonField) bind(e *execCtx) ([]byte, valState, error) {
	obj, err := f.src.object(e)
	if err != nil {
		return nil, vSet, err
	}
	x, ok := obj[f.col.Name]
	if !ok {
		if f.src.unset {
			return nil, vUnset, nil
		}
		return nil, vNull, nil
	}
	v, err := types.FromJSON(f.col.Type, x)
	if err != nil {
		return nil, vSet, invalidf("Error decoding JSON value for %s: %v", f.col.Name, err)
	}
	if v == nil {
		return nil, vNull, nil
	}
	return v, vSet, nil
}
