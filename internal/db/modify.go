package db

import (
	"encoding/binary"
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
	dels      []*Column
	timestamp term
	ttl       term
	pkIndices []uint16
	// staticOnly: every operation is on static columns, so the clustering
	// may be left unrestricted.
	staticOnly bool
	cond       *condition
}

type setOp struct {
	col *Column
	t   term
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
			return nil, invalidf("INSERT JSON is not supported")
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
			m.ops = append(m.ops, setOp{col: c, t: tt})
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
			if op.Op != "=" || op.Key != nil || op.Field != "" {
				return nil, invalidf("Invalid operation (%s) for frozen column %s", operationString(op), c.Name)
			}
			tt, err := vs.prepareTerm(op.Value, c.Name, c.Type)
			if err != nil {
				return nil, err
			}
			m.ops = append(m.ops, setOp{col: c, t: tt})
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
			if del.Key != nil || del.Field != "" {
				return nil, invalidf("Invalid deletion operation for frozen column %s", c.Name)
			}
			m.dels = append(m.dels, c)
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
	if m.where, err = analyzeWhere(t, rels, vs); err != nil {
		return nil, err
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
	m.staticOnly = len(m.ops)+len(m.dels) > 0
	for _, op := range m.ops {
		m.staticOnly = m.staticOnly && op.col.Kind == Static
	}
	for _, c := range m.dels {
		m.staticOnly = m.staticOnly && c.Kind == Static
	}
	fullClustering := len(w.ck) == len(t.Clustering)
	switch {
	case m.kind != deleteKind && w.slice != nil:
		return nil, invalidf("Slice restrictions are not supported on the clustering columns in UPDATE statements")
	case m.kind == deleteKind && len(m.dels) == 0:
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
	// Cells to write, split into static and regular.
	var cells, statics []row.Cell
	cell := func(c *Column, v []byte, st valState) {
		var out row.Cell
		switch st {
		case vUnset:
			return
		case vNull:
			out = row.Cell{Column: c.ID, Timestamp: ts, LocalDeletionTime: e.nowSec}
		default:
			out = row.Cell{Column: c.ID, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: v}
			if ttl > 0 {
				out.TTL, out.LocalDeletionTime = uint32(ttl), e.nowSec+ttl
			}
		}
		if c.Kind == Static {
			statics = append(statics, out)
		} else {
			cells = append(cells, out)
		}
	}
	for _, op := range m.ops {
		v, st, err := op.t.bind(e)
		if err != nil {
			return err
		}
		cell(op.col, v, st)
	}
	for _, c := range m.dels {
		cell(c, nil, vNull)
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

	var ranges []keyRange
	if len(t.Clustering) > 0 && !(m.staticOnly && len(m.where.ck) == 0) {
		if ranges, err = m.where.clusteringRanges(e, t); err != nil {
			return err
		}
	}
	deletion := row.Deletion{MarkedForDeleteAt: row.NoTimestamp}
	wholeRows := m.kind == deleteKind && len(m.dels) == 0
	if wholeRows {
		deletion = row.Deletion{MarkedForDeleteAt: ts, LocalDeletionTime: e.nowSec}
	}
	size := 16
	for _, c := range cells {
		size += len(c.Value) + 24
	}
	for _, c := range statics {
		size += len(c.Value) + 24
	}
	buf := make([]byte, 0, size)
	put := func(key []byte, r *row.Row) {
		buf = row.Append(buf[:0], r)
		b.Put(t.Storage, key, buf)
	}
	for _, pk := range pks {
		p := partitionPrefix(pk)
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
			if len(cells) == 0 {
				continue
			}
			for _, r := range ranges {
				put(append(slices.Clip(p), r.start...), &row.Row{Deletion: deletion, Cells: cells})
			}
		}
	}
	return nil
}

func deletionCell(ts, nowSec int64) row.Cell {
	return row.Cell{Column: deletionColumn, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime,
		Value: binary.BigEndian.AppendUint64(nil, uint64(nowSec))}
}

func sortCells(cells []row.Cell) {
	slices.SortFunc(cells, func(a, b row.Cell) int {
		switch {
		case a.Column < b.Column:
			return -1
		case a.Column > b.Column:
			return 1
		}
		return 0
	})
}

// batchStmt is a prepared BATCH (BatchStatement).
type batchStmt struct {
	mods []*modification
}

func (d *DB) prepareBatch(s *Schema, session string, st *cql.Batch, vs *vars) (*batchStmt, error) {
	if st.Type == "COUNTER" {
		return nil, invalidf("Counter batches are not supported")
	}
	b := &batchStmt{}
	for _, raw := range st.Statements {
		m, err := d.prepareModification(s, session, raw, vs)
		if err != nil {
			return nil, err
		}
		if m.cond != nil {
			return nil, invalidf("Conditional batches are not supported")
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
	if err := e.d.applyModifications(mods, e); err != nil {
		return nil, err
	}
	return &message.VoidResult{}, nil
}
