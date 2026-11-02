package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
)

// Secondary indexes, as Cassandra's legacy (CassandraIndex) indexes: each
// index has its own storage table whose keys are
//
//	indexed value (byte-comparable component) | partition prefix | row suffix
//
// written in the same batch as the row. Entries are never updated in place:
// an overwritten or deleted value leaves a stale entry, which reads check
// against the base row and delete. StorageAttachedIndex (SAI) indexes are
// the same structure; because the entries are ordered by value they also
// serve range restrictions.

// Index is a secondary index.
type Index struct {
	Name   string
	Column string
	// Target is what of the column is indexed: "" (the value), "values"
	// (collection elements or map values), "keys" (map keys), "full" (a
	// frozen collection's whole value).
	Target  string
	Class   string            `json:",omitempty"`
	Options map[string]string `json:",omitempty"`
	Storage lsm.TableID
	col     *Column
}

// sai reports whether the index is a StorageAttachedIndex.
func (ix *Index) sai() bool {
	c := strings.ToLower(ix.Class)
	return strings.HasSuffix(c, "storageattachedindex") || c == "sai"
}

// valueType is the type of the indexed values.
func (ix *Index) valueType() *types.Type {
	t := ix.col.Type
	switch ix.Target {
	case "keys":
		return t.Elems[0]
	case "entries":
		return &types.Type{Kind: types.Tuple, Elems: []*types.Type{t.Elems[0], t.Elems[1]}, Frozen: true}
	case "values":
		if t.Kind == types.Map {
			return t.Elems[1]
		}
		return t.Elems[0]
	}
	return t
}

// targetString is the index target as system_schema.indexes shows it.
func (ix *Index) targetString() string {
	if ix.Target == "" {
		return quoteName(ix.Column)
	}
	return ix.Target + "(" + quoteName(ix.Column) + ")"
}

// newIndex validates CREATE INDEX (CreateIndexStatement).
func newIndex(t *Table, st *cql.CreateIndex) (*Index, error) {
	c := t.byName[st.Column]
	if c == nil {
		return nil, invalidf("Column '%s' doesn't exist", st.Column)
	}
	ix := &Index{Name: st.Name, Column: c.Name, Target: st.Target, Class: st.Class, Options: st.Options, col: c}
	if ix.Class != "" && !ix.sai() {
		return nil, invalidf("Unable to find custom indexer class '%s'", ix.Class)
	}
	switch c.Kind {
	case PartitionKey, Clustering:
		return nil, invalidf("Indexes on primary key columns are not supported")
	}
	if c.Type.Kind == types.Counter {
		return nil, invalidf("Cannot create secondary index on a counter column")
	}
	if c.Type.Kind == types.Vector && !ix.sai() {
		return nil, invalidf("Vector column %s can only be indexed with 'sai' (StorageAttachedIndex)", c.Name)
	}
	if c.Type.Kind == types.Vector {
		if _, err := vectorOptions(ix); err != nil {
			return nil, err
		}
	}
	multi := c.Type.IsMultiCell()
	switch ix.Target {
	case "":
		if multi && c.Type.Kind != types.UDT {
			ix.Target = "values" // the default for collections
		}
		if multi && c.Type.Kind == types.UDT {
			return nil, invalidf("Secondary indexes are not supported on non-frozen UDTs")
		}
	case "values", "keys":
		if !multi || (ix.Target == "keys" && c.Type.Kind != types.Map) {
			return nil, invalidf("Cannot create %s() index on %s column %s", ix.Target, map[bool]string{true: "frozen", false: "non-collection"}[c.Type.IsCollection()], c.Name)
		}
	case "full":
		if multi || !c.Type.IsCollection() {
			return nil, invalidf("full() indexes can only be created on frozen collections")
		}
		ix.Target = ""
	case "entries":
		if !multi || c.Type.Kind != types.Map {
			return nil, invalidf("Cannot create entries() index on %s column %s", map[bool]string{true: "frozen", false: "non-map"}[c.Type.IsCollection()], c.Name)
		}
	}
	if ix.Name == "" {
		ix.Name = t.Name + "_" + c.Name + "_idx"
	}
	if !validName(ix.Name) {
		return nil, invalidf("Index name must not be empty, or contain non-alphanumeric-underscore characters (got \"%s\")", ix.Name)
	}
	return ix, nil
}

// indexValues returns the values a row's cells contribute to an index:
// one value for a simple column, the elements (or keys) of a collection.
func (ix *Index) indexValues(cells []row.Cell, nowSec int64) (vals [][]byte, live []row.Cell) {
	for _, c := range columnCells(cells, ix.col.ID) {
		if c.ComplexDeletion || !c.IsLive(nowSec) {
			continue
		}
		switch {
		case ix.Target == "keys" || (ix.Target == "values" && ix.col.Type.Kind == types.Set):
			k, _, err := types.ReadComponent(c.Path, ix.col.Type.Elems[0], false)
			if err != nil {
				continue
			}
			vals = append(vals, k)
		case ix.Target == "entries":
			k, _, err := types.ReadComponent(c.Path, ix.col.Type.Elems[0], false)
			if err != nil {
				continue
			}
			vals = append(vals, entryTuple(k, c.Value))
		default:
			vals = append(vals, c.Value)
		}
		live = append(live, c)
	}
	return vals, live
}

// entryKey is the key of an index entry.
func (ix *Index) entryKey(v, rowKey []byte) ([]byte, error) {
	k, err := types.AppendComponent(nil, ix.valueType(), v, false)
	if err != nil {
		return nil, err
	}
	return append(k, rowKey...), nil
}

// addEntries adds the index entries of a row write to b.
func (t *Table) addEntries(b *mutation, rowKey []byte, cells []row.Cell, nowSec int64) error {
	for _, ix := range t.Indexes {
		if ix.col.Type.Kind == types.Vector {
			continue // ANN queries score the rows themselves
		}
		vals, live := ix.indexValues(cells, nowSec)
		for i, v := range vals {
			key, err := ix.entryKey(v, rowKey)
			if err != nil {
				return err
			}
			c := live[i]
			marker := row.Cell{Column: 0, Timestamp: c.Timestamp, LocalDeletionTime: row.NoDeletionTime}
			if c.TTL > 0 {
				marker.TTL, marker.LocalDeletionTime = c.TTL, c.LocalDeletionTime
			}
			b.put(ix.Storage, key, row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{marker}}), tokenOf(rowKey))
		}
	}
	return nil
}

// buildIndex writes the entries of the table's existing rows.
func (d *DB) buildIndex(t *Table, ix *Index, nowSec int64) error {
	if isFloatVector(ix.col.Type) {
		return d.buildVectorIndex(t, ix)
	}
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return err
	}
	it, err := lt.NewIter(nil, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	only := *t
	only.Indexes = []*Index{ix}
	b := &mutation{}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(t.Clustering) > 0 {
			_, _, rest, err := parsePartition(k)
			if err != nil || len(rest) == 0 || (rest[0] < types.NullSep && rest[0] != headerSuffix) {
				continue // a range tombstone
			}
		}
		r, err := row.Decode(it.Value())
		if err != nil {
			return err
		}
		cells := liveCells(r.Cells, r.Deletion.MarkedForDeleteAt, nowSec)
		if err := only.addEntries(b, slices.Clone(k), cells, nowSec); err != nil {
			return err
		}
		if len(b.writes) >= 1000 {
			if err := d.applyLocal(b.writes); err != nil {
				return err
			}
			b = &mutation{}
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	return d.applyLocal(b.writes)
}

// indexPlan is a SELECT served by an index: the restriction it uses.
type indexPlan struct {
	ix *Index
	f  filter
}

// findIndex returns an index that can serve one of the filters
// (SecondaryIndexManager.getBestIndexFor): EQ (or CONTAINS, CONTAINS KEY
// on collections), or a range on an SAI index.
func findIndex(t *Table, filters []filter) *indexPlan {
	for _, f := range filters {
		if f.tuple != nil || (f.col.Kind != Regular && f.col.Kind != Static) {
			continue
		}
		for _, ix := range t.Indexes {
			if ix.col != f.col || ix.col.Type.Kind == types.Vector {
				continue
			}
			switch {
			case f.op == "=" && ix.Target == "" && f.in != nil && f.in.eq != nil:
				return &indexPlan{ix: ix, f: f}
			case f.op == "CONTAINS" && ix.Target == "values", f.op == "CONTAINS KEY" && ix.Target == "keys",
				f.op == "ENTRY=" && ix.Target == "entries":
				return &indexPlan{ix: ix, f: f}
			case ix.sai() && ix.Target == "" && (f.op == "<" || f.op == "<=" || f.op == ">" || f.op == ">="):
				return &indexPlan{ix: ix, f: f}
			}
		}
	}
	return nil
}

// scanIndex reads the rows an index selects; the restriction remains a
// filter, which also skips stale entries.
func (q *query) scanIndex(plan *indexPlan) error {
	e := q.e
	var v []byte
	var st valState
	var err error
	if plan.f.in != nil {
		v, st, err = plan.f.in.eq.bind(e)
	} else {
		v, st, err = plan.f.t.bind(e)
	}
	if err != nil {
		return err
	}
	if st != vSet {
		return invalidf("Invalid null value in condition for column %s", plan.f.col.Name)
	}
	if plan.f.op == "ENTRY=" {
		k, kst, err := plan.f.key.bind(e)
		if err != nil {
			return err
		}
		if kst != vSet {
			return invalidf("Invalid null value for map key of %s", plan.f.col.Name)
		}
		v = entryTuple(k, v)
	}
	enc, err := types.AppendComponent(nil, plan.ix.valueType(), v, false)
	if err != nil {
		return invalidf("%v", err)
	}
	var lo, hi []byte
	switch plan.f.op {
	case "=", "CONTAINS", "CONTAINS KEY", "ENTRY=":
		lo, hi = enc, prefixEnd(enc)
	case ">=":
		lo = enc
	case ">":
		lo = prefixEnd(enc)
	case "<=":
		hi = prefixEnd(enc)
	case "<":
		hi = enc
	}
	if q.resume != nil && bytes.Compare(q.resume.lastKey, lo) >= 0 {
		lo = append(slices.Clone(q.resume.lastKey), 0)
	}
	src, ok := q.src.(engineSource)
	if !ok {
		lt, err := q.e.d.engine.Table(q.s.t.Storage)
		if err != nil {
			return invalidf("table %s.%s was dropped", q.s.t.Keyspace, q.s.t.Name)
		}
		src = engineSource{lt}
	}
	it, err := q.e.d.engineIndexIter(plan.ix, lo, hi)
	if err != nil {
		return err
	}
	defer it.Close()
	var stale [][]byte
	for ok := it.First(); ok && !q.done(); ok = it.Next() {
		key := slices.Clone(it.Key())
		_, rest, err := types.ReadComponent(key, plan.ix.valueType(), false)
		if err != nil {
			continue
		}
		_, pk, suffix, err := parsePartition(rest)
		if err != nil {
			continue
		}
		p := rest[:len(rest)-len(suffix)]
		if q.ranges != nil && !q.inRanges(suffix) {
			continue
		}
		if !q.owns(p) {
			continue
		}
		q.indexKey = key
		if len(suffix) > 0 && suffix[0] == headerSuffix {
			// A static column's entry: every row of the partition (the
			// restriction stays a filter on each).
			pkVals, err := splitPartitionKey(pk, len(q.s.t.PartitionKey))
			if err != nil {
				return err
			}
			if err := q.readPartition(p, pkVals); err != nil {
				return err
			}
			continue
		}
		matched, err := q.readRowAt(src, p, pk, suffix, &plan.f)
		if err != nil {
			return err
		}
		if !matched {
			stale = append(stale, key)
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	if !q.done() {
		q.exhausted = true
	}
	q.e.d.deleteStaleEntries(plan.ix, stale, q.e.nowSec)
	return nil
}

func (d *DB) engineIndexIter(ix *Index, lo, hi []byte) (iterator, error) {
	lt, err := d.engine.Table(ix.Storage)
	if err != nil {
		return nil, invalidf("index %s was dropped", ix.Name)
	}
	return lt.NewIter(lo, hi)
}

// deleteStaleEntries deletes index entries whose row no longer has the
// value (as Cassandra's index reads do).
func (d *DB) deleteStaleEntries(ix *Index, keys [][]byte, nowSec int64) {
	if len(keys) == 0 {
		return
	}
	lt, err := d.engine.Table(ix.Storage)
	if err != nil {
		return
	}
	b := lsm.NewBatch()
	for _, k := range keys {
		v, err := lt.Get(k)
		if err != nil {
			continue
		}
		r, err := row.Decode(v)
		if err != nil {
			continue
		}
		ts := int64(row.NoTimestamp)
		for _, c := range r.Cells {
			ts = max(ts, c.Timestamp)
		}
		b.Put(ix.Storage, k, row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: ts, LocalDeletionTime: nowSec}}))
	}
	_ = d.engine.Apply(b, lsm.NoSync)
}

func (q *query) inRanges(suffix []byte) bool {
	for _, r := range q.ranges {
		if r.point && bytes.Equal(suffix, r.start) || !r.point && bytes.Compare(suffix, r.start) >= 0 && bytes.Compare(suffix, r.end) < 0 {
			return true
		}
	}
	return false
}

// readRowAt reads one row (key suffix after the partition prefix p) and
// emits it if it matches the query. It reports whether the row still has
// the indexed value f, which tells stale index entries.
func (q *query) readRowAt(src engineSource, p, pk, suffix []byte, f *filter) (bool, error) {
	t := q.s.t
	pkVals, err := splitPartitionKey(pk, len(t.PartitionKey))
	if err != nil {
		return false, err
	}
	dels := deletions{partition: row.NoTimestamp}
	var static []row.Cell
	if len(t.Clustering) > 0 {
		it, err := src.iter(append(slices.Clone(p), headerSuffix), append(slices.Clone(p), tombstoneSuffix+1))
		if err != nil {
			return false, err
		}
		for ok := it.First(); ok; ok = it.Next() {
			r, err := row.Decode(it.Value())
			if err != nil {
				it.Close()
				return false, err
			}
			if it.Key()[len(p)] == headerSuffix {
				for _, c := range r.Cells {
					if c.Column == deletionColumn {
						dels.partition = max(dels.partition, c.Timestamp)
					} else if c.Column < rtEndColumn {
						static = append(static, c)
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
			return false, err
		}
		static = cloneCells(static)
	}
	key := append(slices.Clone(p), suffix...)
	v, err := src.get(key)
	if errors.Is(err, errNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	kvr := kv{key, slices.Clone(v)}
	rd, err := q.decodeRow(kvr, p, pkVals, liveCells(static, dels.partition, q.e.nowSec), &dels)
	if err != nil || rd == nil {
		return false, err
	}
	if f != nil {
		if ok, err := f.matches(q.e, rd); err != nil || !ok {
			return false, err
		}
	}
	return true, q.emitPartition(p, pkVals, static, dels, []kv{kvr})
}

// entryTuple is the indexed value of a map entry: the tuple (key, value).
func entryTuple(k, v []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(k)))
	out = append(out, k...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(v)))
	return append(out, v...)
}

// owns reports whether a scattered index query answers for a partition:
// one in its token range, or, without one, a partition this node is the
// first live replica of.
func (q *query) owns(p []byte) bool {
	if r := q.e.tokenRange; r != nil {
		return inTokenRange(tokenOf(p), r[0], r[1])
	}
	px := q.e.d.proxy
	return px == nil || px.ownsPrimary(q.s.t.Keyspace, tokenOf(p))
}

// inTokenRange reports whether tok is in (lo, hi], wrapping if lo >= hi.
func inTokenRange(tok, lo, hi int64) bool {
	if lo < hi {
		return tok > lo && tok <= hi
	}
	return tok > lo || tok <= hi
}
