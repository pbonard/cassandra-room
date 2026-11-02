package db

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/cassandra-room/cassandra-room/internal/vecindex"
)

// Vector indexes. An SAI index on a vector column is a proximity graph
// (internal/vecindex, as Cassandra's SAI builds with jvector) kept in the
// index's storage table on each node, over the node's own rows, as SAI
// indexes are local:
// every write applied to the table updates it with the row's current
// vector. ANN queries take candidates from it and read the rows, so
// entries of rows deleted otherwise (partition deletions, TTL) are found
// stale there and removed.

// vecStore is a vecindex.Store over an engine table: values are row cells,
// deletions row tombstones, both timestamped by the node's clock.
type vecStore struct {
	d  *DB
	id lsm.TableID
}

func (s vecStore) table() (*lsm.Table, error) { return s.d.engine.Table(s.id) }

func (s vecStore) Get(key []byte) ([]byte, bool, error) {
	t, err := s.table()
	if err != nil {
		return nil, false, err
	}
	v, err := t.Get(key)
	if errors.Is(err, lsm.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return cellValue(v, s.d.opts.Now().Unix())
}

func cellValue(v []byte, nowSec int64) ([]byte, bool, error) {
	r, err := row.Decode(v)
	if err != nil {
		return nil, false, err
	}
	live := r.Live(nowSec)
	if len(live) == 0 {
		return nil, false, nil
	}
	return live[0].Value, true, nil
}

func (s vecStore) put(key []byte, r *row.Row) error {
	b := lsm.NewBatch()
	b.Put(s.id, key, row.Append(nil, r))
	// (Not under tableMu: callers may hold it; a dropped index's table
	// makes Apply fail, which is harmless.)
	return s.d.engine.Apply(b, s.d.wo)
}

func (s vecStore) Put(key, value []byte) error {
	return s.put(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
		Cells: []row.Cell{{Column: 0, Timestamp: s.d.timestamp(), LocalDeletionTime: row.NoDeletionTime, Value: value}}})
}

func (s vecStore) Delete(key []byte) error {
	return s.put(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: s.d.timestamp(), LocalDeletionTime: s.d.opts.Now().Unix()}})
}

func (s vecStore) Scan(prefix []byte, fn func(key, value []byte) error) error {
	t, err := s.table()
	if err != nil {
		return err
	}
	it, err := t.NewIter(prefix, prefixEnd(prefix))
	if err != nil {
		return err
	}
	defer it.Close()
	now := s.d.opts.Now().Unix()
	for ok := it.First(); ok; ok = it.Next() {
		v, live, err := cellValue(it.Value(), now)
		if err != nil {
			return err
		}
		if live {
			if err := fn(it.Key(), v); err != nil {
				return err
			}
		}
	}
	return it.Error()
}

// vectorIndexes caches the open vector indexes, by storage table.
type vectorIndexes struct {
	mu sync.Mutex
	m  map[lsm.TableID]*vecindex.Index
}

func similarityOf(ix *Index) vecindex.Similarity {
	switch strings.ToLower(ix.Options["similarity_function"]) {
	case "euclidean":
		return vecindex.Euclidean
	case "dot_product":
		return vecindex.DotProduct
	}
	return vecindex.Cosine
}

// vectorOptions validates a vector index's options (IndexWriterConfig).
func vectorOptions(ix *Index) (vecindex.Options, error) {
	o := vecindex.Options{Dims: ix.col.Type.Dim, Similarity: similarityOf(ix)}
	if v, ok := ix.Options["similarity_function"]; ok {
		switch strings.ToUpper(v) {
		case "COSINE", "EUCLIDEAN", "DOT_PRODUCT":
		default:
			return o, invalidf("Similarity function %s was not recognized for index %s. Valid values are: EUCLIDEAN, DOT_PRODUCT, COSINE", strings.ToUpper(v), ix.Name)
		}
	}
	for name, dst := range map[string]*int{"maximum_node_connections": &o.MaxConnections, "construction_beam_width": &o.BeamWidth} {
		v, ok := ix.Options[name]
		if !ok {
			continue
		}
		n, err := strconv.Atoi(v)
		limit := map[string]int{"maximum_node_connections": 512, "construction_beam_width": 3200}[name]
		if err != nil || n <= 0 || n > limit {
			return o, invalidf("%s for index %s must be an integer in (0, %d], was %s", name, ix.Name, limit, v)
		}
		*dst = n
	}
	if v, ok := ix.Options["optimize_for"]; ok && !strings.EqualFold(v, "latency") && !strings.EqualFold(v, "recall") {
		return o, invalidf("optimize_for '%s' was not recognized for index %s. Valid values are: LATENCY, RECALL", strings.ToUpper(v), ix.Name)
	}
	return o, nil
}

// vectorIndex returns the open C-SPANN index of a vector SAI index.
func (d *DB) vectorIndex(ix *Index) (*vecindex.Index, error) {
	d.vecs.mu.Lock()
	defer d.vecs.mu.Unlock()
	if x := d.vecs.m[ix.Storage]; x != nil {
		return x, nil
	}
	o, err := vectorOptions(ix)
	if err != nil {
		return nil, err
	}
	x, err := vecindex.New(vecStore{d, ix.Storage}, o)
	if err != nil {
		return nil, err
	}
	if d.vecs.m == nil {
		d.vecs.m = map[lsm.TableID]*vecindex.Index{}
	}
	d.vecs.m[ix.Storage] = x
	return x, nil
}

// updateVectorIndexes brings the vector indexes of the tables written to
// up to date with the rows' current vectors (after the writes applied).
func (d *DB) updateVectorIndexes(writes []write) {
	s := d.schema.Load()
	for _, w := range writes {
		t := s.byStorage[w.table]
		if t == nil {
			continue
		}
		for _, ix := range t.Indexes {
			if ix.col.Type.Kind != types.Vector || !isFloatVector(ix.col.Type) {
				continue
			}
			_ = d.indexVector(t, ix, w.key)
		}
	}
}

func isFloatVector(t *types.Type) bool {
	return t.Kind == types.Vector && (t.Elems[0].Kind == types.Float || t.Elems[0].Kind == types.Double)
}

// indexVector indexes a row's current vector (or removes it).
func (d *DB) indexVector(t *Table, ix *Index, key []byte) error {
	if len(t.Clustering) > 0 {
		if _, _, rest, err := parsePartition(key); err != nil || len(rest) == 0 || rest[len(rest)-1] != types.Terminator {
			return nil // not a row
		}
	}
	x, err := d.vectorIndex(ix)
	if err != nil {
		return err
	}
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return err
	}
	var vec []float32
	if v, err := lt.Get(key); err == nil {
		if r, err := row.Decode(v); err == nil {
			for _, c := range liveCells(r.Cells, r.Deletion.MarkedForDeleteAt, d.opts.Now().Unix()) {
				if c.Column == ix.col.ID && len(c.Value) > 0 {
					vec, _ = types.FloatVector(ix.col.Type, c.Value)
				}
			}
		}
	}
	if vec == nil {
		return x.Delete(key)
	}
	if err := x.Insert(key, vec); err != nil {
		// Vectors the index cannot hold (zero vectors under cosine, NaN)
		// are left out, as SAI does.
		return x.Delete(key)
	}
	return nil
}

// buildVectorIndex indexes a table's existing rows.
func (d *DB) buildVectorIndex(t *Table, ix *Index) error {
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return err
	}
	it, err := lt.NewIter(nil, nil)
	if err != nil {
		return err
	}
	var keys [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		keys = append(keys, bytes.Clone(it.Key()))
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := d.indexVector(t, ix, k); err != nil {
			return err
		}
	}
	return nil
}

// annSearch serves ORDER BY ... ANN OF from the vector index: it fetches
// candidates, reads their rows through the query (which applies the other
// restrictions and scores them exactly), and asks for more candidates
// while fewer than LIMIT rows qualified.
func (q *query) annSearch(ix *Index) error {
	x, err := q.e.d.vectorIndex(ix)
	if err != nil {
		return err
	}
	src, ok := q.src.(engineSource)
	if !ok {
		lt, err := q.e.d.engine.Table(q.s.t.Storage)
		if err != nil {
			return err
		}
		src = engineSource{lt}
	}
	// As many results as optimize_for asks of the graph for the limit
	// (OptimizeFor.topKFor), more while filters reject them.
	fetch := vecindex.TopKFor(int(min(q.annLimit, 1<<20)), strings.EqualFold(ix.Options["optimize_for"], "recall"))
	for {
		res, err := x.Search(q.annQuery, fetch)
		if err != nil {
			return invalidf("%v", err)
		}
		q.annRows = q.annRows[:0]
		var stale [][]byte
		for _, r := range res {
			_, pk, suffix, err := parsePartition(r.Key)
			if err != nil {
				continue
			}
			p := r.Key[:len(r.Key)-len(suffix)]
			if q.ranges != nil && !q.inRanges(suffix) {
				continue
			}
			if !q.owns(p) {
				continue
			}
			found, err := q.readRowAt(src, p, pk, suffix, nil)
			if err != nil {
				return err
			}
			if !found {
				stale = append(stale, r.Key)
			}
		}
		for _, k := range stale {
			_ = x.Delete(k)
		}
		if int64(len(q.annRows)) >= q.annLimit || len(res) < fetch {
			return nil
		}
		fetch *= 4
	}
}
