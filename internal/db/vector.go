package db

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// Vector search (CEP-30): similarity functions, and ORDER BY v ANN OF q
// LIMIT k on a column with an SAI index, which returns the k rows whose
// vectors are most similar to q under the index's similarity function.
// Without a partition key restriction the candidates come from the
// column's vector index (vecstore.go); with one, every row of the selected
// partitions is scored exactly.

// similarity is a VectorSimilarityFunction: higher is more similar, in
// [0, 1].
type similarity func(a, b []float32) float32

var similarities = map[string]similarity{
	"cosine": func(a, b []float32) float32 {
		var dot, na, nb float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		if na == 0 || nb == 0 {
			return 0
		}
		return float32((1 + dot/math.Sqrt(na*nb)) / 2)
	},
	"euclidean": func(a, b []float32) float32 {
		var d float64
		for i := range a {
			x := float64(a[i]) - float64(b[i])
			d += x * x
		}
		return float32(1 / (1 + d))
	},
	"dot_product": func(a, b []float32) float32 {
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return float32((1 + dot) / 2)
	},
}

// maxANNLimit is the largest LIMIT of an ANN query
// (SAI_VECTOR_SEARCH_MAX_TOP_K): results are merged in memory.
const maxANNLimit = 1000

type annOrder struct {
	col *Column
	t   term
	fn  similarity
	ix  *Index
}

// annIndex returns the vector index serving the query's ANN ordering.
func (s *selectStmt) annIndex() *Index {
	if s.ann == nil || !isFloatVector(s.ann.col.Type) || (s.where.pkComplete() && len(s.t.PartitionKey) > 0) {
		return nil // restricted partitions are scored row by row
	}
	return s.ann.ix
}

// prepareANN validates ORDER BY col ANN OF vector.
func (s *selectStmt) prepareANN(st *cql.Select, vs *vars) error {
	if len(st.OrderBy) > 1 {
		return invalidf("Cannot specify more than one ANN ordering")
	}
	o := st.OrderBy[0]
	c := s.t.byName[o.Column]
	if c == nil {
		return invalidf("Undefined column name %s in table %s.%s", o.Column, s.t.Keyspace, s.t.Name)
	}
	if c.Type.Kind != types.Vector || (c.Type.Elems[0].Kind != types.Float && c.Type.Elems[0].Kind != types.Double) {
		return invalidf("ANN ordering is only supported on float vector indexes")
	}
	var ix *Index
	for _, x := range s.t.Indexes {
		if x.col == c && x.sai() {
			ix = x
		}
	}
	if ix == nil {
		return invalidf("ANN ordering by vector requires the column to be indexed using 'sai'")
	}
	if st.Limit == nil {
		return invalidf("SELECT * query with ANN ordering requires a LIMIT that is not greater than 1000")
	}
	if s.aggregate || len(s.groupBy) > 0 {
		return invalidf("ANN ordering does not support aggregation or GROUP BY")
	}
	t, err := vs.prepareTerm(o.Ann, c.Name, c.Type)
	if err != nil {
		return err
	}
	fn := similarities[strings.ToLower(ix.Options["similarity_function"])]
	if fn == nil {
		fn = similarities["cosine"]
	}
	s.ann = &annOrder{col: c, t: t, fn: fn, ix: ix}
	return nil
}

func (a *annOrder) query(e *execCtx) ([]float32, error) {
	v, st, err := a.t.bind(e)
	if err != nil {
		return nil, err
	}
	if st != vSet {
		return nil, invalidf("Unsupported null value for ANN ordering of column %s", a.col.Name)
	}
	return types.FloatVector(a.col.Type, v)
}

type annRow struct {
	score float32
	row   [][]byte
}

// annAdd scores a candidate row, keeping the best annLimit.
func (q *query) annAdd(r *rowData, out [][]byte) error {
	v := r.value(q.s.ann.col)
	if v == nil {
		return nil
	}
	vec, err := types.FloatVector(q.s.ann.col.Type, v)
	if err != nil || len(vec) != len(q.annQuery) {
		return nil
	}
	q.annRows = append(q.annRows, annRow{score: q.s.ann.fn(vec, q.annQuery), row: out})
	if int64(len(q.annRows)) > 2*q.annLimit+64 {
		q.annTrim()
	}
	return nil
}

func (q *query) annTrim() {
	slices.SortStableFunc(q.annRows, func(a, b annRow) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		}
		return 0
	})
	if int64(len(q.annRows)) > q.annLimit {
		q.annRows = q.annRows[:q.annLimit]
	}
}

func (q *query) annFinish() {
	q.annTrim()
	q.rows = q.rows[:0]
	for _, r := range q.annRows {
		row := r.row
		if q.e.local {
			// Scattered: the coordinator merges the nodes' rows by score.
			row = append(row, binary.BigEndian.AppendUint32(nil, math.Float32bits(r.score)))
		}
		q.rows = append(q.rows, row)
	}
	q.count = int64(len(q.rows))
}

// annScoreColumn is the score column of a scattered ANN query's rows.
const annScoreColumn = "[ann_score]"

// mergeANN merges the nodes' scored rows: the best limit, without the
// score column.
func mergeANN(out *message.RowsResult, limit int64) {
	n := len(out.Metadata.Columns) - 1
	score := func(r [][]byte) float32 { return math.Float32frombits(binary.BigEndian.Uint32(r[n])) }
	slices.SortStableFunc(out.Data, func(a, b [][]byte) int {
		switch sa, sb := score(a), score(b); {
		case sa > sb:
			return -1
		case sa < sb:
			return 1
		}
		return 0
	})
	if int64(len(out.Data)) > limit {
		out.Data = out.Data[:limit]
	}
	for i := range out.Data {
		out.Data[i] = out.Data[i][:n]
	}
	md := *out.Metadata
	md.Columns = md.Columns[:n]
	md.ColumnCount = int32(n)
	out.Metadata = &md
}

// similarityFunc evaluates similarity_cosine/euclidean/dot_product.
func similarityFunc(name string, t *types.Type, a, b []byte) ([]byte, error) {
	if a == nil || b == nil {
		return nil, nil
	}
	va, err := types.FloatVector(t, a)
	if err != nil {
		return nil, invalidf("%v", err)
	}
	vb, err := types.FloatVector(t, b)
	if err != nil {
		return nil, invalidf("%v", err)
	}
	if len(va) != len(vb) {
		return nil, invalidf("All arguments must have the same vector dimensions")
	}
	return binary.BigEndian.AppendUint32(nil, math.Float32bits(similarities[name](va, vb))), nil
}
