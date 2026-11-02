package db

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
)

// Vector search (CEP-30): similarity functions, and ORDER BY v ANN OF q
// LIMIT k on a column with an SAI index, which returns the k rows whose
// vectors are most similar to q under the index's similarity function.
// Candidates are the rows the rest of the query selects; each is scored
// exactly (no approximate graph), so results are exact nearest neighbours.

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

type annOrder struct {
	col *Column
	t   term
	fn  similarity
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
	s.ann = &annOrder{col: c, t: t, fn: fn}
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
		q.rows = append(q.rows, r.row)
	}
	q.count = int64(len(q.rows))
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
