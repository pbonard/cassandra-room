package db

import (
	"encoding/binary"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
)

func TestVectors(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.items (id int PRIMARY KEY, name text, v vector<float, 3>)")
	s.fails("INSERT INTO ks.items (id, v) VALUES (9, [1.0, 2.0])", "expected 3 elements")
	pts := map[int][3]float32{1: {1, 0, 0}, 2: {0.9, 0.1, 0}, 3: {0, 1, 0}, 4: {0, 0, 1}, 5: {-1, 0, 0}}
	for id, p := range pts {
		s.ok("INSERT INTO ks.items (id, name, v) VALUES (?, ?, ?)", id, "n"+string(rune('0'+id)), vec(p[:]...))
	}
	require.Equal(t, []string{"[1.0, 0.0, 0.0]"}, s.rows("SELECT v FROM ks.items WHERE id = 1"))
	require.Equal(t, []string{`{"v": [1.0, 0.0, 0.0]}`}, s.rows("SELECT JSON v FROM ks.items WHERE id = 1"))
	s.fails("SELECT id FROM ks.items ORDER BY v ANN OF [1, 0, 0] LIMIT 2", "indexed using 'sai'")
	s.fails("CREATE INDEX ON ks.items (v)", "can only be indexed with 'sai'")
	s.fails("CREATE INDEX ON ks.items (v) USING 'sai' WITH OPTIONS = {'similarity_function': 'manhattan'}", "was not recognized")
	s.fails("CREATE INDEX ON ks.items (v) USING 'sai' WITH OPTIONS = {'optimize_for': 'speed'}", "was not recognized")
	s.fails("CREATE INDEX ON ks.items (v) USING 'sai' WITH OPTIONS = {'maximum_node_connections': '0'}", "maximum_node_connections")
	s.ok("CREATE INDEX ON ks.items (v) USING 'sai' WITH OPTIONS = {'optimize_for': 'recall', 'maximum_node_connections': '24'}")
	s.fails("SELECT id FROM ks.items ORDER BY v ANN OF [1, 0, 0]", "requires a LIMIT")
	require.Equal(t, []string{"1", "2"}, s.rows("SELECT id FROM ks.items ORDER BY v ANN OF [1, 0, 0] LIMIT 2"))
	all := s.rows("SELECT id FROM ks.items ORDER BY v ANN OF [1, 0, 0] LIMIT 10")
	require.Len(t, all, 5)
	require.Equal(t, "5", all[4], "the opposite vector is last")
	require.Equal(t, []string{"4", "3"}, s.rows("SELECT id FROM ks.items ORDER BY v ANN OF ? LIMIT 2", vec(0, 0.2, 1)))
	// Similarity functions.
	rows := s.ok("SELECT similarity_cosine(v, [1, 0, 0]), similarity_euclidean(v, [1, 0, 0]), similarity_dot_product(v, [1, 0, 0]) FROM ks.items WHERE id = 5").(*message.RowsResult).Data[0]
	f := func(b []byte) float32 { return math.Float32frombits(binary.BigEndian.Uint32(b)) }
	require.InDelta(t, 0, f(rows[0]), 1e-6)   // opposite directions
	require.InDelta(t, 0.2, f(rows[1]), 1e-6) // 1/(1+4)
	require.InDelta(t, 0, f(rows[2]), 1e-6)   // (1 + -1)/2
	// With euclidean similarity on a dot-product index option.
	s.ok("CREATE TABLE ks.e (id int PRIMARY KEY, v vector<float, 2>)")
	s.ok("CREATE INDEX ON ks.e (v) USING 'sai' WITH OPTIONS = {'similarity_function': 'euclidean'}")
	s.ok("INSERT INTO ks.e (id, v) VALUES (1, [10, 10])")
	s.ok("INSERT INTO ks.e (id, v) VALUES (2, [1, 1])")
	require.Equal(t, []string{"2"}, s.rows("SELECT id FROM ks.e ORDER BY v ANN OF [0, 0] LIMIT 1"))
}

// vec serializes a float vector.
func vec(xs ...float32) []byte {
	var b []byte
	for _, x := range xs {
		b = binary.BigEndian.AppendUint32(b, math.Float32bits(x))
	}
	return b
}

// TestVectorIndexANN checks ANN through the C-SPANN index on enough rows
// to build a tree, against brute force, with updates, deletes and filters.
func TestVectorIndexANN(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.docs (id int PRIMARY KEY, kind int, emb vector<float, 8>)")
	rng := rand.New(rand.NewSource(5))
	var centers [][]float32
	for range 20 {
		c := make([]float32, 8)
		for i := range c {
			c[i] = float32(rng.NormFloat64())
		}
		centers = append(centers, c)
	}
	vecs := map[int][]float32{}
	point := func() []float32 {
		c := centers[rng.Intn(len(centers))]
		v := make([]float32, 8)
		for i := range v {
			v[i] = c[i] + 0.3*float32(rng.NormFloat64())
		}
		return v
	}
	// Half the rows before the index (built from them), half after.
	for id := 0; id < 600; id++ {
		if id == 300 {
			s.ok("CREATE INDEX ON ks.docs (emb) USING 'sai' WITH OPTIONS = {'similarity_function': 'euclidean'}")
		}
		vecs[id] = point()
		s.ok("INSERT INTO ks.docs (id, kind, emb) VALUES (?, ?, ?)", id, id%2, vec(vecs[id]...))
	}
	s.ok("DELETE FROM ks.docs WHERE id = 7")
	delete(vecs, 7)
	vecs[8] = point()
	s.ok("UPDATE ks.docs SET emb = ? WHERE id = 8", vec(vecs[8]...))
	brute := func(q []float32, k int, keep func(int) bool) []string {
		type kd struct {
			id int
			d  float64
		}
		var all []kd
		for id, v := range vecs {
			if !keep(id) {
				continue
			}
			var d float64
			for i := range v {
				d += float64((v[i] - q[i]) * (v[i] - q[i]))
			}
			all = append(all, kd{id, d})
		}
		sort.Slice(all, func(a, b int) bool { return all[a].d < all[b].d })
		var out []string
		for _, e := range all[:k] {
			out = append(out, strconv.Itoa(e.id))
		}
		return out
	}
	hits, total := 0, 0
	for i := 0; i < 20; i++ {
		q := point()
		got := s.rows("SELECT id FROM ks.docs ORDER BY emb ANN OF ? LIMIT 5", vec(q...))
		want := brute(q, 5, func(int) bool { return true })
		for _, g := range got {
			if slices.Contains(want, g) {
				hits++
			}
		}
		total += 5
		require.NotContains(t, got, "7")
		// With a filter: only even ids.
		got = s.rows("SELECT id FROM ks.docs WHERE kind = 0 ORDER BY emb ANN OF ? LIMIT 5 ALLOW FILTERING", vec(q...))
		require.Len(t, got, 5)
		for _, g := range got {
			n, _ := strconv.Atoi(g)
			require.Zero(t, n%2)
		}
	}
	require.Greater(t, float64(hits)/float64(total), 0.9)
}
