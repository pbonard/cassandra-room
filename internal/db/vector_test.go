package db

import (
	"encoding/binary"
	"math"
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
	s.ok("CREATE INDEX ON ks.items (v) USING 'sai'")
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
