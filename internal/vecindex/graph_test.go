package vecindex

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type memStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMem() *memStore { return &memStore{m: map[string][]byte{}} }

func (s *memStore) Get(k []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[string(k)]
	return v, ok, nil
}
func (s *memStore) Put(k, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[string(k)] = append([]byte(nil), v...)
	return nil
}
func (s *memStore) Delete(k []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, string(k))
	return nil
}
func (s *memStore) Scan(prefix []byte, fn func(k, v []byte) error) error {
	s.mu.Lock()
	var keys []string
	for k := range s.m {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		vals[i] = s.m[k]
	}
	s.mu.Unlock()
	for i, k := range keys {
		if err := fn([]byte(k), vals[i]); err != nil {
			return err
		}
	}
	return nil
}

func randVec(rng *rand.Rand, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

func key(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }

func recall(t *testing.T, x *Index, data map[int][]float32, queries [][]float32, k int, forRecall bool) float64 {
	hits := 0
	for _, q := range queries {
		res, err := x.Search(q, TopKFor(k, forRecall))
		require.NoError(t, err)
		type ks struct {
			k int
			s float32
		}
		var all []ks
		for i, v := range data {
			all = append(all, ks{i, x.opts.Similarity.Score(q, v)})
		}
		sort.Slice(all, func(a, b int) bool { return all[a].s > all[b].s })
		want := map[int]bool{}
		for _, e := range all[:k] {
			want[e.k] = true
		}
		for _, r := range res[:min(k, len(res))] {
			if want[int(binary.BigEndian.Uint32(r.Key))] {
				hits++
			}
		}
	}
	return float64(hits) / float64(k*len(queries))
}

func TestGraphRecall(t *testing.T) {
	for _, sim := range []Similarity{Cosine, Euclidean, DotProduct} {
		rng := rand.New(rand.NewSource(1))
		st := newMem()
		x, err := New(st, Options{Dims: 24, Similarity: sim})
		require.NoError(t, err)
		data := map[int][]float32{}
		for i := 0; i < 3000; i++ {
			v := randVec(rng, 24)
			if sim == DotProduct { // dot product expects unit vectors
				n := dot(v, v)
				for j := range v {
					v[j] /= float32(sqrt(n))
				}
			}
			data[i] = v
			require.NoError(t, x.Insert(key(i), v))
		}
		var queries [][]float32
		for i := 0; i < 50; i++ {
			queries = append(queries, randVec(rng, 24))
		}
		// Unclustered gaussian data, the hard case: optimize_for latency
		// searches 16 for 10, recall 42.
		lat, rec := recall(t, x, data, queries, 10, false), recall(t, x, data, queries, 10, true)
		t.Logf("similarity %d: recall@10 %.3f (latency), %.3f (recall)", sim, lat, rec)
		require.Greater(t, lat, 0.75)
		require.Greater(t, rec, 0.95)

		// Deletes and replacements; then reopen from the store.
		for i := 0; i < 1000; i++ {
			require.NoError(t, x.Delete(key(i)))
			delete(data, i)
		}
		for i := 1000; i < 1300; i++ {
			v := randVec(rng, 24)
			if sim == DotProduct {
				n := dot(v, v)
				for j := range v {
					v[j] /= float32(sqrt(n))
				}
			}
			data[i] = v
			require.NoError(t, x.Insert(key(i), v))
		}
		require.Equal(t, 2000, x.Len())
		y, err := New(st, Options{Dims: 24, Similarity: sim})
		require.NoError(t, err)
		require.Equal(t, 2000, y.Len())
		require.Greater(t, recall(t, y, data, queries, 10, true), 0.95)
		for _, q := range queries[:5] {
			res, err := y.Search(q, 20)
			require.NoError(t, err)
			for _, e := range res {
				require.GreaterOrEqual(t, int(binary.BigEndian.Uint32(e.Key)), 1000)
			}
		}
	}
}

func sqrt(x float32) float64 {
	r := float64(x)
	for range 30 {
		r = (r + float64(x)/r) / 2
	}
	return r
}

func TestRejects(t *testing.T) {
	x, err := New(newMem(), Options{Dims: 2})
	require.NoError(t, err)
	require.Error(t, x.Insert(key(1), []float32{0, 0}), "zero vector under cosine")
	require.Error(t, x.Insert(key(1), []float32{1}), "wrong dimensions")
	require.NoError(t, x.Insert(key(1), []float32{1, 0}))
	res, err := x.Search([]float32{1, 0.1}, 5)
	require.NoError(t, err)
	require.Len(t, res, 1)
}

// TestGraphCleanup deletes most vectors: the deleted nodes are removed
// from the graph and the store, and search still finds the live ones.
func TestGraphCleanup(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	st := newMem()
	x, err := New(st, Options{Dims: 16, Similarity: Euclidean})
	require.NoError(t, err)
	data := map[int][]float32{}
	for i := 0; i < 2000; i++ {
		data[i] = randVec(rng, 16)
		require.NoError(t, x.Insert(key(i), data[i]))
	}
	for i := 0; i < 1500; i++ {
		require.NoError(t, x.Delete(key(i)))
		delete(data, i)
	}
	require.NoError(t, x.Cleanup())
	require.Equal(t, 500, x.Len())
	require.Len(t, x.nodes, 500, "deleted nodes are gone")
	nodes := 0
	require.NoError(t, st.Scan([]byte("N"), func(k, v []byte) error { nodes++; return nil }))
	require.Equal(t, 500, nodes)
	var queries [][]float32
	for range 30 {
		queries = append(queries, randVec(rng, 16))
	}
	require.Greater(t, recall(t, x, data, queries, 10, true), 0.95)
	// Reopened from the store.
	y, err := New(st, Options{Dims: 16, Similarity: Euclidean})
	require.NoError(t, err)
	require.Greater(t, recall(t, y, data, queries, 10, true), 0.95)
}
