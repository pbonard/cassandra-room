// Package vecindex is the vector index of storage-attached (SAI) vector
// indexes: a proximity graph searched greedily, as Cassandra's SAI builds
// with jvector (Apache License 2.0; GraphIndexBuilder, GraphSearcher,
// ConcurrentNeighborSet), a DiskANN/Vamana-style graph:
//
//   - each vector is a node with at most M neighbors (M is
//     maximum_node_connections, 16 by default);
//   - a new node finds its candidate neighbors by a beam search of width
//     construction_beam_width (100) from the entry node, keeps a diverse
//     subset (a candidate is kept only if it is closer to the node than to
//     any neighbor already kept, relaxed by alpha up to 1.4), and is linked
//     back from them; a neighbor list may overflow M by 1.2x before it is
//     pruned back to its diverse subset;
//   - a search is a best-first beam search keeping the topK best nodes.
//
// Scores are similarities (higher is closer), as VectorSimilarityFunction:
// cosine (1 + cos) / 2, dot product (1 + dot) / 2, euclidean 1 / (1 + d²).
//
// The graph lives in a key-value store (the index's table), cached in
// memory once loaded. A deleted vector's node stays in the graph for
// routing, marked deleted, and is never returned.
package vecindex

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
)

// Similarity is a VectorSimilarityFunction.
type Similarity uint8

const (
	Cosine Similarity = iota
	DotProduct
	Euclidean
)

// Score compares two vectors: higher is more similar, in [0, 1] for
// unit vectors (dot product) and always for cosine and euclidean.
func (s Similarity) Score(a, b []float32) float32 {
	switch s {
	case DotProduct:
		return (1 + dot(a, b)) / 2
	case Euclidean:
		var d float32
		for i := range a {
			x := a[i] - b[i]
			d += x * x
		}
		return 1 / (1 + d)
	}
	na, nb := dot(a, a), dot(b, b)
	if na == 0 || nb == 0 {
		return 0
	}
	return (1 + dot(a, b)/float32(math.Sqrt(float64(na)*float64(nb)))) / 2
}

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// Store is the key-value store a graph lives in.
type Store interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	// Scan calls fn for every key with the prefix, in key order.
	Scan(prefix []byte, fn func(key, value []byte) error) error
}

// Options are an index's parameters (IndexWriterConfig).
type Options struct {
	Dims       int
	Similarity Similarity
	// MaxConnections is maximum_node_connections (default 16);
	// BeamWidth construction_beam_width (default 100).
	MaxConnections int
	BeamWidth      int
}

const (
	neighborOverflow = 1.2
	alpha            = 1.4
)

// node is a graph node.
type node struct {
	Key       []byte
	Vector    []float32
	Neighbors []uint32
	Deleted   bool
}

type meta struct {
	Entry int64 // -1 for an empty graph
	Next  uint32
	Live  int
}

// Index is a vector graph. Searches run concurrently; changes are
// serialized.
//
// Keys in the store: "M" metadata, "N" | ordinal a node, "K" | key the
// ordinal of a key's live node.
type Index struct {
	st   Store
	opts Options

	mu    sync.RWMutex
	meta  meta
	nodes map[uint32]*node
	byKey map[string]uint32
}

// New opens an index in st, loading its graph.
func New(st Store, o Options) (*Index, error) {
	if o.Dims <= 0 {
		return nil, errors.New("vecindex: dimensions must be positive")
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = 16
	}
	if o.BeamWidth <= 0 {
		o.BeamWidth = 100
	}
	x := &Index{st: st, opts: o, meta: meta{Entry: -1}, nodes: map[uint32]*node{}, byKey: map[string]uint32{}}
	if b, ok, err := st.Get([]byte("M")); err != nil {
		return nil, err
	} else if ok {
		if err := json.Unmarshal(b, &x.meta); err != nil {
			return nil, fmt.Errorf("vecindex: corrupt metadata: %w", err)
		}
	}
	err := st.Scan([]byte("N"), func(k, v []byte) error {
		n, err := decodeNode(v)
		if err != nil {
			return err
		}
		ord := binary.BigEndian.Uint32(k[1:])
		x.nodes[ord] = n
		if !n.Deleted {
			x.byKey[string(n.Key)] = ord
		}
		return nil
	})
	return x, err
}

func nodeKey(ord uint32) []byte { return binary.BigEndian.AppendUint32([]byte{'N'}, ord) }

func encodeNode(n *node) []byte {
	b := binary.AppendUvarint(nil, uint64(len(n.Key)))
	b = append(b, n.Key...)
	if n.Deleted {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(n.Vector)))
	for _, f := range n.Vector {
		b = binary.BigEndian.AppendUint32(b, math.Float32bits(f))
	}
	b = binary.AppendUvarint(b, uint64(len(n.Neighbors)))
	for _, o := range n.Neighbors {
		b = binary.AppendUvarint(b, uint64(o))
	}
	return b
}

func decodeNode(b []byte) (*node, error) {
	bad := errors.New("vecindex: corrupt node")
	n := &node{}
	l, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < l+1 {
		return nil, bad
	}
	n.Key = append([]byte(nil), b[k:k+int(l)]...)
	b = b[k+int(l):]
	n.Deleted = b[0] == 1
	b = b[1:]
	d, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < 4*d {
		return nil, bad
	}
	b = b[k:]
	n.Vector = make([]float32, d)
	for i := range n.Vector {
		n.Vector[i] = math.Float32frombits(binary.BigEndian.Uint32(b[4*i:]))
	}
	b = b[4*d:]
	c, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, bad
	}
	b = b[k:]
	for range c {
		o, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, bad
		}
		n.Neighbors = append(n.Neighbors, uint32(o))
		b = b[k:]
	}
	return n, nil
}

func (x *Index) saveNode(ord uint32) error { return x.st.Put(nodeKey(ord), encodeNode(x.nodes[ord])) }

func (x *Index) saveMeta() error {
	b, _ := json.Marshal(&x.meta)
	return x.st.Put([]byte("M"), b)
}

func (x *Index) check(v []float32) error {
	if len(v) != x.opts.Dims {
		return fmt.Errorf("vecindex: vector has %d dimensions, index %d", len(v), x.opts.Dims)
	}
	for _, f := range v {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return errors.New("vecindex: NaN and infinite values cannot be indexed")
		}
	}
	if x.opts.Similarity == Cosine && dot(v, v) == 0 {
		return errors.New("vecindex: zero and near-zero vectors cannot be indexed with cosine similarity")
	}
	return nil
}

// scored is a node and its similarity to a target.
type scored struct {
	ord   uint32
	score float32
}

// search is GraphSearcher.searchInternal: a best-first search from the
// entry node keeping the topK best accepted nodes. Every node visited is
// a candidate for expansion, deleted or not; accept filters results.
func (x *Index) search(target []float32, topK int, accept func(uint32) bool) []scored {
	if x.meta.Entry < 0 || topK <= 0 {
		return nil
	}
	sim := x.opts.Similarity
	visited := map[uint32]bool{}
	var candidates, results []scored // candidates: best last; results: worst first
	push := func(q []scored, s scored, worstFirst bool) []scored {
		i := sort.Search(len(q), func(i int) bool {
			if worstFirst {
				return q[i].score > s.score
			}
			return q[i].score >= s.score
		})
		return slices.Insert(q, i, s)
	}
	ep := uint32(x.meta.Entry)
	visited[ep] = true
	s := scored{ep, sim.Score(target, x.nodes[ep].Vector)}
	candidates = push(candidates, s, false)
	if accept(ep) {
		results = push(results, s, true)
	}
	minAccepted := float32(math.Inf(-1))
	if len(results) >= topK {
		minAccepted = results[0].score
	}
	for len(candidates) > 0 {
		top := candidates[len(candidates)-1]
		if top.score < minAccepted {
			break
		}
		candidates = candidates[:len(candidates)-1]
		for _, nb := range x.nodes[top.ord].Neighbors {
			if visited[nb] {
				continue
			}
			visited[nb] = true
			n := x.nodes[nb]
			if n == nil {
				continue
			}
			s := scored{nb, sim.Score(target, n.Vector)}
			if s.score < minAccepted {
				continue
			}
			candidates = push(candidates, s, false)
			if accept(nb) {
				results = push(results, s, true)
				if len(results) > topK {
					results = results[1:]
				}
				if len(results) >= topK {
					minAccepted = results[0].score
				}
			}
		}
	}
	slices.Reverse(results)
	return results
}

// diverse selects up to M diverse neighbors among candidates sorted best
// first (ConcurrentNeighborSet.selectDiverse): a candidate is kept if it
// is more similar to the node than alpha times its similarity to any kept
// neighbor, alpha growing from 1 to the limit so that the nearest are
// preferred.
func (x *Index) diverse(cands []scored) []scored {
	m := x.opts.MaxConnections
	selected := make([]bool, len(cands))
	n := 0
	for a := float32(1); a <= alpha+1e-6 && n < m; a += 0.2 {
		for i, c := range cands {
			if n >= m {
				break
			}
			if selected[i] {
				continue
			}
			ok := true
			for j := range cands {
				if !selected[j] {
					continue
				}
				if x.opts.Similarity.Score(x.nodes[c.ord].Vector, x.nodes[cands[j].ord].Vector) > c.score*a {
					ok = false
					break
				}
			}
			if ok {
				selected[i] = true
				n++
			}
		}
	}
	var out []scored
	for i, c := range cands {
		if selected[i] {
			out = append(out, c)
		}
	}
	return out
}

func (x *Index) neighborsScored(ord uint32) []scored {
	n := x.nodes[ord]
	out := make([]scored, 0, len(n.Neighbors))
	for _, nb := range n.Neighbors {
		if m := x.nodes[nb]; m != nil {
			out = append(out, scored{nb, x.opts.Similarity.Score(n.Vector, m.Vector)})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].score > out[b].score })
	return out
}

func ords(s []scored) []uint32 {
	out := make([]uint32, len(s))
	for i, x := range s {
		out[i] = x.ord
	}
	return out
}

// Insert adds or replaces key's vector (GraphIndexBuilder.addGraphNode).
func (x *Index) Insert(key []byte, vec []float32) error {
	if err := x.check(vec); err != nil {
		return err
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if old, ok := x.byKey[string(key)]; ok {
		if slices.Equal(x.nodes[old].Vector, vec) {
			return nil
		}
		if err := x.deleteLocked(old); err != nil {
			return err
		}
	}
	ord := x.meta.Next
	x.meta.Next++
	n := &node{Key: append([]byte(nil), key...), Vector: append([]float32(nil), vec...)}
	x.nodes[ord] = n
	dirty := map[uint32]bool{ord: true}
	if x.meta.Entry < 0 {
		x.meta.Entry = int64(ord)
	} else {
		// Natural candidates from a beam search, then the diverse subset.
		cands := x.search(vec, x.opts.BeamWidth, func(o uint32) bool { return o != ord })
		chosen := x.diverse(cands)
		n.Neighbors = ords(chosen)
		// Backlinks, pruned to the diverse subset beyond the overflow.
		limit := int(neighborOverflow * float32(x.opts.MaxConnections))
		for _, c := range chosen {
			m := x.nodes[c.ord]
			if slices.Contains(m.Neighbors, ord) {
				continue
			}
			m.Neighbors = append(m.Neighbors, ord)
			if len(m.Neighbors) > limit {
				m.Neighbors = ords(x.diverse(x.neighborsScored(c.ord)))
			}
			dirty[c.ord] = true
		}
	}
	x.byKey[string(key)] = ord
	x.meta.Live++
	for o := range dirty {
		if err := x.saveNode(o); err != nil {
			return err
		}
	}
	if err := x.st.Put(append([]byte{'K'}, key...), binary.BigEndian.AppendUint32(nil, ord)); err != nil {
		return err
	}
	return x.saveMeta()
}

// Delete removes key's vector: its node stays for routing, marked deleted.
func (x *Index) Delete(key []byte) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	ord, ok := x.byKey[string(key)]
	if !ok {
		return nil
	}
	if err := x.deleteLocked(ord); err != nil {
		return err
	}
	return x.saveMeta()
}

func (x *Index) deleteLocked(ord uint32) error {
	n := x.nodes[ord]
	n.Deleted = true
	delete(x.byKey, string(n.Key))
	x.meta.Live--
	if err := x.st.Delete(append([]byte{'K'}, n.Key...)); err != nil {
		return err
	}
	return x.saveNode(ord)
}

// Len returns the number of live vectors.
func (x *Index) Len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.meta.Live
}

// Result is a search result: a key and its similarity to the query.
type Result struct {
	Key   []byte
	Score float32
}

// Search returns up to topK live keys most similar to the query, best
// first.
func (x *Index) Search(query []float32, topK int) ([]Result, error) {
	if len(query) != x.opts.Dims {
		return nil, fmt.Errorf("vecindex: query has %d dimensions, index %d", len(query), x.opts.Dims)
	}
	x.mu.RLock()
	defer x.mu.RUnlock()
	res := x.search(query, topK, func(o uint32) bool { return !x.nodes[o].Deleted })
	out := make([]Result, len(res))
	for i, r := range res {
		out[i] = Result{Key: x.nodes[r.ord].Key, Score: r.score}
	}
	return out, nil
}

// TopKFor is how many results to ask the graph for a LIMIT
// (OptimizeFor.topKFor): searching wider than the limit improves recall.
func TopKFor(limit int, recall bool) int {
	f := 0.979 + 4.021*math.Pow(float64(limit), -0.761) // LATENCY
	if recall {
		f = 0.509 + 9.491*math.Pow(float64(limit), -0.402)
	}
	return int(math.Max(1, f) * float64(limit))
}
