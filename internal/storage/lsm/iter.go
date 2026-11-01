package lsm

import (
	"bytes"
	"container/heap"
	"context"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
)

// source is a sorted stream of (key, fragment) pairs with at most one entry
// per key. value is valid until the next positioning call.
type source interface {
	first() (key, value []byte, trailer uint64, ok bool)
	seekGE(k []byte) (key, value []byte, trailer uint64, ok bool)
	next() (key, value []byte, trailer uint64, ok bool)
	err() error
	close() error
}

// memSource iterates a memtable, merging each key's versions at or below a
// sequence number.
type memSource struct {
	m      *memTable
	merger Merger
	seq    uint64
	n      *memNode
	e      error
}

func (s *memSource) collect() (key, value []byte, trailer uint64, ok bool) {
	for s.n != nil {
		// Skip versions newer than the read's sequence number.
		if s.n.trailer>>8 > s.seq {
			s.n = s.n.next()
			continue
		}
		key, trailer, value = s.n.key, s.n.trailer, s.n.value
		for s.n = s.n.next(); s.n != nil && bytes.Equal(s.n.key, key); s.n = s.n.next() {
			v, err := s.merger.Merge(nil, value, s.n.value)
			if err != nil {
				s.e = err
				return nil, nil, 0, false
			}
			value = v
		}
		return key, value, trailer, true
	}
	return nil, nil, 0, false
}

func (s *memSource) first() ([]byte, []byte, uint64, bool) {
	s.n = s.m.first()
	return s.collect()
}

func (s *memSource) seekGE(k []byte) ([]byte, []byte, uint64, bool) {
	s.n = s.m.seekGE(k, makeTrailer(s.seq, 0xff))
	return s.collect()
}

func (s *memSource) next() ([]byte, []byte, uint64, bool) { return s.collect() }
func (s *memSource) err() error                           { return s.e }
func (s *memSource) close() error                         { return nil }

// tableSource iterates an sstable; flushes and compactions write one entry
// per key, so it needs no merging of its own.
type tableSource struct {
	it sstable.Iterator
	e  error
}

// init opens an iterator over the sstable within [lower, upper).
func (s *tableSource) init(r *tableReader, lower, upper []byte, useFilter bool) error {
	limit := sstable.NeverUseFilterBlock
	if useFilter {
		limit = sstable.AlwaysUseFilterBlock
	}
	it, err := r.r.NewPointIter(context.Background(), sstable.IterOptions{
		Lower:                lower,
		Upper:                upper,
		FilterBlockSizeLimit: limit,
		Env:                  sstable.NoReadEnv,
		ReaderProvider:       sstable.MakeTrivialReaderProvider(r.r),
	})
	if err != nil {
		return err
	}
	*s = tableSource{it: it}
	return nil
}

// newCompactionSource iterates a whole sstable for a compaction. Blocks are
// read into bp rather than the block cache, so a compaction does not evict
// the blocks reads use, and with sequential readahead.
func newCompactionSource(r *tableReader, bp *block.BufferPool) (*tableSource, error) {
	it, err := r.r.NewCompactionIter(context.Background(), sstable.IterTransforms{},
		sstable.ReadEnv{Block: block.ReadEnv{BufferPool: bp}},
		sstable.MakeTrivialReaderProvider(r.r), sstable.TableBlobContext{})
	if err != nil {
		return nil, err
	}
	return &tableSource{it: it}, nil
}

// result converts the sstable iterator's position into a source result.
// (Pebble's key-value type is internal, so its parts are passed instead.)
func (s *tableSource) result(key []byte, trailer uint64, value func([]byte) ([]byte, bool, error)) ([]byte, []byte, uint64, bool) {
	v, _, err := value(nil)
	if err != nil {
		s.e = err
		return nil, nil, 0, false
	}
	return key, v, trailer, true
}

func (s *tableSource) first() ([]byte, []byte, uint64, bool) {
	if kv := s.it.First(); kv != nil {
		return s.result(kv.K.UserKey, uint64(kv.K.Trailer), kv.Value)
	}
	return nil, nil, 0, false
}

func (s *tableSource) seekGE(k []byte) ([]byte, []byte, uint64, bool) {
	if kv := s.it.SeekGE(k, 0); kv != nil {
		return s.result(kv.K.UserKey, uint64(kv.K.Trailer), kv.Value)
	}
	return nil, nil, 0, false
}

func (s *tableSource) next() ([]byte, []byte, uint64, bool) {
	if kv := s.it.Next(); kv != nil {
		return s.result(kv.K.UserKey, uint64(kv.K.Trailer), kv.Value)
	}
	return nil, nil, 0, false
}

func (s *tableSource) err() error {
	if s.e != nil {
		return s.e
	}
	return s.it.Error()
}

func (s *tableSource) close() error { return s.it.Close() }

// mergingIter merges sources by key, combining every source's fragment of a
// key with the Merger. Key and value are valid until the next positioning
// call.
//
// Sources that hold the current key are advanced lazily, at the next
// positioning call, so a key held by one source is returned without copying
// its key or value.
type mergingIter struct {
	merger  Merger
	sources []source
	h       srcHeap
	// cur holds the entries of the current key, out of the heap; they are
	// advanced and pushed back by the next step.
	cur []*srcEntry
	// entries backs the heap's entries, one per source, reused by every
	// seek. Allocated on first use if the caller did not supply it.
	entries []srcEntry
	upper   []byte

	key, value []byte
	trailer    uint64
	e          error
}

type srcEntry struct {
	src        source
	key, value []byte
	trailer    uint64
}

type srcHeap []*srcEntry

func (h srcHeap) Len() int           { return len(h) }
func (h srcHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) < 0 }
func (h srcHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *srcHeap) Push(x any)        { *h = append(*h, x.(*srcEntry)) }
func (h *srcHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func (m *mergingIter) reset(position func(source) ([]byte, []byte, uint64, bool)) bool {
	m.h = m.h[:0]
	m.cur = m.cur[:0]
	if len(m.entries) != len(m.sources) {
		m.entries = make([]srcEntry, len(m.sources))
	}
	for i, s := range m.sources {
		if k, v, tr, ok := position(s); ok {
			m.entries[i] = srcEntry{src: s, key: k, value: v, trailer: tr}
			m.h = append(m.h, &m.entries[i])
		} else if err := s.err(); err != nil {
			m.e = err
			return false
		}
	}
	heap.Init(&m.h)
	return m.step()
}

// step advances the sources of the previous key, then assembles the smallest
// key from the heap, merging all sources' fragments for it.
func (m *mergingIter) step() bool {
	for _, e := range m.cur {
		if k, v, tr, ok := e.src.next(); ok {
			e.key, e.value, e.trailer = k, v, tr
			heap.Push(&m.h, e)
		} else if err := e.src.err(); err != nil {
			m.e = err
		}
	}
	m.cur = m.cur[:0]
	if m.e != nil || len(m.h) == 0 {
		return false
	}
	top := m.h[0]
	if m.upper != nil && bytes.Compare(top.key, m.upper) >= 0 {
		return false
	}
	heap.Pop(&m.h)
	m.cur = append(m.cur, top)
	m.key, m.value, m.trailer = top.key, top.value, top.trailer
	for len(m.h) > 0 && bytes.Equal(m.h[0].key, top.key) {
		e := heap.Pop(&m.h).(*srcEntry)
		m.cur = append(m.cur, e)
		merged, err := m.merger.Merge(nil, m.value, e.value)
		if err != nil {
			m.e = err
			return false
		}
		m.value = merged
		m.trailer = max(m.trailer, e.trailer)
	}
	return true
}

func (m *mergingIter) first() bool {
	return m.reset(func(s source) ([]byte, []byte, uint64, bool) { return s.first() })
}

func (m *mergingIter) seekGE(k []byte) bool {
	return m.reset(func(s source) ([]byte, []byte, uint64, bool) { return s.seekGE(k) })
}

func (m *mergingIter) next() bool { return m.step() }

func (m *mergingIter) close() error {
	var err error
	for _, s := range m.sources {
		if cerr := s.close(); err == nil {
			err = cerr
		}
	}
	m.sources = nil
	m.h, m.cur, m.entries = nil, nil, nil
	return err
}
