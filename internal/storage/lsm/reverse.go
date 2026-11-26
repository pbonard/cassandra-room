package lsm

import (
	"bytes"
	"container/heap"

	"github.com/cockroachdb/pebble"
)

// Reverse iteration: rows from the upper bound down, as a query in the
// opposite of the stored clustering order reads them (ORDER BY ... DESC on
// an ascending table): it reads only the rows it returns, not the whole
// range before them.

// revSource is a source iterated backwards.
type revSource interface {
	// last moves to the last row below upper (nil: the last row).
	last(upper []byte) (key, value []byte, ok bool)
	prev() (key, value []byte, ok bool)
	err() error
	close() error
}

// seekLT returns the last node whose key is below key (the last node if
// key is nil), or nil.
func (m *memTable) seekLT(key []byte) *memNode {
	x := m.head
	for level := int(m.height.Load()) - 1; level >= 0; level-- {
		for {
			nx := x.tower[level].Load()
			if nx == nil || key != nil && bytes.Compare(nx.key, key) >= 0 {
				break
			}
			x = nx
		}
	}
	if x == m.head {
		return nil
	}
	return x
}

// revMemSource iterates a memtable backwards, merging each key's versions
// at or below a sequence number.
type revMemSource struct {
	fwd memSource
	key []byte
}

// at returns the visible, merged row of the last key below bound, going
// further back past keys with no visible version.
func (s *revMemSource) at(bound []byte) ([]byte, []byte, bool) {
	for {
		n := s.fwd.m.seekLT(bound)
		if n == nil {
			return nil, nil, false
		}
		// The key's versions, newest first: those the read sees, merged.
		s.fwd.n = s.fwd.m.seekGE(n.key, makeTrailer(s.fwd.seq, pebble.InternalKeyKindMax))
		k, v, _, ok := s.fwd.collect()
		if s.fwd.e != nil {
			return nil, nil, false
		}
		if ok && bytes.Equal(k, n.key) {
			s.key = k
			return k, v, true
		}
		bound = n.key // only newer versions: not visible here
	}
}

func (s *revMemSource) last(upper []byte) ([]byte, []byte, bool) { return s.at(upper) }
func (s *revMemSource) prev() ([]byte, []byte, bool)             { return s.at(s.key) }
func (s *revMemSource) err() error                               { return s.fwd.e }
func (s *revMemSource) close() error                             { return nil }

// revTableSource iterates an sstable backwards (one entry per key).
type revTableSource struct{ tableSource }

func (s *revTableSource) last(upper []byte) ([]byte, []byte, bool) {
	if upper == nil {
		kv := s.it.Last()
		if kv == nil {
			return nil, nil, false
		}
		k, v, _, ok := s.result(kv.K.UserKey, uint64(kv.K.Trailer), kv.Value)
		return k, v, ok
	}
	kv := s.it.SeekLT(upper, 0)
	if kv == nil {
		return nil, nil, false
	}
	k, v, _, ok := s.result(kv.K.UserKey, uint64(kv.K.Trailer), kv.Value)
	return k, v, ok
}

func (s *revTableSource) prev() ([]byte, []byte, bool) {
	kv := s.it.Prev()
	if kv == nil {
		return nil, nil, false
	}
	k, v, _, ok := s.result(kv.K.UserKey, uint64(kv.K.Trailer), kv.Value)
	return k, v, ok
}

// revEntry is a source's current row in the reverse merge.
type revEntry struct {
	src        revSource
	key, value []byte
}

// revHeap is a max-heap by key.
type revHeap []*revEntry

func (h revHeap) Len() int           { return len(h) }
func (h revHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) > 0 }
func (h revHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *revHeap) Push(x any)        { *h = append(*h, x.(*revEntry)) }
func (h *revHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// ReverseIterator iterates a table's rows in [lower, upper) from the last
// one down. Key and Value are valid until the next positioning call.
type ReverseIterator struct {
	s            *readState
	merger       Merger
	sources      []revSource
	h            revHeap
	cur          []*revEntry
	lower, upper []byte
	key, value   []byte
	valid        bool
	e            error
}

// NewReverseIter returns a reverse iterator over [lower, upper); nil bounds
// are open. It reads a consistent view, as NewIter does.
func (t *Table) NewReverseIter(lower, upper []byte) (*ReverseIterator, error) {
	if t.dropped.Load() {
		return nil, ErrUnknownTable
	}
	s := t.acquireState()
	seq := t.e.visibleSeq.Load()
	it := &ReverseIterator{s: s, merger: t.e.opts.Merger, lower: lower, upper: upper}
	for _, m := range s.mems {
		if !m.empty() {
			it.sources = append(it.sources, &revMemSource{fwd: memSource{m: m, merger: it.merger, seq: seq}})
		}
	}
	files, err := t.readFiles(s, lower, upper)
	if err != nil {
		it.Close()
		return nil, err
	}
	for _, f := range files {
		src := &revTableSource{}
		if err := src.init(f.reader, lower, upper, false); err != nil {
			it.Close()
			return nil, err
		}
		it.sources = append(it.sources, src)
	}
	return it, nil
}

// Last moves to the last row below the upper bound.
func (it *ReverseIterator) Last() bool { return it.SeekLT(it.upper) }

// SeekLT moves to the last row below key (and the upper bound).
func (it *ReverseIterator) SeekLT(key []byte) bool {
	if key == nil || it.upper != nil && bytes.Compare(key, it.upper) > 0 {
		key = it.upper
	}
	it.h, it.cur = it.h[:0], it.cur[:0]
	for _, s := range it.sources {
		if k, v, ok := s.last(key); ok {
			it.h = append(it.h, &revEntry{src: s, key: k, value: v})
		} else if err := s.err(); err != nil {
			it.e = err
			it.valid = false
			return false
		}
	}
	heap.Init(&it.h)
	it.valid = it.step()
	return it.valid
}

// Prev moves to the previous row.
func (it *ReverseIterator) Prev() bool {
	if !it.valid {
		return false
	}
	it.valid = it.step()
	return it.valid
}

// step moves the sources of the current key back, then assembles the
// largest key left, merging every source's fragment of it.
func (it *ReverseIterator) step() bool {
	for _, e := range it.cur {
		if k, v, ok := e.src.prev(); ok {
			e.key, e.value = k, v
			heap.Push(&it.h, e)
		} else if err := e.src.err(); err != nil {
			it.e = err
		}
	}
	it.cur = it.cur[:0]
	if it.e != nil || len(it.h) == 0 {
		return false
	}
	top := it.h[0]
	if it.lower != nil && bytes.Compare(top.key, it.lower) < 0 {
		return false
	}
	heap.Pop(&it.h)
	it.cur = append(it.cur, top)
	it.key, it.value = top.key, top.value
	for len(it.h) > 0 && bytes.Equal(it.h[0].key, top.key) {
		e := heap.Pop(&it.h).(*revEntry)
		it.cur = append(it.cur, e)
		merged, err := it.merger.Merge(nil, it.value, e.value)
		if err != nil {
			it.e = err
			return false
		}
		it.value = merged
	}
	return true
}

// Valid reports whether the iterator is at a row.
func (it *ReverseIterator) Valid() bool { return it.valid }

// Key returns the current row's key.
func (it *ReverseIterator) Key() []byte { return it.key }

// Value returns the current row: the merge of its fragments.
func (it *ReverseIterator) Value() []byte { return it.value }

// Error returns the first error the iterator hit.
func (it *ReverseIterator) Error() error { return it.e }

// Close releases the iterator's sources and its view of the table.
func (it *ReverseIterator) Close() error {
	var err error
	for _, s := range it.sources {
		if cerr := s.close(); err == nil {
			err = cerr
		}
	}
	it.sources, it.h, it.cur = nil, nil, nil
	if it.s != nil {
		it.s.release()
		it.s = nil
	}
	return err
}
