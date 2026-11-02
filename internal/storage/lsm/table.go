package lsm

import (
	"bytes"
	"cmp"
	"math"
	"slices"
	"sync/atomic"
	"time"
)

// Table is one table's store: its memtables and sstables, in its own
// directory. Writes go through Engine.Apply; reads go through the Table.
type Table struct {
	e   *Engine
	id  TableID
	dir string

	// state is what readers use; it is published while holding Engine.mu.
	state   atomic.Pointer[readState]
	dropped atomic.Bool

	// Guarded by Engine.mu.
	mem        *memTable
	imm        []*immMemTable // oldest first
	files      []*fileMeta    // newest first (by maxSeq)
	flushedSeq uint64
	opts       TableOptions
	// flushSize is a moving average of flush output sizes, UCS's m.
	flushSize float64
	// lastExpiredCheck is when fully expired sstables were last looked for.
	lastExpiredCheck time.Time
	// highestWindowSeen is the time-window mode's current window: the
	// newest seen, never moving back.
	highestWindowSeen int64
	windowSeen        bool
	// recentWindows are past windows not yet compacted into one run since
	// they were the newest (ScyllaDB's recent_active_windows).
	recentWindows map[int64]bool
	// majorPending pauses background compactions while Compact waits.
	majorPending bool
}

// immMemTable is a memtable waiting to be flushed.
type immMemTable struct {
	mem *memTable
	// boundary is the last sequence number assigned when the memtable was
	// rotated: every entry of the table at or below it is in this memtable or
	// an older one.
	boundary uint64
}

// flushBoundaries returns the shard boundaries a flush of mem is split at:
// UCS shards flush output like compaction output, by its density (the
// memtable covers the whole token space).
func (t *Table) flushBoundaries(mem *memTable) [][]byte {
	if t.opts.TimeWindow > 0 {
		return nil
	}
	return shardBoundaries(t.opts.numShards(float64(mem.approximateSize())))
}

// readState is an immutable view of a table's memtables and sstables. It is
// reference counted: the table holds one reference while the state is
// current, and each reader holds one while it reads. The state holds a
// reference on each of its files, so a file replaced by a compaction is
// closed and deleted only after its last reader finishes.
type readState struct {
	mems []*memTable // newest first
	// files are ordered by maximum timestamp, highest first, so a read can
	// stop once a row deletion shadows everything that remains.
	files []*fileMeta
	refs  atomic.Int32
}

// acquireState returns the current state with a reference held.
func (t *Table) acquireState() *readState {
	for {
		s := t.state.Load()
		if r := s.refs.Load(); r > 0 && s.refs.CompareAndSwap(r, r+1) {
			return s
		}
		// The state was released after we loaded it; a newer one is
		// already published.
	}
}

// release drops a reference; the last one releases the state's files.
func (s *readState) release() {
	if s.refs.Add(-1) == 0 {
		for _, f := range s.files {
			f.unref()
		}
	}
}

// ID returns the table's ID.
func (t *Table) ID() TableID { return t.id }

// publishLocked publishes the table's current memtables and files to
// readers. Engine.mu must be held.
func (t *Table) publishLocked() {
	s := &readState{
		mems:  make([]*memTable, 0, len(t.imm)+1),
		files: slices.Clone(t.files),
	}
	slices.SortStableFunc(s.files, func(a, b *fileMeta) int { return cmp.Compare(b.maxTS, a.maxTS) })
	s.mems = append(s.mems, t.mem)
	for i := len(t.imm) - 1; i >= 0; i-- {
		s.mems = append(s.mems, t.imm[i].mem)
	}
	for _, f := range s.files {
		f.refs.Add(1)
	}
	s.refs.Store(1)
	if old := t.state.Swap(s); old != nil {
		old.release()
	}
}

// releaseStateLocked drops the table's reference on its current state, when
// the table is dropped or the engine closes. Readers that still hold the old
// state keep its files open until they finish.
func (t *Table) releaseStateLocked() {
	empty := &readState{}
	empty.refs.Store(1)
	if old := t.state.Swap(empty); old != nil {
		old.release()
	}
}

// memtableBytesLocked returns the memory used by the table's memtables.
func (t *Table) memtableBytesLocked() int64 {
	n := t.mem.approximateSize()
	for _, im := range t.imm {
		n += im.mem.approximateSize()
	}
	return n
}

// Get returns the row stored under key: the merge of all its fragments in
// memtables and sstables (Cassandra's single-partition read). It returns
// ErrNotFound if the key has no fragments. The row may hold only
// tombstones; interpreting it is up to the caller (see the row package).
//
// Get must not run concurrently with DropTable for the same table; the
// schema layer serializes them.
func (t *Table) Get(key []byte) ([]byte, error) {
	if t.dropped.Load() {
		return nil, ErrUnknownTable
	}
	// Load the state before the sequence number: every sstable in the state
	// holds only entries at or below the sequence number loaded after it.
	s := t.acquireState()
	defer s.release()
	return t.getFromState(s, t.e.visibleSeq.Load(), key)
}

// getFromState looks key up in a read state, ignoring memtable entries
// above seq.
func (t *Table) getFromState(s *readState, seq uint64, key []byte) ([]byte, error) {
	merger := t.e.opts.Merger
	var acc []byte
	found := false
	for _, m := range s.mems {
		var ok bool
		var err error
		if acc, ok, err = m.getMerged(merger, key, seq, acc); err != nil {
			return nil, err
		}
		found = found || ok
	}
	deletedAt := int64(math.MinInt64)
	if found {
		var err error
		if deletedAt, err = merger.DeletedAt(acc); err != nil {
			return nil, err
		}
	}
	for _, f := range s.files {
		// Files are ordered by maximum timestamp: once a row deletion is
		// newer than this file's newest data, it shadows every remaining
		// file. The comparison is strict: a deletion with the same timestamp
		// in a remaining file could still supersede it (by local deletion
		// time), and a file without timestamps has maxTS = MinInt64.
		if found && f.maxTS < deletedAt {
			break
		}
		if !f.mayContain(key) {
			continue
		}
		var ok bool
		var err error
		if acc, ok, err = f.reader.getMerged(merger, key, acc); err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		found = true
		if deletedAt, err = merger.DeletedAt(acc); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, ErrNotFound
	}
	return acc, nil
}

// Flush flushes the table's mutable memtable and waits until all of the
// table's memtables are written to sstables.
func (t *Table) Flush() error {
	e := t.e
	e.commitMu.Lock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkUsableLocked(); err != nil {
		e.commitMu.Unlock()
		return err
	}
	if t.dropped.Load() {
		e.commitMu.Unlock()
		return ErrUnknownTable
	}
	e.rotateLocked(t)
	e.commitMu.Unlock()
	for len(t.imm) > 0 && !t.dropped.Load() {
		if err := e.checkUsableLocked(); err != nil {
			return err
		}
		e.mu.cond.Wait()
	}
	if t.dropped.Load() {
		return ErrUnknownTable
	}
	return nil
}

// Iterator iterates a table's rows in key order within [lower, upper),
// merging each row's fragments (Cassandra's partition-range and slice
// reads). Key and Value are valid until the next positioning call. An
// Iterator is not safe for concurrent use.
type Iterator struct {
	s            *readState
	m            mergingIter
	lower, upper []byte
	valid        bool

	// Inline storage, so that an iterator over a few memtables and sstables
	// needs one allocation (slices grow past it if needed).
	buf struct {
		sources [iterInlineSources]source
		mems    [2]memSource
		tables  [iterInlineSources]tableSource
		entries [iterInlineSources]srcEntry
		heap    [iterInlineSources]*srcEntry
		cur     [iterInlineSources]*srcEntry
	}
}

const iterInlineSources = 4

// NewIter returns an iterator over [lower, upper); nil bounds are open. It
// reads a consistent view: writes after NewIter returns are not visible.
func (t *Table) NewIter(lower, upper []byte) (*Iterator, error) {
	if t.dropped.Load() {
		return nil, ErrUnknownTable
	}
	s := t.acquireState()
	return t.newIterFromState(s, t.e.visibleSeq.Load(), lower, upper)
}

// newIterFromState returns an iterator over a read state, ignoring
// memtable entries above seq. It takes over the caller's reference on s.
func (t *Table) newIterFromState(s *readState, seq uint64, lower, upper []byte) (*Iterator, error) {
	it := &Iterator{s: s, lower: lower, upper: upper}
	it.m.merger = t.e.opts.Merger
	it.m.upper = upper
	it.m.sources = it.buf.sources[:0]
	// Sources point into these slices, so they must not be reallocated
	// after the first pointer is taken: size them up front.
	mems := it.buf.mems[:0]
	if len(s.mems) > len(it.buf.mems) {
		mems = make([]memSource, 0, len(s.mems))
	}
	for _, m := range s.mems {
		if m.empty() {
			continue
		}
		mems = append(mems, memSource{m: m, merger: it.m.merger, seq: seq})
		it.m.sources = append(it.m.sources, &mems[len(mems)-1])
	}
	tables := it.buf.tables[:0]
	if len(s.files) > len(it.buf.tables) {
		tables = make([]tableSource, 0, len(s.files))
	}
	for _, f := range s.files {
		if (upper != nil && bytes.Compare(f.smallest.UserKey, upper) >= 0) ||
			(lower != nil && bytes.Compare(f.largest.UserKey, lower) < 0) {
			continue
		}
		tables = append(tables, tableSource{})
		src := &tables[len(tables)-1]
		if err := src.init(f.reader, lower, upper, false); err != nil {
			it.Close()
			return nil, err
		}
		it.m.sources = append(it.m.sources, src)
	}
	n := len(it.m.sources)
	if n <= iterInlineSources {
		it.m.entries, it.m.h, it.m.cur = it.buf.entries[:n], it.buf.heap[:0], it.buf.cur[:0]
	}
	return it, nil
}

// First moves to the first row at or after the lower bound.
func (it *Iterator) First() bool {
	if it.lower != nil {
		return it.SeekGE(it.lower)
	}
	it.valid = it.m.first()
	return it.valid
}

// SeekGE moves to the first row at or after key (and the lower bound).
func (it *Iterator) SeekGE(key []byte) bool {
	if it.lower != nil && bytes.Compare(key, it.lower) < 0 {
		key = it.lower
	}
	it.valid = it.m.seekGE(key)
	return it.valid
}

// Next moves to the next row.
func (it *Iterator) Next() bool {
	if !it.valid {
		return false
	}
	it.valid = it.m.next()
	return it.valid
}

// Valid reports whether the iterator is at a row.
func (it *Iterator) Valid() bool { return it.valid }

// Key returns the current row's key.
func (it *Iterator) Key() []byte { return it.m.key }

// Value returns the current row: the merge of its fragments.
func (it *Iterator) Value() []byte { return it.m.value }

// Error returns the first error the iterator hit.
func (it *Iterator) Error() error { return it.m.e }

// Close releases the iterator's sources and its view of the table.
func (it *Iterator) Close() error {
	err := it.m.close()
	if it.s != nil {
		it.s.release()
		it.s = nil
	}
	return err
}
