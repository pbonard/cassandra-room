package lsm

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// Engine is a node's storage: one commit log (WAL) and memtable memory
// budget shared by all tables, and a store per table (Cassandra's
// architecture). It is safe for concurrent use.
//
// Lock order: commitMu, then manifestMu, then mu.
type Engine struct {
	opts       Options
	fs         vfs.FS
	dir        string
	tablesDir  string
	lock       io.Closer
	writerOpts sstable.WriterOptions
	readerOpts sstable.ReaderOptions
	merger     Merger
	// unrefCache releases the engine's reference to the block cache.
	unrefCache func()

	// commitMu serializes writes: WAL appends and memtable inserts happen in
	// sequence-number order, and every memtable has a single writer.
	commitMu sync.Mutex
	lastSeq  uint64 // guarded by commitMu
	// visibleSeq is the highest sequence number whose batch is fully applied.
	// Readers ignore memtable entries above it, so batches appear atomically.
	visibleSeq atomic.Uint64
	// memUsed is the memory used by all memtables, including those waiting
	// to be flushed.
	memUsed atomic.Int64

	// manifestMu serializes manifest writes, which happen without mu held.
	manifestMu sync.Mutex

	// Read by Apply without mu, so the common write path takes only commitMu.
	// tables is a snapshot of mu.tables, republished when it changes.
	tables atomic.Pointer[map[TableID]*Table]
	// immBytes is the memory used by memtables waiting to be flushed.
	immBytes atomic.Int64
	// closedWALBytes is the size of live WAL segments other than the current.
	closedWALBytes atomic.Int64
	// unusable is set once the engine is closed or has a background error.
	unusable atomic.Bool

	mu struct {
		sync.Mutex
		cond   sync.Cond
		tables map[TableID]*Table
		wal    *walWriter
		// walSegs are the live WAL segments, oldest first; the last one is
		// being written.
		walSegs []walSegment
		// recycledWAL holds obsolete WAL segments kept for reuse, oldest
		// first (at most walRecycleLimit).
		recycledWAL []uint64
		manifest    *manifestWriter
		nextFileNum uint64
		// flushQueue holds rotated memtables in rotation order.
		flushQueue  []flushJob
		flushing    bool
		compactions int // running
		bgErr       error
		closed      bool
	}
	flushWG   sync.WaitGroup
	compactWG sync.WaitGroup
	// testAfterEarlyRelease, if set, runs after a compaction swaps out inputs
	// before finishing (with the engine mutexes released).
	testAfterEarlyRelease func()
	// compactionSem bounds the compaction tasks (output shards) running at
	// once, across compactions (Cassandra: concurrent_compactors).
	compactionSem chan struct{}
	metrics       struct {
		bytesFlushed, bytesCompacted atomic.Int64
		flushes, compactions         atomic.Int64
		tombstoneCompactions         atomic.Int64
		earlyReleases                atomic.Int64
		expiredDropped               atomic.Int64
	}

	// The periodic WAL syncer (WALSyncPeriod).
	syncStop, syncDone chan struct{}
}

type walSegment struct {
	num  uint64
	size int64 // final size; the current segment's size is in mu.wal
}

type flushJob struct {
	t   *Table
	imm *immMemTable
}

// Open opens or creates the engine in dir.
func Open(dir string, opts *Options) (*Engine, error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	o.ensureDefaults()
	e := &Engine{
		opts:       o,
		fs:         o.FS,
		dir:        dir,
		tablesDir:  o.FS.PathJoin(dir, tablesDirName),
		writerOpts: makeWriterOptions(&o),
		readerOpts: makeReaderOptions(),
		merger:     o.Merger,
	}
	e.compactionSem = make(chan struct{}, o.MaxConcurrentCompactions)
	// One block cache and handle per engine; blocks are keyed by file number.
	c := pebble.NewCache(o.CacheSize)
	e.readerOpts.CacheOpts.CacheHandle = c.NewHandle()
	e.unrefCache = c.Unref
	e.mu.cond.L = &e.mu.Mutex
	e.mu.tables = map[TableID]*Table{}
	fail := func(err error) (*Engine, error) {
		e.closeFiles()
		if e.lock != nil {
			e.lock.Close()
		}
		e.releaseCache()
		return nil, err
	}
	if err := e.fs.MkdirAll(e.tablesDir, 0755); err != nil {
		return fail(err)
	}
	// Make the directories durable: each one's entry lives in its parent.
	for _, d := range []string{e.fs.PathDir(dir), dir} {
		if err := syncDir(e.fs, d); err != nil {
			return fail(err)
		}
	}
	lock, err := e.fs.Lock(e.fs.PathJoin(dir, "LOCK"))
	if err != nil {
		return fail(fmt.Errorf("lsm: locking %s: %w", dir, err))
	}
	e.lock = lock
	if err := e.recover(); err != nil {
		return fail(err)
	}
	e.mu.Lock()
	e.maybeScheduleCompactionsLocked()
	e.mu.Unlock()
	if o.WALSyncPeriod > 0 {
		e.syncStop = make(chan struct{})
		e.syncDone = make(chan struct{})
		go e.periodicSync(o.WALSyncPeriod)
	}
	return e, nil
}

func (e *Engine) newTable(id TableID) *Table {
	t := &Table{e: e, id: id, dir: e.fs.PathJoin(e.tablesDir, id.dirName())}
	t.mem = newMemTable(&e.memUsed)
	var o TableOptions
	if e.opts.TableOptions != nil {
		o = e.opts.TableOptions(id)
	}
	o.ensureDefaults()
	t.opts.Store(&o)
	return t
}

// recover loads the manifest, opens every table's sstables, replays the WAL
// into memtables, flushes them, and starts a new manifest and WAL segment.
func (e *Engine) recover() error {
	state, err := e.loadState()
	if err != nil {
		return err
	}
	nextFileNum := max(state.nextFileNum, 1)
	lastSeq := state.lastSeq
	bump := func(num uint64) { nextFileNum = max(nextFileNum, num+1) }

	for id, tm := range state.tables {
		t := e.newTable(id)
		t.flushedSeq = tm.flushedSeq
		// Sequence numbers resume above every table's flushed sequence
		// number, not only above the data that remains: purging can remove
		// the newest rows, and a reused sequence number at or below a
		// table's flushed one would be skipped by a later WAL replay.
		lastSeq = max(lastSeq, tm.flushedSeq)
		for _, f := range tm.files {
			if err := e.openFile(t, f); err != nil {
				return fmt.Errorf("lsm: opening table %d file %d: %w", id, f.num, err)
			}
			t.files = append(t.files, f)
			lastSeq = max(lastSeq, f.maxSeq)
			bump(f.num)
		}
		sortFilesNewestFirst(t.files)
		e.mu.tables[id] = t
	}

	// File numbers are global; make sure new ones exceed every file on disk,
	// including files no manifest records.
	rootNames, err := e.fs.List(e.dir)
	if err != nil {
		return err
	}
	var wals []uint64
	for _, name := range rootNames {
		if kind, num, ok := parseFileName(name); ok {
			bump(num)
			if kind == "wal" && num >= state.minWAL {
				wals = append(wals, num)
			}
		}
	}
	slices.Sort(wals)
	tableDirs, err := e.fs.List(e.tablesDir)
	if err != nil {
		return err
	}
	for _, d := range tableDirs {
		names, _ := e.fs.List(e.fs.PathJoin(e.tablesDir, d))
		for _, name := range names {
			if _, num, ok := parseFileName(name); ok {
				bump(num)
			}
		}
	}

	// Replay the WAL. An entry is applied only if its table exists and the
	// table has not flushed past it.
	recovered := map[TableID]*memTable{}
	var scratch atomic.Int64
	for _, num := range wals {
		err := replayWAL(e.fs, e.fs.PathJoin(e.dir, walFileName(num)), num, func(data []byte) error {
			if len(data) == 0 {
				return nil // a periodic sync marker
			}
			br, err := newBatchReader(data)
			if err != nil {
				return err
			}
			for seq := br.seq; ; seq++ {
				ent, ok, err := br.next()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				lastSeq = max(lastSeq, seq)
				t := e.mu.tables[ent.table]
				if t == nil || seq <= t.flushedSeq {
					continue
				}
				m := recovered[ent.table]
				if m == nil {
					m = newMemTable(&scratch)
					recovered[ent.table] = m
				}
				m.add(ent.key, makeTrailer(seq, ent.kind), ent.value)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Flush what was recovered; afterwards every table has flushed up to
	// lastSeq, so the old WAL segments are no longer needed.
	for id, m := range recovered {
		t := e.mu.tables[id]
		alloc := func() uint64 { nextFileNum++; return nextFileNum - 1 }
		metas, err := writeTable(e.fs, t.dir, alloc, e.writerOpts, e.merger, m, t.flushBoundaries(m), t.o().fragmentSize(), t.timeSplitter(), e.opts.Now().Unix())
		if err != nil {
			return err
		}
		for _, meta := range metas {
			if err := e.openFile(t, meta); err != nil {
				return err
			}
		}
		if err := syncDir(e.fs, t.dir); err != nil {
			return err
		}
		t.files = append(slices.Clone(metas), t.files...)
	}

	walNum := nextFileNum
	nextFileNum++
	wal, err := createWAL(e.fs, e.dir, walNum, 0, e.opts.WALSegmentSize)
	if err != nil {
		return err
	}
	e.mu.wal = wal
	e.mu.walSegs = []walSegment{{num: walNum}}
	manifestNum := nextFileNum
	nextFileNum++
	snapshot := &versionEdit{nextFileNum: nextFileNum, lastSeq: lastSeq, minWAL: walNum}
	for id, t := range e.mu.tables {
		t.flushedSeq = lastSeq
		snapshot.tables = append(snapshot.tables, &tableEdit{
			id: id, created: true, added: slices.Clone(t.files), flushedSeq: lastSeq,
		})
	}
	m, err := createManifest(e.fs, e.dir, manifestNum, snapshot)
	if err != nil {
		return err
	}
	e.mu.manifest = m
	e.mu.nextFileNum = nextFileNum
	e.lastSeq = lastSeq
	e.visibleSeq.Store(lastSeq)
	for _, t := range e.mu.tables {
		t.publishLocked()
	}
	e.publishTablesLocked()

	// Remove what the new manifest no longer references: old WAL segments and
	// manifests, directories of dropped tables, and sstables from flushes that
	// crashed before they were recorded.
	for _, name := range rootNames {
		kind, num, ok := parseFileName(name)
		if (ok && ((kind == "wal" && num < walNum) || (kind == "manifest" && num != manifestNum))) ||
			name == currentFileName+".tmp" {
			_ = e.fs.Remove(e.fs.PathJoin(e.dir, name))
		}
	}
	for _, d := range tableDirs {
		var t *Table
		for _, tt := range e.mu.tables {
			if tt.id.dirName() == d {
				t = tt
			}
		}
		path := e.fs.PathJoin(e.tablesDir, d)
		if t == nil {
			_ = e.fs.RemoveAll(path)
			continue
		}
		live := map[uint64]bool{}
		for _, f := range t.files {
			live[f.num] = true
		}
		names, _ := e.fs.List(path)
		for _, name := range names {
			if kind, num, ok := parseFileName(name); ok && kind == "sst" && !live[num] {
				_ = e.fs.Remove(e.fs.PathJoin(path, name))
			}
		}
	}
	return nil
}

func (e *Engine) loadState() (*manifestState, error) {
	state, exists, err := loadManifest(e.fs, e.dir)
	if err != nil {
		return nil, err
	}
	if !exists {
		state = newManifestState()
	}
	return state, nil
}

func sortFilesNewestFirst(files []*fileMeta) {
	slices.SortFunc(files, func(a, b *fileMeta) int {
		switch {
		case a.maxSeq > b.maxSeq:
			return -1
		case a.maxSeq < b.maxSeq:
			return 1
		}
		return 0
	})
}

// publishTablesLocked publishes a snapshot of mu.tables for Apply.
func (e *Engine) publishTablesLocked() {
	m := make(map[TableID]*Table, len(e.mu.tables))
	for id, t := range e.mu.tables {
		m[id] = t
	}
	e.tables.Store(&m)
}

func (e *Engine) setBackgroundErrorLocked(err error) {
	if e.mu.bgErr == nil {
		e.mu.bgErr = err
	}
	e.unusable.Store(true)
	e.mu.cond.Broadcast()
}

func (e *Engine) checkUsableLocked() error {
	if e.mu.closed {
		return ErrClosed
	}
	return e.mu.bgErr
}

// CreateTable creates a table and returns its store.
func (e *Engine) CreateTable(id TableID) (*Table, error) {
	// commitMu fixes lastSeq: WAL entries at or below it belong to earlier
	// tables with the same ID, if any, and must never be replayed into this
	// one.
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if err := e.checkUsableLocked(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if _, ok := e.mu.tables[id]; ok {
		e.mu.Unlock()
		return nil, ErrTableExists
	}
	manifest := e.mu.manifest
	e.mu.Unlock()

	t := e.newTable(id)
	if err := e.fs.MkdirAll(t.dir, 0755); err != nil {
		return nil, err
	}
	if err := syncDir(e.fs, e.tablesDir); err != nil {
		return nil, err
	}
	t.flushedSeq = e.lastSeq
	edit := &versionEdit{tables: []*tableEdit{{id: id, created: true, flushedSeq: e.lastSeq}}}
	if err := manifest.write(edit); err != nil {
		e.setBackgroundError(fmt.Errorf("lsm: manifest write failed: %w", err))
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t.publishLocked()
	e.mu.tables[id] = t
	e.publishTablesLocked()
	return t, nil
}

// Table returns the store of an existing table.
func (e *Engine) Table(id TableID) (*Table, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.mu.tables[id]; ok {
		return t, nil
	}
	return nil, ErrUnknownTable
}

// DropTable drops a table: it is removed from the manifest, its pending
// memtables are discarded, and its directory is deleted. WAL entries for the
// table are ignored from then on.
//
// DropTable must not run concurrently with reads of the table; the schema
// layer serializes them.
func (e *Engine) DropTable(id TableID) error {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if err := e.checkUsableLocked(); err != nil {
		e.mu.Unlock()
		return err
	}
	t, ok := e.mu.tables[id]
	if !ok {
		e.mu.Unlock()
		return ErrUnknownTable
	}
	manifest := e.mu.manifest
	e.mu.Unlock()

	if err := manifest.write(&versionEdit{tables: []*tableEdit{{id: id, dropped: true}}}); err != nil {
		e.setBackgroundError(fmt.Errorf("lsm: manifest write failed: %w", err))
		return err
	}

	e.mu.Lock()
	t.dropped.Store(true)
	delete(e.mu.tables, id)
	e.publishTablesLocked()
	// Discard the table's memtables. No flush is running: flushes hold
	// manifestMu, which DropTable holds.
	e.memUsed.Add(-t.mem.approximateSize())
	queue := e.mu.flushQueue[:0]
	for _, job := range e.mu.flushQueue {
		if job.t == t {
			e.immBytes.Add(-job.imm.mem.approximateSize())
			e.memUsed.Add(-job.imm.mem.approximateSize())
			continue
		}
		queue = append(queue, job)
	}
	e.mu.flushQueue = queue
	t.files, t.imm = nil, nil
	// Readers still using the table's files keep them open until they
	// finish; a running compaction notices the drop and discards its output.
	t.releaseStateLocked()
	e.mu.cond.Broadcast()
	e.mu.Unlock()

	return errors.Join(e.fs.RemoveAll(t.dir), syncDir(e.fs, e.tablesDir))
}

func (e *Engine) setBackgroundError(err error) {
	e.mu.Lock()
	e.setBackgroundErrorLocked(err)
	e.mu.Unlock()
}

// Apply writes the batch atomically. Every table it writes to must exist.
//
// Writes are serialized by commitMu: a batch gets its sequence numbers, is
// appended to the WAL (a buffer copy; the WAL is written in the background),
// and is inserted into the memtables. Readers see it once the visible
// sequence number covers it, so a batch appears atomically. With Sync, Apply
// then waits until the WAL is durable, outside commitMu, so concurrent synced
// writes share one fsync; the batch may be visible before that, as in
// Cassandra.
//
// (Linking memtable entries outside commitMu and publishing batches in order,
// as Pebble's commit pipeline does, was measured slower here: see the design
// doc.)
func (e *Engine) Apply(b *Batch, wo *WriteOptions) error {
	if b.count == 0 {
		return nil
	}
	sync := wo != nil && wo.Sync
	e.commitMu.Lock()

	// Fast path: no limit is crossed, so mu is not needed. The WAL and the
	// tables' mutable memtables only change while commitMu is held.
	if !e.roomAvailable() {
		e.mu.Lock()
		err := e.makeRoomLocked()
		e.mu.Unlock()
		if err != nil {
			e.commitMu.Unlock()
			return err
		}
	}
	wal := e.mu.wal
	type target struct {
		id  TableID
		mem *memTable
	}
	var buf [4]target
	targets := buf[:0]
	tables := *e.tables.Load()
	for _, id := range b.tables {
		t, ok := tables[id]
		if !ok {
			e.commitMu.Unlock()
			return fmt.Errorf("%w: %d", ErrUnknownTable, id)
		}
		targets = append(targets, target{id, t.mem})
	}

	seq := e.lastSeq + 1
	b.setSeqNum(seq)
	var ps *pendingSync
	if sync {
		ps = &b.ps
	}
	err := wal.add(b.data, ps)
	if err != nil {
		e.commitMu.Unlock()
		// The WAL is in an unknown state; refuse further writes.
		e.setBackgroundError(fmt.Errorf("lsm: WAL write failed: %w", err))
		return err
	}
	for _, tg := range targets {
		tg.mem.firstSeg.CompareAndSwap(0, wal.num)
	}
	br, _ := newBatchReader(b.data)
	for s := seq; ; s++ {
		ent, ok, _ := br.next()
		if !ok {
			break
		}
		for _, tg := range targets {
			if tg.id == ent.table {
				tg.mem.add(ent.key, makeTrailer(s, ent.kind), ent.value)
				break
			}
		}
	}
	e.lastSeq = seq + uint64(b.count) - 1
	e.visibleSeq.Store(e.lastSeq)
	e.commitMu.Unlock()

	if ps != nil {
		if err := ps.wait(); err != nil {
			e.setBackgroundError(fmt.Errorf("lsm: WAL sync failed: %w", err))
			return err
		}
	}
	return nil
}

// SyncWAL makes every write so far durable. The periodic syncer calls it
// every WALSyncPeriod.
func (e *Engine) SyncWAL() error {
	b := batchPool.Get().(*Batch)
	defer batchPool.Put(b)
	e.commitMu.Lock()
	if e.unusable.Load() {
		e.commitMu.Unlock()
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.checkUsableLocked()
	}
	// An empty record carries the sync; replay skips it.
	err := e.mu.wal.add(nil, &b.ps)
	e.commitMu.Unlock()
	if err == nil {
		err = b.ps.wait()
	}
	if err != nil {
		e.setBackgroundError(fmt.Errorf("lsm: WAL sync failed: %w", err))
	}
	return err
}

func (e *Engine) periodicSync(period time.Duration) {
	defer close(e.syncDone)
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-e.syncStop:
			return
		case <-t.C:
			_ = e.SyncWAL()
		}
	}
}

// Metrics reports flush and compaction activity.
type Metrics struct {
	Flushes, Compactions         int64
	BytesFlushed, BytesCompacted int64
	// TombstoneCompactions counts single-sstable compactions run for
	// droppable tombstones; ExpiredSSTablesDropped counts fully expired
	// sstables deleted without being rewritten.
	TombstoneCompactions, ExpiredSSTablesDropped int64
	// EarlyReleasedInputs counts compaction inputs released before their
	// compaction finished (FragmentSize).
	EarlyReleasedInputs int64
	// WriteAmplification is (flushed + compacted) / flushed bytes: how many
	// times each flushed byte was written to sstables.
	WriteAmplification float64
}

// Metrics returns the engine's flush and compaction counters.
func (e *Engine) Metrics() Metrics {
	m := Metrics{
		Flushes:        e.metrics.flushes.Load(),
		Compactions:    e.metrics.compactions.Load(),
		BytesFlushed:   e.metrics.bytesFlushed.Load(),
		BytesCompacted: e.metrics.bytesCompacted.Load(),

		TombstoneCompactions:   e.metrics.tombstoneCompactions.Load(),
		ExpiredSSTablesDropped: e.metrics.expiredDropped.Load(),
		EarlyReleasedInputs:    e.metrics.earlyReleases.Load(),
	}
	if m.BytesFlushed > 0 {
		m.WriteAmplification = float64(m.BytesFlushed+m.BytesCompacted) / float64(m.BytesFlushed)
	}
	return m
}

// WaitForCompactions waits until no compaction is running or needed.
func (e *Engine) WaitForCompactions() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		if err := e.checkUsableLocked(); err != nil {
			return err
		}
		e.maybeScheduleCompactionsLocked()
		if e.mu.compactions == 0 && len(e.mu.flushQueue) == 0 {
			return nil
		}
		e.mu.cond.Wait()
	}
}

// Put writes one row fragment, using a pooled batch.
func (e *Engine) Put(table TableID, key, value []byte, wo *WriteOptions) error {
	b := batchPool.Get().(*Batch)
	b.Put(table, key, value)
	err := e.Apply(b, wo)
	b.Reset()
	batchPool.Put(b)
	return err
}

// roomAvailable reports, without mu, whether a write may proceed without
// rotating anything or stalling. commitMu is held.
func (e *Engine) roomAvailable() bool {
	if e.unusable.Load() {
		return false
	}
	walSize := e.mu.wal.size()
	if walSize >= e.opts.WALSegmentSize || e.closedWALBytes.Load()+walSize > e.opts.WALTotalSpace {
		return false
	}
	used := e.memUsed.Load()
	return float64(used-e.immBytes.Load()) <= float64(e.opts.MemTableBudget)*e.opts.MemTableCleanupThreshold &&
		used <= e.opts.MemTableBudget
}

// makeRoomLocked enforces the WAL and memory limits before a write, rotating
// memtables and WAL segments as needed and stalling while memtables exceed
// the budget. commitMu and mu are held.
func (e *Engine) makeRoomLocked() error {
	for {
		if err := e.checkUsableLocked(); err != nil {
			return err
		}
		if e.mu.wal.size() >= e.opts.WALSegmentSize {
			if err := e.rotateWALLocked(); err != nil {
				return err
			}
		}
		if e.walBytesLocked() > e.opts.WALTotalSpace {
			// Flush the tables whose mutable memtables pin the oldest segment.
			// Rotated memtables are already queued for flush.
			oldest := e.mu.walSegs[0].num
			for _, t := range e.mu.tables {
				if fs := t.mem.firstSeg.Load(); fs != 0 && fs <= oldest {
					e.rotateLocked(t)
				}
			}
		}
		mutable := e.memUsed.Load() - e.immBytes.Load()
		if float64(mutable) > float64(e.opts.MemTableBudget)*e.opts.MemTableCleanupThreshold {
			if t := e.largestMutableLocked(); t != nil {
				e.rotateLocked(t)
				continue
			}
		}
		if e.memUsed.Load() <= e.opts.MemTableBudget || len(e.mu.flushQueue) == 0 {
			return nil
		}
		e.mu.cond.Wait()
	}
}

func (e *Engine) largestMutableLocked() *Table {
	var largest *Table
	for _, t := range e.mu.tables {
		if !t.mem.empty() && (largest == nil || t.mem.approximateSize() > largest.mem.approximateSize()) {
			largest = t
		}
	}
	return largest
}

func (e *Engine) walBytesLocked() int64 {
	return e.closedWALBytes.Load() + e.mu.wal.size()
}

// rotateWALLocked starts a new WAL segment. commitMu and mu are held.
func (e *Engine) rotateWALLocked() error {
	num := e.mu.nextFileNum
	e.mu.nextFileNum++
	var recycle uint64
	if len(e.mu.recycledWAL) > 0 {
		recycle = e.mu.recycledWAL[0]
		e.mu.recycledWAL = e.mu.recycledWAL[1:]
	}
	wal, err := createWAL(e.fs, e.dir, num, recycle, e.opts.WALSegmentSize)
	if err != nil && recycle != 0 {
		// Fall back to a new file; the old one is no longer needed.
		_ = e.fs.Remove(e.fs.PathJoin(e.dir, walFileName(recycle)))
		wal, err = createWAL(e.fs, e.dir, num, 0, e.opts.WALSegmentSize)
	}
	if err == nil {
		err = syncDir(e.fs, e.dir)
	}
	if err == nil {
		err = e.mu.wal.close()
	}
	if err != nil {
		e.setBackgroundErrorLocked(fmt.Errorf("lsm: rotating WAL: %w", err))
		return e.mu.bgErr
	}
	closed := e.mu.wal.size()
	e.mu.walSegs[len(e.mu.walSegs)-1].size = closed
	e.closedWALBytes.Add(closed)
	e.mu.walSegs = append(e.mu.walSegs, walSegment{num: num})
	e.mu.wal = wal
	return nil
}

// rotateLocked makes the table's mutable memtable immutable and queues it for
// flush. It does nothing if the memtable is empty. commitMu and mu are held,
// so no write is in progress and lastSeq bounds the memtable's entries.
func (e *Engine) rotateLocked(t *Table) {
	if t.mem.empty() {
		return
	}
	im := &immMemTable{mem: t.mem, boundary: e.lastSeq}
	t.imm = append(t.imm, im)
	t.mem = newMemTable(&e.memUsed)
	e.immBytes.Add(im.mem.approximateSize())
	e.mu.flushQueue = append(e.mu.flushQueue, flushJob{t: t, imm: im})
	t.publishLocked()
	e.maybeScheduleFlushLocked()
}

func (e *Engine) maybeScheduleFlushLocked() {
	if e.mu.flushing || len(e.mu.flushQueue) == 0 || e.mu.bgErr != nil || e.mu.closed {
		return
	}
	e.mu.flushing = true
	e.flushWG.Add(1)
	go e.flushLoop()
}

// minNeededWALLocked returns the oldest WAL segment some memtable still
// needs, ignoring the memtable being flushed by except.
func (e *Engine) minNeededWALLocked(except *immMemTable) uint64 {
	minSeg := e.mu.wal.num
	for _, t := range e.mu.tables {
		if fs := t.mem.firstSeg.Load(); fs != 0 {
			minSeg = min(minSeg, fs)
		}
		for _, im := range t.imm {
			if im != except {
				minSeg = min(minSeg, im.mem.firstSeg.Load())
			}
		}
	}
	return minSeg
}

// flushLoop flushes queued memtables in rotation order until none remain.
func (e *Engine) flushLoop() {
	defer e.flushWG.Done()
	for {
		e.mu.Lock()
		if len(e.mu.flushQueue) == 0 || e.mu.bgErr != nil || e.mu.closed {
			e.mu.flushing = false
			e.mu.cond.Broadcast()
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		if err := e.flushOne(); err != nil {
			e.setBackgroundError(fmt.Errorf("lsm: flush failed: %w", err))
		}
	}
}

// flushOne flushes the memtable at the head of the queue. The sstable is
// written and the manifest edit is synced before readers switch from the
// memtable to the sstable, so a read never misses data.
func (e *Engine) flushOne() error {
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()

	e.mu.Lock()
	if len(e.mu.flushQueue) == 0 {
		// DropTable removed the queued memtables.
		e.mu.Unlock()
		return nil
	}
	job := e.mu.flushQueue[0]
	t, im := job.t, job.imm
	e.mu.Unlock()

	alloc := func() uint64 {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.mu.nextFileNum++
		return e.mu.nextFileNum - 1
	}
	metas, err := writeTable(e.fs, t.dir, alloc, e.writerOpts, e.merger, im.mem, t.flushBoundaries(im.mem), t.o().fragmentSize(), t.timeSplitter(), e.opts.Now().Unix())
	closeAll := func() {
		for _, m := range metas {
			if m.reader != nil {
				m.reader.close()
			}
		}
	}
	for _, meta := range metas {
		if err == nil {
			err = e.openFile(t, meta)
		}
	}
	if err == nil {
		err = syncDir(e.fs, t.dir)
	}
	if err != nil {
		closeAll()
		if t.dropped.Load() {
			// The table's directory may already be gone.
			err = nil
			e.mu.Lock()
			e.finishFlushLocked(job)
			e.mu.Unlock()
		}
		return err
	}

	e.mu.Lock()
	dropped := t.dropped.Load()
	edit := &versionEdit{nextFileNum: e.mu.nextFileNum, minWAL: e.minNeededWALLocked(im)}
	if !dropped {
		edit.tables = []*tableEdit{{id: t.id, flushedSeq: im.boundary, added: metas}}
	}
	manifest := e.mu.manifest
	e.mu.Unlock()

	if err := manifest.write(edit); err != nil {
		closeAll()
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if t.dropped.Load() {
		closeAll()
	} else {
		if len(metas) > 0 {
			var size uint64
			for _, m := range metas {
				size += m.size
			}
			t.files = append(slices.Clone(metas), t.files...)
			t.observeFlushLocked(size)
			e.metrics.bytesFlushed.Add(int64(size))
			e.metrics.flushes.Add(1)
		}
		t.flushedSeq = im.boundary
		t.imm = t.imm[1:]
		t.publishLocked()
	}
	e.finishFlushLocked(job)
	e.deleteObsoleteWALLocked(edit.minWAL)
	e.maybeScheduleCompactionsLocked()
	return nil
}

// finishFlushLocked removes a finished job from the queue and releases its
// memory.
func (e *Engine) finishFlushLocked(job flushJob) {
	e.mu.flushQueue = e.mu.flushQueue[1:]
	sz := job.imm.mem.approximateSize()
	e.immBytes.Add(-sz)
	e.memUsed.Add(-sz)
	e.mu.cond.Broadcast()
}

// walRecycleLimit is the number of obsolete WAL segments kept for reuse.
// Segments become obsolete about as fast as new ones are needed, so a small
// pool suffices; more would only hold disk space.
const walRecycleLimit = 2

// deleteObsoleteWALLocked deletes WAL segments below minWAL, or keeps them
// for reuse. The manifest already records, durably, that every table has
// flushed past them, so recovery never replays them (it starts at minWAL)
// and deletes any left over.
func (e *Engine) deleteObsoleteWALLocked(minWAL uint64) {
	for len(e.mu.walSegs) > 1 && e.mu.walSegs[0].num < minWAL {
		if len(e.mu.recycledWAL) < walRecycleLimit {
			e.mu.recycledWAL = append(e.mu.recycledWAL, e.mu.walSegs[0].num)
		} else {
			_ = e.fs.Remove(e.fs.PathJoin(e.dir, walFileName(e.mu.walSegs[0].num)))
		}
		e.closedWALBytes.Add(-e.mu.walSegs[0].size)
		e.mu.walSegs = e.mu.walSegs[1:]
	}
}

// Flush flushes every table and waits until all memtables are written.
func (e *Engine) Flush() error {
	e.commitMu.Lock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkUsableLocked(); err != nil {
		e.commitMu.Unlock()
		return err
	}
	for _, t := range e.mu.tables {
		e.rotateLocked(t)
	}
	e.commitMu.Unlock()
	for len(e.mu.flushQueue) > 0 {
		if err := e.checkUsableLocked(); err != nil {
			return err
		}
		e.mu.cond.Wait()
	}
	return e.checkUsableLocked()
}

// Close waits for an in-progress flush, then closes the engine. Data in
// memtables is recovered from the WAL on the next Open.
func (e *Engine) Close() error {
	if e.syncStop != nil {
		select {
		case <-e.syncStop:
		default:
			close(e.syncStop)
			<-e.syncDone
		}
	}
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if e.mu.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.mu.closed = true
	e.unusable.Store(true)
	e.mu.cond.Broadcast()
	e.mu.Unlock()
	// Flushes and compactions in progress need manifestMu to finish; release
	// it meanwhile. Compactions notice the close and stop early.
	e.manifestMu.Unlock()
	e.flushWG.Wait()
	e.compactWG.Wait()
	e.manifestMu.Lock()

	e.mu.Lock()
	defer e.mu.Unlock()
	err := e.closeFiles()
	if lerr := e.lock.Close(); err == nil {
		err = lerr
	}
	e.releaseCache()
	return err
}

func (e *Engine) releaseCache() {
	if h := e.readerOpts.CacheOpts.CacheHandle; h != nil {
		h.Close()
		e.readerOpts.CacheOpts.CacheHandle = nil
	}
	if e.unrefCache != nil {
		e.unrefCache()
		e.unrefCache = nil
	}
}

func (e *Engine) closeFiles() error {
	var errs []error
	if e.mu.wal != nil {
		errs = append(errs, e.mu.wal.close())
		e.mu.wal = nil
	}
	if e.mu.manifest != nil {
		errs = append(errs, e.mu.manifest.close())
		e.mu.manifest = nil
	}
	for _, t := range e.mu.tables {
		if t.state.Load() != nil {
			t.releaseStateLocked()
		} else {
			// Open failed before the table's state was published.
			for _, f := range t.files {
				if f.reader != nil {
					errs = append(errs, f.reader.close())
				}
			}
		}
	}
	return errors.Join(errs...)
}
