package lsm

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// purgeEngine opens an engine with a controlled clock (Unix seconds) and
// compactions off, so tests choose what is compacted.
func purgeEngine(t *testing.T, fs vfs.FS, clock *atomic.Int64, o TableOptions) (*Engine, *Table) {
	t.Helper()
	e, err := Open("db", &Options{
		FS:                 fs,
		Now:                func() time.Time { return time.Unix(clock.Load(), 0) },
		DisableCompactions: true,
		TableOptions:       func(TableID) TableOptions { return o },
	})
	require.NoError(t, err)
	tbl, err := e.Table(1)
	if err != nil {
		tbl, err = e.CreateTable(1)
		require.NoError(t, err)
	}
	return e, tbl
}

func putCell(t *testing.T, e *Engine, key string, c row.Cell) {
	t.Helper()
	v := row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{c}})
	require.NoError(t, e.Put(1, []byte(key), v, NoSync))
}

func liveCell(ts int64, v string) row.Cell {
	return row.Cell{Column: 1, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: []byte(v)}
}

func tombCell(ts, ldt int64) row.Cell {
	return row.Cell{Column: 1, Timestamp: ts, LocalDeletionTime: ldt}
}

// rawRows returns the keys stored in the table's sstables and memtables,
// whether or not they have live cells.
func rawRows(t *testing.T, tbl *Table) []string {
	t.Helper()
	it, err := tbl.NewIter(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	var keys []string
	for ok := it.First(); ok; ok = it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.NoError(t, it.Error())
	return keys
}

// liveAt reports whether key has a live cell at nowSec.
func liveAt(t *testing.T, tbl *Table, key string, nowSec int64) bool {
	t.Helper()
	v, err := tbl.Get([]byte(key))
	if err == ErrNotFound {
		return false
	}
	require.NoError(t, err)
	r, err := row.Decode(v)
	require.NoError(t, err)
	return len(r.Live(nowSec)) > 0
}

func compactAll(t *testing.T, e *Engine, tbl *Table, files []*fileMeta) {
	t.Helper()
	e.mu.Lock()
	for _, f := range files {
		f.compacting = true
		f.refs.Add(1)
	}
	e.mu.compactions++
	e.compactWG.Add(1)
	e.mu.Unlock()
	e.runCompaction(tbl, &compactionPick{inputs: files})
	require.NoError(t, e.checkUsable())
}

func (e *Engine) checkUsable() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.checkUsableLocked()
}

func tableFileList(e *Engine, tbl *Table) []*fileMeta {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(tbl.files)
}

// TestPurgeRule checks Cassandra's purge rule in compaction: a tombstone
// past gc_grace_seconds is dropped, unless data it may shadow (older
// timestamp) lives outside the compaction.
func TestPurgeRule(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{GCGraceSeconds: 100})
	defer e.Close()

	// File A (not compacted): "blocked" has old live data at ts 10.
	putCell(t, e, "blocked", liveCell(10, "old"))
	require.NoError(t, tbl.Flush())
	a := tableFileList(e, tbl)
	require.Len(t, a, 1)
	// Files B and C: live data then tombstones for both keys.
	putCell(t, e, "blocked", liveCell(20, "new"))
	putCell(t, e, "free", liveCell(20, "v"))
	require.NoError(t, tbl.Flush())
	putCell(t, e, "blocked", tombCell(30, clock.Load()))
	putCell(t, e, "free", tombCell(30, clock.Load()))
	require.NoError(t, tbl.Flush())
	bc := tableFileList(e, tbl)[:2] // newest first
	require.NotContains(t, bc, a[0])

	// Within gc grace nothing is purged.
	compactAll(t, e, tbl, bc)
	require.Equal(t, []string{"blocked", "free"}, rawRows(t, tbl))

	// Past gc grace, "free" is purged; "blocked" keeps its tombstone, which
	// still shadows file A's ts-10 cell.
	clock.Add(101)
	compactAll(t, e, tbl, slices.DeleteFunc(tableFileList(e, tbl), func(f *fileMeta) bool { return f == a[0] }))
	require.Equal(t, []string{"blocked"}, rawRows(t, tbl))
	mustNotFound(t, tbl, "blocked")
	mustNotFound(t, tbl, "free")

	// Compacting everything purges the tombstone together with what it
	// shadows.
	compactAll(t, e, tbl, tableFileList(e, tbl))
	require.Empty(t, rawRows(t, tbl))
}

// TestPurgeRespectsMemtables checks that a tombstone is kept while a
// memtable holds older data for its key.
func TestPurgeRespectsMemtables(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{GCGraceSeconds: -1})
	defer e.Close()
	putCell(t, e, "k", tombCell(30, clock.Load()-1))
	require.NoError(t, tbl.Flush())
	putCell(t, e, "x", liveCell(1, "x"))
	require.NoError(t, tbl.Flush())
	putCell(t, e, "k", liveCell(10, "late but older")) // stays in the memtable
	compactAll(t, e, tbl, tableFileList(e, tbl))
	mustNotFound(t, tbl, "k")
	require.Contains(t, rawRows(t, tbl), "k")
}

// TestExpiredSSTableDrop checks that an sstable whose data has all expired
// past gc grace is deleted without being rewritten, unless an overlapping
// sstable holds data older than its newest.
func TestExpiredSSTableDrop(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{GCGraceSeconds: 100, ExpiredCheckInterval: -1})
	defer e.Close()
	expiring := func(ts int64, v string) row.Cell {
		return row.Cell{Column: 1, Timestamp: ts, TTL: 60, LocalDeletionTime: clock.Load() + 60, Value: []byte(v)}
	}
	// "a"-"c": expiring data at ts 100, no overlap: droppable.
	for _, k := range []string{"a", "b", "c"} {
		putCell(t, e, k, expiring(100, k))
	}
	require.NoError(t, tbl.Flush())
	// "m"-"o" expiring at ts 100, overlapping a live ts-50 cell for "n".
	putCell(t, e, "n", liveCell(50, "live"))
	require.NoError(t, tbl.Flush())
	for _, k := range []string{"m", "n", "o"} {
		putCell(t, e, k, expiring(100, k))
	}
	require.NoError(t, tbl.Flush())
	require.Len(t, tableFileList(e, tbl), 3)

	pick := func() *compactionPick {
		e.mu.Lock()
		defer e.mu.Unlock()
		return tbl.nextCompactionLocked()
	}
	// Not expired yet; then expired but within gc grace.
	require.Nil(t, pick())
	clock.Add(61)
	require.Nil(t, pick())
	require.False(t, liveAt(t, tbl, "a", clock.Load()))
	require.False(t, liveAt(t, tbl, "n", clock.Load())) // the expired cell shadows the ts-50 cell

	clock.Add(100)
	p := pick()
	require.NotNil(t, p)
	require.True(t, p.dropOnly)
	require.Len(t, p.inputs, 1)
	require.Equal(t, "a", string(p.inputs[0].smallest.UserKey))
	e.mu.Lock()
	for _, f := range p.inputs {
		f.compacting = true
		f.refs.Add(1)
	}
	e.mu.compactions++
	e.compactWG.Add(1)
	e.mu.Unlock()
	e.runCompaction(tbl, p)
	require.Len(t, tableFileList(e, tbl), 2)
	require.EqualValues(t, 1, e.Metrics().ExpiredSSTablesDropped)
	require.False(t, liveAt(t, tbl, "n", clock.Load()))
	require.NotContains(t, rawRows(t, tbl), "a")
}

// TestTombstoneCompaction checks that an sstable mostly made of droppable
// tombstones is compacted on its own once it is old enough, and that the
// tombstones are purged.
func TestTombstoneCompaction(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	fs := vfs.NewMem()
	opts := TableOptions{GCGraceSeconds: 10, TombstoneCompactionInterval: time.Hour}
	e, tbl := purgeEngine(t, fs, &clock, opts)
	for i := 0; i < 100; i++ {
		putCell(t, e, fmt.Sprintf("k%03d", i), liveCell(1, "v"))
	}
	for i := 0; i < 50; i++ {
		putCell(t, e, fmt.Sprintf("k%03d", i), tombCell(2, clock.Load()))
	}
	require.NoError(t, tbl.Flush())
	files := tableFileList(e, tbl)
	require.Len(t, files, 1)
	st := files[0].stats
	require.True(t, st.valid)
	require.EqualValues(t, 100, st.entries)
	require.EqualValues(t, 50, st.tombstones)
	require.Equal(t, int64(row.NoDeletionTime), st.maxLDT)

	// The summary survives a reopen.
	require.NoError(t, e.Close())
	e, tbl = purgeEngine(t, fs, &clock, opts)
	defer e.Close()
	require.Equal(t, st.tombstones, tableFileList(e, tbl)[0].stats.tombstones)
	require.Equal(t, st.hist, tableFileList(e, tbl)[0].stats.hist)

	pick := func() *compactionPick {
		e.mu.Lock()
		defer e.mu.Unlock()
		return tbl.nextCompactionLocked()
	}
	clock.Add(20) // past gc grace, but the file is younger than an hour
	require.Nil(t, pick())
	clock.Add(3600)
	p := pick()
	require.NotNil(t, p)
	require.True(t, p.single)
	compactAll(t, e, tbl, p.inputs)
	require.Len(t, rawRows(t, tbl), 50)
	require.EqualValues(t, 0, tableFileList(e, tbl)[0].stats.tombstones)
	require.Nil(t, pick())
}

// TestLDTHistogram checks the streaming histogram's estimates.
func TestLDTHistogram(t *testing.T) {
	var h ldtHistogram
	for i := int64(0); i < 1000; i++ {
		h.add(i, 1)
	}
	require.LessOrEqual(t, len(h.points), histBins)
	require.InDelta(t, 500, h.countBefore(500), 50)
	require.Equal(t, 0.0, h.countBefore(0))
	require.Equal(t, 1000.0, h.countBefore(2000))
}

// TestMajorCompaction checks Table.Compact: all sstables merge into one set
// of shards, tombstones past gc grace go with the data they shadow, and
// live rows are untouched, with background compactions running meanwhile.
func TestMajorCompaction(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, err := Open("db", &Options{
		FS:             vfs.NewMem(),
		MemTableBudget: 64 << 10,
		Now:            func() time.Time { return time.Unix(clock.Load(), 0) },
		TableOptions:   func(TableID) TableOptions { return TableOptions{GCGraceSeconds: 100} },
	})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	want := map[string]string{}
	ts := int64(0)
	for round := 0; round < 10; round++ {
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("k%04d", (round*500+i)%1500)
			ts++
			if i%4 == 3 {
				putCell(t, e, k, tombCell(ts, clock.Load()))
				delete(want, k)
			} else {
				putCell(t, e, k, liveCell(ts, fmt.Sprint(ts)))
				want[k] = fmt.Sprint(ts)
			}
		}
		require.NoError(t, tbl.Flush())
	}
	require.Equal(t, want, scanAll(t, tbl, nil, nil))
	require.Greater(t, len(rawRows(t, tbl)), len(want))

	clock.Add(101)
	require.NoError(t, tbl.Compact())
	require.Len(t, tableFileList(e, tbl), 1, "narrow keys: one shard")
	require.Equal(t, len(want), len(rawRows(t, tbl)), "every tombstone purged")
	require.Equal(t, want, scanAll(t, tbl, nil, nil))
	require.NoError(t, e.WaitForCompactions())
}

// TestSTCSBuckets checks the port of SizeTieredCompactionStrategy.getBuckets
// and mostInterestingBucket.
func TestSTCSBuckets(t *testing.T) {
	o := TableOptions{STCSMinSSTableSize: 10}
	o.ensureDefaults()
	o.STCSMinSSTableSize = 10
	f := func(size uint64) *fileMeta { return &fileMeta{size: size} }
	files := []*fileMeta{f(100), f(110), f(1000), f(90), f(5), f(3), f(1100), f(105), f(95)}
	buckets := stcsBuckets(files, &o)
	sizes := func(b []*fileMeta) []uint64 {
		var out []uint64
		for _, x := range b {
			out = append(out, x.size)
		}
		return out
	}
	require.Len(t, buckets, 3)
	require.Equal(t, []uint64{3, 5}, sizes(buckets[0]))                  // both below the minimum size
	require.Equal(t, []uint64{90, 95, 100, 105, 110}, sizes(buckets[1])) // within 0.5-1.5x of the average
	require.Equal(t, []uint64{1000, 1100}, sizes(buckets[2]))
	// Needs min_threshold sstables; among eligible buckets the largest
	// average size wins (Collections.max over the comparator).
	require.Equal(t, []uint64{90, 95, 100, 105, 110}, sizes(mostInterestingBucket(buckets, 4, 32)))
	require.Equal(t, []uint64{1000, 1100}, sizes(mostInterestingBucket(buckets, 2, 32)))
	require.Equal(t, []uint64{90, 95}, sizes(mostInterestingBucket(buckets[1:2], 2, 2)))
	require.Nil(t, mostInterestingBucket(buckets, 6, 32))
}

// TestTimeWindowCurrentWindow checks Cassandra's TWCS rules: the newest window ever
// seen needs min_threshold sstables (size-tiered), an older one only two,
// and the current window does not move back when newer files disappear.
func TestTimeWindowCurrentWindow(t *testing.T) {
	hour := time.Hour
	tbl := &Table{opts: TableOptions{TimeWindow: hour, TimeWindowCassandra: true}}
	tbl.opts.ensureDefaults()
	h := hour.Microseconds()
	f := func(window int64, size uint64) *fileMeta {
		return &fileMeta{maxTS: window*h + 1, size: size, smallest: makeKey("a"), largest: makeKey("z")}
	}
	// Window 5 is current with 3 files: below min_threshold, nothing.
	tbl.files = []*fileMeta{f(5, 100), f(5, 100), f(5, 100)}
	require.Nil(t, tbl.pickCompactionLocked())
	// A fourth file of similar size: size-tiered compaction of all four.
	tbl.files = append(tbl.files, f(5, 110))
	require.Len(t, tbl.pickCompactionLocked(), 4)
	// Only window 4 left, with 2 files: still older than the highest window
	// seen (5), so they are compacted.
	tbl.files = []*fileMeta{f(4, 100), f(4, 300)}
	require.Len(t, tbl.pickCompactionLocked(), 2)
	// Older windows are compacted up to max_threshold smallest sstables.
	tbl.files = nil
	for i := 0; i < 40; i++ {
		tbl.files = append(tbl.files, f(1, uint64(1000-i)))
	}
	p := tbl.pickCompactionLocked()
	require.Len(t, p, 32)
	require.EqualValues(t, 961, p[0].size)
}

// TestWorthDroppingTombstones checks the overlap check of
// worthDroppingTombstones.
func TestWorthDroppingTombstones(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{GCGraceSeconds: 10, TombstoneCompactionInterval: time.Hour})
	defer e.Close()
	key := func(tok uint64, s string) string { return string(tokenKey(tok)) + s }
	// File A: tombstones over tokens [0, 1<<62].
	for i := uint64(0); i <= 4; i++ {
		putCell(t, e, key(i<<60, "a"), tombCell(5, clock.Load()))
	}
	require.NoError(t, tbl.Flush())
	a := tableFileList(e, tbl)[0]
	clock.Add(7200)
	now, gc := clock.Load(), tbl.opts.gcBefore(clock.Load())
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := tbl.worthDroppingTombstonesLocked(a, now, gc)
	require.True(t, ok, "no overlap")

	// B overlaps all of A's token span with live data: not worth it.
	b := &fileMeta{smallest: makeKey(key(0, "")), largest: makeKey(key(1<<62, "z")), minTS: 1, maxTS: 1,
		stats: fileStats{valid: true, maxLDT: row.NoDeletionTime}}
	tbl.files = append(tbl.files, b)
	_, ok = tbl.worthDroppingTombstonesLocked(a, now, gc)
	require.False(t, ok, "fully overlapped, data older than the tombstones")
	tbl.opts.UncheckedTombstoneCompaction = true
	_, ok = tbl.worthDroppingTombstonesLocked(a, now, gc)
	require.True(t, ok, "unchecked")
	tbl.opts.UncheckedTombstoneCompaction = false
	// B covering only the first quarter leaves 3/4 of A: 0.75·1 > 0.2.
	b.largest = makeKey(key(1<<60-1, ""))
	_, ok = tbl.worthDroppingTombstonesLocked(a, now, gc)
	require.True(t, ok, "mostly outside the overlap")
	tbl.files = tbl.files[:1]
}

func TestUncoveredTokenShare(t *testing.T) {
	f := func(lo, hi uint64) *fileMeta {
		return &fileMeta{smallest: makeKey(string(tokenKey(lo))), largest: makeKey(string(tokenKey(hi)))}
	}
	x := f(0, 99)
	require.InDelta(t, 1, uncoveredTokenShare(x, nil), 1e-9)
	require.InDelta(t, 0.5, uncoveredTokenShare(x, []*fileMeta{f(0, 49)}), 1e-9)
	require.InDelta(t, 0.5, uncoveredTokenShare(x, []*fileMeta{f(0, 29), f(20, 49)}), 1e-9)
	require.InDelta(t, 0, uncoveredTokenShare(x, []*fileMeta{f(0, 200)}), 1e-9)
	require.InDelta(t, 0.25, uncoveredTokenShare(x, []*fileMeta{f(25, 99)}), 1e-9)
}

// TestIncrementalRelease checks ICS-style incremental compaction: with small
// fragments, inputs are swapped out before the compaction ends, and after
// every such swap the table still reads exactly as before.
func TestIncrementalRelease(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{FragmentSize: 2 << 10})
	defer e.Close()
	want := map[string]string{}
	for f := 0; f < 8; f++ { // overlapping flushes, each over a different stretch
		for i := 0; i < 300; i++ {
			k := fmt.Sprintf("k%04d", f*100+i)
			v := fmt.Sprintf("f%d", f)
			putCell(t, e, k, liveCell(int64(f+1), v))
			want[k] = v
		}
		require.NoError(t, tbl.Flush())
	}
	checks := 0
	e.testAfterEarlyRelease = func() {
		checks++
		require.Equal(t, want, scanAll(t, tbl, nil, nil))
	}
	compactAll(t, e, tbl, tableFileList(e, tbl))
	require.Positive(t, e.Metrics().EarlyReleasedInputs)
	require.Positive(t, checks)
	require.Equal(t, want, scanAll(t, tbl, nil, nil))
	require.Greater(t, len(tableFileList(e, tbl)), 1, "output cut into fragments")
}

// TestIncrementalReleasePurgeTie checks the hazard of incremental release
// with purging: A holds tombstones for k0000-k0099, B older live data for
// k0000-k0999. Purging drops both; if A were released before B, B's
// shadowed cells would come back. The engine releases them together, so at
// every early release, and after a crash at that point, the rows stay
// deleted.
func TestIncrementalReleasePurgeTie(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	fs := vfs.NewCrashableMem()
	opts := TableOptions{FragmentSize: 2 << 10, GCGraceSeconds: -1}
	e, tbl := purgeEngine(t, fs, &clock, opts)
	for i := 0; i < 1000; i++ {
		putCell(t, e, fmt.Sprintf("k%04d", i), liveCell(1, "old"))
	}
	require.NoError(t, tbl.Flush())
	for i := 0; i < 100; i++ {
		putCell(t, e, fmt.Sprintf("k%04d", i), tombCell(2, clock.Load()-10))
	}
	require.NoError(t, tbl.Flush())
	// C (k0200-k0299, nothing purged) can be released early while A and B
	// wait for each other.
	for i := 200; i < 300; i++ {
		putCell(t, e, fmt.Sprintf("k%04d", i), liveCell(3, "c"))
	}
	require.NoError(t, tbl.Flush())
	require.NoError(t, e.SyncWAL())
	clock.Add(10)
	deleted := func(tbl *Table) {
		for i := 0; i < 100; i += 7 {
			require.False(t, liveAt(t, tbl, fmt.Sprintf("k%04d", i), clock.Load()), "k%04d resurrected", i)
		}
	}
	var crashed []*vfs.MemFS
	e.testAfterEarlyRelease = func() {
		deleted(tbl)
		crashed = append(crashed, fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0}))
	}
	compactAll(t, e, tbl, tableFileList(e, tbl))
	deleted(tbl)
	require.Positive(t, len(crashed), "C was released early")
	require.Positive(t, e.Metrics().EarlyReleasedInputs) // C, then A and B together
	require.Equal(t, 900, len(rawRows(t, tbl)), "tombstones and shadowed cells purged")
	require.NoError(t, e.Close())
	for _, c := range crashed {
		e2, tbl2 := purgeEngine(t, c, &clock, opts)
		deleted(tbl2)
		require.NoError(t, e2.Close())
	}

	// Control: without purging (long gc grace) there is nothing to tie, and
	// inputs are released early.
	e3, tbl3 := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{FragmentSize: 2 << 10})
	defer e3.Close()
	for i := 0; i < 1000; i++ {
		putCell(t, e3, fmt.Sprintf("k%04d", i), liveCell(1, "old"))
	}
	require.NoError(t, tbl3.Flush())
	for i := 0; i < 100; i++ {
		putCell(t, e3, fmt.Sprintf("k%04d", i), tombCell(2, clock.Load()-10))
	}
	require.NoError(t, tbl3.Flush())
	compactAll(t, e3, tbl3, tableFileList(e3, tbl3))
	require.Positive(t, e3.Metrics().EarlyReleasedInputs)
	deleted(tbl3)
}

// TestGarbageCollectionJob checks the ICS-style cross-level job: tombstones
// in a small sstable shadow older data in a large one that overlaps it, so
// compacting the small one alone could not purge them (Cassandra's
// worthDroppingTombstones rejects it); the engine compacts both levels
// together and purges tombstones and data.
func TestGarbageCollectionJob(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{GCGraceSeconds: 10, TombstoneCompactionInterval: time.Hour, FlushSizeOverride: 1 << 10})
	defer e.Close()
	val := string(make([]byte, 200))
	for i := 0; i < 2000; i++ { // B: large, old live data
		putCell(t, e, fmt.Sprintf("k%04d", i), liveCell(1, val))
	}
	require.NoError(t, tbl.Flush())
	for i := 0; i < 100; i++ { // A: small, tombstones over part of B
		putCell(t, e, fmt.Sprintf("k%04d", i*20), tombCell(5, clock.Load()))
	}
	require.NoError(t, tbl.Flush())
	clock.Add(7200)
	pick := func() *compactionPick {
		e.mu.Lock()
		defer e.mu.Unlock()
		return tbl.nextCompactionLocked()
	}
	p := pick()
	require.NotNil(t, p)
	require.Len(t, p.inputs, 2, "both levels")
	e.mu.Lock()
	for _, f := range p.inputs {
		f.compacting = true
		f.refs.Add(1)
	}
	e.mu.compactions++
	e.compactWG.Add(1)
	e.mu.Unlock()
	e.runCompaction(tbl, p)
	require.Len(t, rawRows(t, tbl), 1900, "tombstones and the data they shadowed purged")
	require.False(t, liveAt(t, tbl, "k0020", clock.Load()))
	require.True(t, liveAt(t, tbl, "k0021", clock.Load()))
}

// TestSpaceAmplificationJob checks the space amplification goal: two
// density levels whose combined size exceeds goal times the top level's are
// compacted together.
func TestSpaceAmplificationJob(t *testing.T) {
	tbl := &Table{opts: TableOptions{SpaceAmplificationGoal: 1.5, FlushSizeOverride: 1 << 20}}
	tbl.opts.ensureDefaults()
	num := uint64(0)
	f := func(size uint64, a, b string) *fileMeta {
		num++
		return &fileMeta{num: num, size: size, smallest: makeKey(a), largest: makeKey(b)}
	}
	// Every file spans the whole token range, so density is size.
	lo, hi := string(tokenKey(0)), string(tokenKey(1<<64-1))
	big := f(400<<20, lo, hi)
	mid := f(150<<20, lo, hi)
	small := f(1<<10, lo, hi)
	tbl.files = []*fileMeta{big, mid, small}
	require.Nil(t, tbl.spaceAmplificationJobLocked(), "(400+150)/400 < 1.5")
	mid2 := f(150<<20, lo, hi)
	tbl.files = append(tbl.files, mid2)
	p := tbl.spaceAmplificationJobLocked()
	require.ElementsMatch(t, []*fileMeta{big, mid, mid2}, p, "(400+300)/400 > 1.5")
	// The same files as fragments of one run: compacting would only
	// rewrite that run.
	for _, x := range []*fileMeta{big, mid, mid2} {
		x.run = 99
	}
	require.Nil(t, tbl.spaceAmplificationJobLocked())
	big.run = 0
	tbl.opts.SpaceAmplificationGoal = 0
	require.Nil(t, tbl.spaceAmplificationJobLocked())
}

// TestRecoveredSeqAfterPurge is a regression test: when purging removes the
// rows with the highest sequence numbers, recovery must still resume
// sequence numbers above every table's flushed sequence number. Otherwise a
// write acknowledged after the restart reuses a sequence number the table
// counts as flushed, and WAL replay after a second crash skips it.
func TestRecoveredSeqAfterPurge(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	fs := vfs.NewCrashableMem()
	open := func(fs vfs.FS) (*Engine, *Table) {
		e, err := Open("db", &Options{
			FS: fs, DisableCompactions: true, WALSegmentSize: 1 << 10, WALSyncPeriod: -1,
			Now:          func() time.Time { return time.Unix(clock.Load(), 0) },
			TableOptions: func(TableID) TableOptions { return TableOptions{GCGraceSeconds: -1, FragmentSize: -1} },
		})
		require.NoError(t, err)
		tbl, err := e.Table(1)
		if err != nil {
			tbl, err = e.CreateTable(1)
			require.NoError(t, err)
		}
		return e, tbl
	}
	e, tbl := open(fs)
	putCell(t, e, "a", liveCell(1, "v"))
	require.NoError(t, tbl.Flush())
	putCell(t, e, "a", tombCell(2, clock.Load()-10))
	// Rotate the WAL, so flushing the tombstone retires the segment holding
	// sequence numbers 1-2: afterwards only the table's flushed sequence
	// number in the manifest remembers them.
	e.commitMu.Lock()
	e.mu.Lock()
	require.NoError(t, e.rotateWALLocked())
	e.mu.Unlock()
	e.commitMu.Unlock()
	require.NoError(t, tbl.Flush())
	require.NoError(t, syncDir(fs, "db"))
	compactAll(t, e, tbl, tableFileList(e, tbl))
	require.Empty(t, rawRows(t, tbl), "row and tombstone purged")

	crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	require.NoError(t, e.Close())
	e, tbl = open(crashed)
	require.GreaterOrEqual(t, e.lastSeq, uint64(2), "sequence numbers resume past the flushed ones")
	require.NoError(t, e.Put(1, []byte("b"), row.Append(nil, &row.Row{
		Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
		Cells:    []row.Cell{liveCell(3, "synced")},
	}), Sync))

	crashed2 := crashed.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	require.NoError(t, e.Close())
	e, tbl = open(crashed2)
	defer e.Close()
	require.True(t, liveAt(t, tbl, "b", clock.Load()), "synced write lost after the second crash")
}

// TestTimeWindowScyllaRules checks the default time-window rules (from
// ScyllaDB): a past window is compacted whole while it is recent; once it
// holds one run it is not, and later stray runs in it get size-tiered
// compaction from two similar runs instead.
func TestTimeWindowScyllaRules(t *testing.T) {
	hour := time.Hour
	tbl := &Table{opts: TableOptions{TimeWindow: hour}}
	tbl.opts.ensureDefaults()
	h := hour.Microseconds()
	// Above and below STCS's min_sstable_size (50 MiB), which groups all
	// smaller runs together.
	const big, small = 100 << 20, 1 << 20
	num := uint64(0)
	f := func(window int64, size uint64, run uint64) *fileMeta {
		num++
		return &fileMeta{num: num, run: run, maxTS: window*h + 1, size: size, smallest: makeKey("a"), largest: makeKey("z")}
	}
	// Window 5 current with 3 runs: below min_threshold.
	tbl.files = []*fileMeta{f(5, big, 0), f(5, big, 0), f(5, big, 0)}
	require.Nil(t, tbl.pickCompactionLocked())
	// Window 6 starts; window 5 is now recent and past: compacted whole.
	tbl.files = append(tbl.files, f(6, big, 0))
	p := tbl.pickCompactionLocked()
	require.Len(t, p, 3)
	// Window 5 compacted into one run of three fragments: done, and no
	// longer recent.
	tbl.files = []*fileMeta{f(5, big, 50), f(5, big, 50), f(5, big, 50), f(6, big, 0)}
	require.Nil(t, tbl.pickCompactionLocked())
	require.False(t, tbl.recentWindows[5])
	// A stray late run lands in window 5: not recent, so no whole-window
	// rewrite, and a run of very different size is not compacted with it.
	tbl.files = append(tbl.files, f(5, small, 0))
	require.Nil(t, tbl.pickCompactionLocked())
	// Two similar stray runs are compacted together (size-tiered, from 2),
	// without the window's big run.
	tbl.files = append(tbl.files, f(5, small, 0))
	p = tbl.pickCompactionLocked()
	require.Len(t, p, 2)
	for _, x := range p {
		require.EqualValues(t, small, x.size)
	}
}

// TestTimeWindowSegregation checks that flushes split late and
// out-of-order writes by window, so every sstable holds one window's data,
// and that reads see the same rows.
func TestTimeWindowSegregation(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), DisableCompactions: true,
		TableOptions: func(TableID) TableOptions { return TableOptions{TimeWindow: time.Hour} }})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	h := time.Hour.Microseconds()
	want := map[string]string{}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("k%03d", i)
		// Mostly window 10, some late writes into windows 7-9, and rows
		// whose cells span two windows.
		w := int64(10)
		if i%5 == 0 {
			w = int64(7 + i%3)
		}
		cells := []row.Cell{{Column: 1, Timestamp: w*h + int64(i), LocalDeletionTime: row.NoDeletionTime, Value: []byte(k)}}
		if i%7 == 0 {
			cells = append(cells, row.Cell{Column: 2, Timestamp: (w-1)*h + 5, LocalDeletionTime: row.NoDeletionTime, Value: []byte("old")})
		}
		require.NoError(t, e.Put(1, []byte(k), row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: cells}), NoSync))
		want[k] = k
		if i%7 == 0 {
			want[k] = "old" // scanAll keeps a row's last live cell
		}
	}
	require.NoError(t, tbl.Flush())
	files := tableFileList(e, tbl)
	require.Greater(t, len(files), 3)
	windows := map[int64]bool{}
	for _, f := range files {
		require.Equal(t, f.minTS/h, f.maxTS/h, "file %d mixes windows", f.num)
		windows[f.maxTS/h] = true
	}
	require.Len(t, windows, 5) // 6-10: late writes into 7-9, and cells one window back
	require.Equal(t, want, scanAll(t, tbl, nil, nil))
	// Cassandra's behaviour keeps one file.
	e2, err := Open("db2", &Options{FS: vfs.NewMem(), DisableCompactions: true,
		TableOptions: func(TableID) TableOptions { return TableOptions{TimeWindow: time.Hour, TimeWindowCassandra: true} }})
	require.NoError(t, err)
	defer e2.Close()
	tbl2, _ := e2.CreateTable(1)
	require.NoError(t, e2.Put(1, []byte("a"), row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
		Cells: []row.Cell{{Column: 1, Timestamp: 1, LocalDeletionTime: row.NoDeletionTime}, {Column: 2, Timestamp: 5 * h, LocalDeletionTime: row.NoDeletionTime}}}), NoSync))
	require.NoError(t, tbl2.Flush())
	require.Len(t, tableFileList(e2, tbl2), 1)
}

// TestAggressiveExpiration checks unsafe_aggressive_sstable_expiration: a
// fully expired sstable overlapping older data is dropped only with it.
func TestAggressiveExpiration(t *testing.T) {
	for _, aggressive := range []bool{false, true} {
		var clock atomic.Int64
		clock.Store(1_000_000)
		e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{TimeWindow: time.Hour, GCGraceSeconds: -1,
			ExpiredCheckInterval: -1, UnsafeAggressiveExpiration: aggressive})
		putCell(t, e, "a", liveCell(1, "old")) // older live data overlapping
		require.NoError(t, tbl.Flush())
		putCell(t, e, "a", row.Cell{Column: 1, Timestamp: 2, TTL: 10, LocalDeletionTime: clock.Load() + 10, Value: []byte("ttl")})
		require.NoError(t, tbl.Flush())
		clock.Add(20)
		e.mu.Lock()
		p := tbl.nextCompactionLocked()
		e.mu.Unlock()
		if aggressive {
			require.NotNil(t, p)
			require.True(t, p.dropOnly)
		} else {
			require.True(t, p == nil || !p.dropOnly)
		}
		require.NoError(t, e.Close())
	}
}
