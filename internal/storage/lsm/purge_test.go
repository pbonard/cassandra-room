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
