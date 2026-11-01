package lsm

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// testTS issues increasing timestamps, so in tests that do not exercise
// out-of-order writes the latest write wins, as with key-value semantics.
var testTS atomic.Int64

func cellRow(ts int64, value string, tomb bool) []byte {
	c := row.Cell{Column: 1, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: []byte(value)}
	if tomb {
		c = row.Cell{Column: 1, Timestamp: ts, LocalDeletionTime: 1}
	}
	return row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{c}})
}

// getVal returns the live value of column 1 of key, or ok=false.
func getVal(t *testing.T, tbl *Table, key string) (string, bool) {
	t.Helper()
	v, err := tbl.Get([]byte(key))
	if errors.Is(err, ErrNotFound) {
		return "", false
	}
	require.NoError(t, err, "key %q", key)
	r, err := row.Decode(v)
	require.NoError(t, err)
	for _, c := range r.Live(0) {
		if c.Column == 1 {
			return string(c.Value), true
		}
	}
	return "", false
}

func mustGet(t *testing.T, tbl *Table, key, want string) {
	t.Helper()
	got, ok := getVal(t, tbl, key)
	require.True(t, ok, "key %q not found", key)
	require.Equal(t, want, got, "key %q", key)
}

func mustNotFound(t *testing.T, tbl *Table, key string) {
	t.Helper()
	_, ok := getVal(t, tbl, key)
	require.False(t, ok, "key %q", key)
}

func set(t *testing.T, e *Engine, id TableID, key, value string, wo *WriteOptions) {
	t.Helper()
	require.NoError(t, e.Put(id, []byte(key), cellRow(testTS.Add(1), value, false), wo))
}

func del(t *testing.T, e *Engine, id TableID, key string) {
	t.Helper()
	require.NoError(t, e.Put(id, []byte(key), cellRow(testTS.Add(1), "", true), NoSync))
}

func tableFiles(e *Engine, id TableID) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.mu.tables[id].files)
}

func walFiles(t *testing.T, fs vfs.FS) int {
	names, err := fs.List("db")
	require.NoError(t, err)
	n := 0
	for _, name := range names {
		if kind, _, ok := parseFileName(name); ok && kind == "wal" {
			n++
		}
	}
	return n
}

func TestBasic(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer e.Close()
	a, err := e.CreateTable(1)
	require.NoError(t, err)
	b, err := e.CreateTable(2)
	require.NoError(t, err)
	_, err = e.CreateTable(1)
	require.ErrorIs(t, err, ErrTableExists)

	mustNotFound(t, a, "k")
	set(t, e, 1, "k", "a1", NoSync)
	set(t, e, 2, "k", "b1", NoSync)
	mustGet(t, a, "k", "a1")
	mustGet(t, b, "k", "b1") // tables are separate key spaces
	del(t, e, 1, "k")
	mustNotFound(t, a, "k")
	mustGet(t, b, "k", "b1")

	// A batch across tables is atomic and rejected as a whole if any table
	// is unknown.
	batch := NewBatch()
	batch.Put(1, []byte("x"), cellRow(testTS.Add(1), "1", false))
	batch.Put(2, []byte("y"), cellRow(testTS.Add(1), "2", false))
	require.NoError(t, e.Apply(batch, Sync))
	mustGet(t, a, "x", "1")
	mustGet(t, b, "y", "2")
	bad := NewBatch()
	bad.Put(1, []byte("z"), cellRow(testTS.Add(1), "1", false))
	bad.Put(99, []byte("z"), cellRow(testTS.Add(1), "1", false))
	require.ErrorIs(t, e.Apply(bad, NoSync), ErrUnknownTable)
	mustNotFound(t, a, "z")
}

// TestFlushReopen checks reads across memtables and several sstables, with
// overwrites and deletes spanning sstables, in two tables, before and after
// reopening.
func TestFlushReopen(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{FS: fs, MemTableBudget: 96 << 10}
	e, err := Open("db", opts)
	require.NoError(t, err)
	for _, id := range []TableID{1, 2} {
		_, err := e.CreateTable(id)
		require.NoError(t, err)
	}

	model := map[TableID]map[string]string{1: {}, 2: {}}
	rng := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 20000; i++ {
		id := TableID(1 + rng.IntN(2))
		key := fmt.Sprintf("key%05d", rng.IntN(3000))
		if rng.IntN(5) == 0 {
			del(t, e, id, key)
			delete(model[id], key)
		} else {
			val := fmt.Sprintf("v%d", i)
			set(t, e, id, key, val, NoSync)
			model[id][key] = val
		}
	}
	check := func(e *Engine) {
		for id, m := range model {
			tbl, err := e.Table(id)
			require.NoError(t, err)
			for i := 0; i < 3000; i++ {
				key := fmt.Sprintf("key%05d", i)
				if v, ok := m[key]; ok {
					mustGet(t, tbl, key, v)
				} else {
					mustNotFound(t, tbl, key)
				}
			}
		}
	}
	check(e)
	require.Greater(t, e.Metrics().Flushes, int64(1), "expected flushes driven by the memory budget")
	require.NoError(t, e.Flush())
	check(e)
	require.NoError(t, e.Close())

	e, err = Open("db", opts)
	require.NoError(t, err)
	check(e)
	require.NoError(t, e.Close())
}

// TestRecoverFromWAL closes without flushing; reopening must replay the WAL
// into the right tables.
func TestRecoverFromWAL(t *testing.T) {
	fs := vfs.NewMem()
	e, err := Open("db", &Options{FS: fs})
	require.NoError(t, err)
	_, err = e.CreateTable(1)
	require.NoError(t, err)
	_, err = e.CreateTable(2)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		set(t, e, TableID(1+i%2), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), NoSync)
	}
	del(t, e, 2, "k5")
	require.NoError(t, e.Close())

	e, err = Open("db", &Options{FS: fs})
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		tbl, _ := e.Table(TableID(1 + i%2))
		if i == 5 {
			mustNotFound(t, tbl, "k5")
			continue
		}
		mustGet(t, tbl, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	// New writes get higher sequence numbers than recovered ones.
	set(t, e, 1, "k0", "new", NoSync)
	a, _ := e.Table(1)
	mustGet(t, a, "k0", "new")
	require.NoError(t, e.Close())

	e, err = Open("db", &Options{FS: fs})
	require.NoError(t, err)
	a, _ = e.Table(1)
	mustGet(t, a, "k0", "new")
	require.NoError(t, e.Close())
}

// TestTableFlushIsPerTable checks that flushing one table leaves the others'
// memtables alone, as Cassandra's per-table flush does.
func TestTableFlushIsPerTable(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer e.Close()
	a, _ := e.CreateTable(1)
	_, _ = e.CreateTable(2)
	set(t, e, 1, "a", "1", NoSync)
	set(t, e, 2, "b", "2", NoSync)
	require.NoError(t, a.Flush())
	require.Equal(t, 1, tableFiles(e, 1))
	require.Equal(t, 0, tableFiles(e, 2))
	mustGet(t, a, "a", "1")
}

// TestMemoryBudgetFlushesLargest checks that exceeding the cleanup threshold
// flushes the largest memtable, not every table.
func TestMemoryBudgetFlushesLargest(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 300 << 10, MemTableCleanupThreshold: 0.5, DisableCompactions: true})
	require.NoError(t, err)
	defer e.Close()
	_, _ = e.CreateTable(1)
	quiet, _ := e.CreateTable(2)
	set(t, e, 2, "quiet", "1", NoSync)
	val := string(make([]byte, 100))
	for i := 0; i < 5000; i++ {
		set(t, e, 1, fmt.Sprintf("k%05d", i), val, NoSync)
	}
	busy, _ := e.Table(1)
	require.NoError(t, busy.Flush())
	require.Greater(t, tableFiles(e, 1), 1)
	require.Equal(t, 0, tableFiles(e, 2), "the quiet table must not be flushed")
	mustGet(t, quiet, "quiet", "1")
}

// TestQuietTableDoesNotPinWAL checks Cassandra's commitlog_total_space rule:
// a table whose memtable holds entries from the oldest WAL segment is flushed
// so the segments can be deleted.
func TestQuietTableDoesNotPinWAL(t *testing.T) {
	fs := vfs.NewMem()
	e, err := Open("db", &Options{
		FS:             fs,
		MemTableBudget: 1 << 20,
		WALSegmentSize: 16 << 10,
		WALTotalSpace:  64 << 10,
	})
	require.NoError(t, err)
	defer e.Close()
	_, _ = e.CreateTable(1)
	quiet, _ := e.CreateTable(2)
	set(t, e, 2, "quiet", "1", NoSync)
	val := string(make([]byte, 100))
	for i := 0; i < 5000; i++ {
		set(t, e, 1, fmt.Sprintf("k%05d", i), val, NoSync)
	}
	// Flush only the busy table; the quiet table may only have been flushed by
	// the WAL space rule.
	busy, _ := e.Table(1)
	require.NoError(t, busy.Flush())
	require.Equal(t, 1, tableFiles(e, 2), "the quiet table must have been flushed to free the WAL")
	e.mu.Lock()
	live := len(e.mu.walSegs)
	e.mu.Unlock()
	require.LessOrEqual(t, live, 2)
	// Obsolete segments are deleted or kept for reuse.
	require.LessOrEqual(t, walFiles(t, fs), live+walRecycleLimit)
	mustGet(t, quiet, "quiet", "1")
}

// TestDropTable checks that a dropped table's data and directory are gone, and
// that re-creating a table with the same ID does not replay the old table's
// WAL entries.
func TestDropTable(t *testing.T) {
	fs := vfs.NewMem()
	e, err := Open("db", &Options{FS: fs})
	require.NoError(t, err)
	a, _ := e.CreateTable(1)
	_, _ = e.CreateTable(2)
	set(t, e, 1, "flushed", "1", NoSync)
	require.NoError(t, a.Flush())
	set(t, e, 1, "unflushed", "1", NoSync) // only in the WAL
	set(t, e, 2, "other", "1", NoSync)

	require.NoError(t, e.DropTable(1))
	_, err = a.Get([]byte("flushed"))
	require.ErrorIs(t, err, ErrUnknownTable)
	_, err = e.Table(1)
	require.ErrorIs(t, err, ErrUnknownTable)
	_, err = fs.Stat(fs.PathJoin("db", tablesDirName, TableID(1).dirName()))
	require.Error(t, err, "the table's directory must be deleted")

	a2, err := e.CreateTable(1)
	require.NoError(t, err)
	mustNotFound(t, a2, "unflushed")
	require.NoError(t, e.Close())

	e, err = Open("db", &Options{FS: fs})
	require.NoError(t, err)
	defer e.Close()
	a2, _ = e.Table(1)
	mustNotFound(t, a2, "flushed")
	mustNotFound(t, a2, "unflushed")
	b, _ := e.Table(2)
	mustGet(t, b, "other", "1")
}

// TestCrashRecovery crashes the file system at random points, dropping
// unsynced data, and checks that every synced write survives in the right
// table and nothing else appears.
func TestCrashRecovery(t *testing.T) {
	for seed := uint64(0); seed < 20; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			fs := vfs.NewCrashableMem()
			opts := &Options{FS: fs, MemTableBudget: 64 << 10, WALSegmentSize: 8 << 10, WALTotalSpace: 32 << 10}
			e, err := Open("db", opts)
			require.NoError(t, err)
			for _, id := range []TableID{1, 2, 3} {
				_, err := e.CreateTable(id)
				require.NoError(t, err)
			}

			type tk struct {
				id  TableID
				key string
			}
			synced := map[tk]string{}  // must survive
			written := map[tk]string{} // may survive
			n := 500 + rng.IntN(3000)
			for i := 0; i < n; i++ {
				k := tk{TableID(1 + rng.IntN(3)), fmt.Sprintf("k%05d", i)} // each key written once
				val := fmt.Sprintf("v%d", i)
				sync := rng.IntN(10) == 0
				wo := NoSync
				if sync {
					wo = Sync
				}
				set(t, e, k.id, k.key, val, wo)
				written[k] = val
				if sync {
					// A synced write makes every earlier write durable too.
					for k, v := range written {
						synced[k] = v
					}
				}
			}
			crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: rng.IntN(100), RNG: rng})
			require.NoError(t, e.Close())

			e2, err := Open("db", &Options{FS: crashed, MemTableBudget: 64 << 10})
			require.NoError(t, err)
			for k, v := range written {
				tbl, err := e2.Table(k.id)
				require.NoError(t, err)
				got, ok := getVal(t, tbl, k.key)
				if _, mustExist := synced[k]; mustExist {
					require.True(t, ok, "synced key %v", k)
				}
				if ok {
					require.Equal(t, v, got, "key %v", k)
				}
			}
			require.NoError(t, e2.Close())
		})
	}
}

// TestConcurrentReadWrite runs readers while a writer writes to two tables
// and memtables are flushed, under the race detector. Each key's value only
// grows, so a reader must never see a value go backwards.
func TestConcurrentReadWrite(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 48 << 10})
	require.NoError(t, err)
	defer e.Close()
	tables := []*Table{}
	for _, id := range []TableID{1, 2} {
		tbl, err := e.CreateTable(id)
		require.NoError(t, err)
		tables = append(tables, tbl)
	}

	const keys = 200
	var wg sync.WaitGroup
	done := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			tbl := tables[r%2]
			seen := make([]int, keys)
			rng := rand.New(rand.NewPCG(uint64(r), 1))
			for {
				select {
				case <-done:
					return
				default:
				}
				k := rng.IntN(keys)
				v, err := tbl.Get([]byte(fmt.Sprintf("k%03d", k)))
				if err == ErrNotFound {
					if seen[k] != 0 {
						t.Errorf("key %d disappeared after value %d", k, seen[k])
						return
					}
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				r, err := row.Decode(v)
				if err != nil || len(r.Cells) != 1 {
					t.Errorf("bad row %v %v", r, err)
					return
				}
				var n int
				fmt.Sscanf(string(r.Cells[0].Value), "%d", &n)
				if n < seen[k] {
					t.Errorf("key %d went backwards: %d after %d", k, n, seen[k])
					return
				}
				seen[k] = n
			}
		}(r)
	}
	for i := 1; i <= 20000; i++ {
		require.NoError(t, e.Put(TableID(1+i%2), []byte(fmt.Sprintf("k%03d", i%keys)), cellRow(int64(i), fmt.Sprint(i), false), NoSync))
	}
	require.NoError(t, e.Flush())
	close(done)
	wg.Wait()
}

func TestClosedAndLocked(t *testing.T) {
	fs := vfs.NewMem()
	e, err := Open("db", &Options{FS: fs})
	require.NoError(t, err)
	_, _ = e.CreateTable(1)
	_, err = Open("db", &Options{FS: fs})
	require.Error(t, err, "a second Open must fail while the engine is locked")
	require.NoError(t, e.Close())
	require.ErrorIs(t, e.Put(1, []byte("a"), cellRow(1, "a", false), NoSync), ErrClosed)
	require.ErrorIs(t, e.Close(), ErrClosed)
}

// TestTimestampSemantics checks Cassandra's last-write-wins by timestamp
// across memtables and sstables: arrival order does not matter.
func TestTimestampSemantics(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	put := func(key string, r *row.Row) {
		if r.Deletion == (row.Deletion{}) {
			r.Deletion.MarkedForDeleteAt = row.NoTimestamp
		}
		require.NoError(t, e.Put(1, []byte(key), row.Append(nil, r), NoSync))
	}
	cell := func(col uint32, ts int64, v string) row.Cell {
		return row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: []byte(v)}
	}
	read := func(key string) *row.Row {
		v, err := tbl.Get([]byte(key))
		require.NoError(t, err)
		r, err := row.Decode(v)
		require.NoError(t, err)
		return r
	}

	// A newer write that arrives first is not overwritten by an older one,
	// whether they meet in the memtable or across an sstable.
	put("a", &row.Row{Cells: []row.Cell{cell(1, 20, "new")}})
	require.NoError(t, tbl.Flush())
	put("a", &row.Row{Cells: []row.Cell{cell(1, 10, "old")}})
	require.Equal(t, []row.Cell{cell(1, 20, "new")}, read("a").Cells)

	// An older tombstone arriving later does not delete newer data.
	put("a", &row.Row{Cells: []row.Cell{{Column: 1, Timestamp: 15, LocalDeletionTime: 1}}})
	require.Equal(t, []row.Cell{cell(1, 20, "new")}, read("a").Cells)

	// Writes to different columns merge (an upsert), across sstables.
	put("b", &row.Row{Cells: []row.Cell{cell(1, 5, "x")}})
	require.NoError(t, tbl.Flush())
	put("b", &row.Row{Cells: []row.Cell{cell(2, 6, "y")}})
	require.Equal(t, []row.Cell{cell(1, 5, "x"), cell(2, 6, "y")}, read("b").Cells)

	// A row deletion shadows older cells in every sstable; newer cells
	// survive. The read stops at sstables entirely older than the deletion.
	put("c", &row.Row{Cells: []row.Cell{cell(1, 1, "v1")}})
	require.NoError(t, tbl.Flush())
	put("c", &row.Row{Cells: []row.Cell{cell(2, 2, "v2")}})
	require.NoError(t, tbl.Flush())
	put("c", &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: 50, LocalDeletionTime: 1}})
	put("c", &row.Row{Cells: []row.Cell{cell(3, 60, "after")}})
	r := read("c")
	require.Equal(t, int64(50), r.Deletion.MarkedForDeleteAt)
	require.Equal(t, []row.Cell{cell(3, 60, "after")}, r.Cells)
}

// TestConcurrentWriters runs several writers against two tables while
// memtables are flushed, and checks the final contents against a model.
// Each writer owns its keys, so the expected value of every key is known.
func TestConcurrentWriters(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 64 << 10})
	require.NoError(t, err)
	defer e.Close()
	for _, id := range []TableID{1, 2} {
		_, err := e.CreateTable(id)
		require.NoError(t, err)
	}
	const writers, perWriter = 8, 2000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				b := NewBatch()
				key := []byte(fmt.Sprintf("w%d-k%03d", w, i%100))
				val := cellRow(int64(i), fmt.Sprint(i), false)
				b.Put(1, key, val) // one batch writes both tables
				b.Put(2, key, val)
				wo := NoSync
				if i%50 == 0 {
					wo = Sync
				}
				if err := e.Apply(b, wo); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	require.NoError(t, e.Flush())
	for _, id := range []TableID{1, 2} {
		tbl, _ := e.Table(id)
		for w := 0; w < writers; w++ {
			for k := 0; k < 100; k++ {
				// The last write to key k is i = perWriter-100+k.
				mustGet(t, tbl, fmt.Sprintf("w%d-k%03d", w, k), fmt.Sprint(perWriter-100+k))
			}
		}
	}
}

// TestPeriodicSync checks Cassandra's default periodic commit log mode: NoSync
// writes become durable at the next periodic sync.
func TestPeriodicSync(t *testing.T) {
	fs := vfs.NewCrashableMem()
	e, err := Open("db", &Options{FS: fs, WALSyncPeriod: -1})
	require.NoError(t, err)
	_, _ = e.CreateTable(1)
	set(t, e, 1, "a", "1", NoSync)
	require.NoError(t, e.SyncWAL()) // what the periodic syncer does
	set(t, e, 1, "b", "2", NoSync)  // not synced
	crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	require.NoError(t, e.Close())

	e, err = Open("db", &Options{FS: crashed})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.Table(1)
	mustGet(t, tbl, "a", "1")
	mustNotFound(t, tbl, "b")

	// The background syncer runs on its own.
	e2fs := vfs.NewCrashableMem()
	e2, err := Open("db", &Options{FS: e2fs, WALSyncPeriod: 10 * time.Millisecond})
	require.NoError(t, err)
	_, _ = e2.CreateTable(1)
	set(t, e2, 1, "c", "3", NoSync)
	time.Sleep(100 * time.Millisecond)
	crashed2 := e2fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	require.NoError(t, e2.Close())
	e3, err := Open("db", &Options{FS: crashed2})
	require.NoError(t, err)
	defer e3.Close()
	tbl3, _ := e3.Table(1)
	mustGet(t, tbl3, "c", "3")
}

// TestWALRecycleReplay checks that a recycled WAL segment replays only the
// records written to it after reuse: the old records past the new end carry
// the old segment number and end the replay, whether the new segment was
// closed or the machine crashed while it was open.
func TestWALRecycleReplay(t *testing.T) {
	for _, closeIt := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%t", closeIt), func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			require.NoError(t, fs.MkdirAll("db", 0755))
			require.NoError(t, syncDir(fs, ""))
			old, err := createWAL(fs, "db", 1, 0)
			require.NoError(t, err)
			require.NoError(t, syncDir(fs, "db"))
			for i := 0; i < 2000; i++ {
				require.NoError(t, old.add([]byte(fmt.Sprintf("old-%04d", i)), nil))
			}
			require.NoError(t, old.close())

			w, err := createWAL(fs, "db", 5, 1)
			require.NoError(t, err)
			require.NoError(t, syncDir(fs, "db"))
			var ps pendingSync
			for i := 0; i < 3; i++ {
				p := &ps
				if i < 2 {
					p = nil
				}
				require.NoError(t, w.add([]byte(fmt.Sprintf("new-%d", i)), p))
			}
			require.NoError(t, ps.wait())
			replayFS := vfs.FS(fs)
			if closeIt {
				require.NoError(t, w.close())
			} else {
				replayFS = fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
				require.NoError(t, w.close())
			}
			_, err = replayFS.Stat("db/" + walFileName(1))
			require.Error(t, err, "the old segment was renamed")

			var got []string
			require.NoError(t, replayWAL(replayFS, "db/"+walFileName(5), 5, func(data []byte) error {
				got = append(got, string(data))
				return nil
			}))
			require.Equal(t, []string{"new-0", "new-1", "new-2"}, got)
		})
	}
}

// TestWALRecycling checks that obsolete segments are reused and that reused
// segments survive crashes like new ones: every synced write is recovered,
// and nothing newer than the crash point appears.
func TestWALRecycling(t *testing.T) {
	fs := vfs.NewCrashableMem()
	opts := func(fs vfs.FS) *Options {
		return &Options{FS: fs, MemTableBudget: 32 << 10, WALSegmentSize: 4 << 10, WALSyncPeriod: -1}
	}
	e, err := Open("db", opts(fs))
	require.NoError(t, err)
	_, err = e.CreateTable(1)
	require.NoError(t, err)
	rng := rand.New(rand.NewPCG(3, 3))
	recycled := false
	for round := 0; round < 20; round++ {
		written := map[string]string{}
		synced := map[string]string{}
		for i := 0; i < 300; i++ {
			k, v := fmt.Sprintf("r%02d-k%03d", round, i), fmt.Sprintf("v%d-%d", round, i)
			sync := rng.IntN(8) == 0
			wo := NoSync
			if sync {
				wo = Sync
			}
			set(t, e, 1, k, v, wo)
			written[k] = v
			if sync {
				for k, v := range written {
					synced[k] = v
				}
			}
			e.mu.Lock()
			recycled = recycled || len(e.mu.recycledWAL) > 0
			e.mu.Unlock()
		}
		crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: rng.IntN(100), RNG: rng})
		require.NoError(t, e.Close())
		fs = crashed
		e, err = Open("db", opts(fs))
		require.NoError(t, err)
		tbl, err := e.Table(1)
		require.NoError(t, err)
		for k, v := range written {
			got, ok := getVal(t, tbl, k)
			if _, must := synced[k]; must {
				require.True(t, ok, "round %d: synced key %s lost", round, k)
			}
			if ok {
				require.Equal(t, v, got)
			}
		}
	}
	require.True(t, recycled, "no WAL segment was recycled")
	require.NoError(t, e.Close())
}
