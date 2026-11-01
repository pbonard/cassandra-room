package lsm

import (
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestReadSnapshot checks that a snapshot keeps returning the rows as of
// its creation across later writes, flushes, compactions (whose replaced
// files stay readable) and drops of newer data.
func TestReadSnapshot(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 64 << 10})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	for i := 0; i < 300; i++ {
		set(t, e, 1, fmt.Sprintf("k%03d", i), "old", NoSync)
	}
	sn, err := tbl.NewSnapshot()
	require.NoError(t, err)
	for round := 0; round < 20; round++ {
		for i := 0; i < 300; i++ {
			set(t, e, 1, fmt.Sprintf("k%03d", i), fmt.Sprint(round), NoSync)
		}
		set(t, e, 1, fmt.Sprintf("new%02d", round), "x", NoSync)
		require.NoError(t, tbl.Flush())
	}
	del(t, e, 1, "k007")
	require.NoError(t, e.WaitForCompactions())
	require.Positive(t, e.Metrics().Compactions)

	for i := 0; i < 300; i++ {
		v, err := sn.Get([]byte(fmt.Sprintf("k%03d", i)))
		require.NoError(t, err)
		r, err := row.Decode(v)
		require.NoError(t, err)
		require.Equal(t, "old", string(r.Live(0)[0].Value))
	}
	_, err = sn.Get([]byte("new00"))
	require.ErrorIs(t, err, ErrNotFound)
	it, err := sn.NewIter(nil, nil)
	require.NoError(t, err)
	require.NoError(t, sn.Close()) // the iterator holds its own reference
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	require.Equal(t, 300, n)
	mustNotFound(t, tbl, "k007")
	mustGet(t, tbl, "k008", "19")
}

// TestSnapshotToDiskAndImport takes an on-disk snapshot, keeps writing and
// compacting, then restores the snapshot into a new table and checks it
// holds exactly the snapshot's rows; the snapshot's files survive the live
// table's compactions.
func TestSnapshotToDiskAndImport(t *testing.T) {
	fs := vfs.NewMem()
	e, err := Open("db", &Options{FS: fs, MemTableBudget: 64 << 10})
	require.NoError(t, err)
	tbl, _ := e.CreateTable(1)
	want := map[string]string{}
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("k%04d", i%700)
		set(t, e, 1, k, fmt.Sprint(i), NoSync)
		want[k] = fmt.Sprint(i)
	}
	dir, err := tbl.SnapshotToDisk("s1")
	require.NoError(t, err)
	_, err = tbl.SnapshotToDisk("s1")
	require.Error(t, err, "snapshot names are unique")
	for i := 0; i < 3000; i++ {
		set(t, e, 1, fmt.Sprintf("k%04d", i%900), "later", NoSync)
	}
	require.NoError(t, e.Flush())
	require.NoError(t, e.WaitForCompactions())

	// Reopen: snapshots are not engine files and survive recovery.
	require.NoError(t, e.Close())
	e, err = Open("db", &Options{FS: fs})
	require.NoError(t, err)
	defer e.Close()
	tbl, err = e.Table(1)
	require.NoError(t, err)
	files, err := tbl.SnapshotFiles("s1")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.Contains(t, f, dir)
	}

	restored, err := e.CreateTable(2)
	require.NoError(t, err)
	require.NoError(t, restored.Import(fs, files))
	require.Equal(t, want, scanAll(t, restored, nil, nil))
	// Importing again changes nothing: fragments reconcile by timestamp.
	require.NoError(t, restored.Import(fs, files))
	require.Equal(t, want, scanAll(t, restored, nil, nil))
	// The live table was not affected.
	mustGet(t, tbl, "k0001", "later")

	require.NoError(t, e.ClearSnapshot("s1"))
	_, err = tbl.SnapshotFiles("s1")
	require.Error(t, err)
	require.Equal(t, want, scanAll(t, restored, nil, nil))
}

// TestImportRejectsBadFiles checks that Import validates before installing
// anything.
func TestImportRejectsBadFiles(t *testing.T) {
	fs := vfs.NewMem()
	e, err := Open("db", &Options{FS: fs})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	set(t, e, 1, "a", "1", NoSync)
	require.NoError(t, fs.MkdirAll("out", 0755))
	paths, err := tbl.ExportRange(nil, nil, fs, "out")
	require.NoError(t, err)
	require.Len(t, paths, 1)

	// Not an sstable.
	require.NoError(t, writeFileSync(fs, "out/junk.sst", []byte("not an sstable")))
	other, _ := e.CreateTable(2)
	require.Error(t, other.Import(fs, []string{paths[0], "out/junk.sst"}))
	// A value the Merger cannot decode.
	bad, err := newSSTBuilder(fs, "out", 99, e.writerOpts, e.merger, false, 0)
	require.NoError(t, err)
	require.NoError(t, bad.add([]byte("k"), []byte{0xff}, 1<<8|uint64(2)))
	_, err = bad.finish()
	require.NoError(t, err)
	require.Error(t, other.Import(fs, []string{"out/" + tableFileName(99)}))
	require.Empty(t, scanAll(t, other, nil, nil))
	require.Empty(t, tableFileList(e, other))
}

// TestRepair runs a repair between two replicas: they build Merkle trees,
// stream the differing ranges both ways, and end with identical trees and
// rows. A deletion purged on one replica but not the other does not count
// as a difference.
func TestRepair(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	open := func() (*Engine, *Table) {
		e, err := Open("db", &Options{
			FS: vfs.NewMem(), MemTableBudget: 64 << 10,
			Now: func() time.Time { return time.Unix(clock.Load(), 0) },
			TableOptions: func(TableID) TableOptions {
				return TableOptions{GCGraceSeconds: 100}
			},
		})
		require.NoError(t, err)
		tbl, _ := e.CreateTable(1)
		return e, tbl
	}
	a, ta := open()
	defer a.Close()
	b, tb := open()
	defer b.Close()
	key := func(i int) []byte {
		k := fmt.Sprintf("p%05d", i)
		return append(tokenKey(xxhash.Sum64String(k)), k...)
	}
	put := func(e *Engine, i int, ts int64, v string) {
		putCell(t, e, string(key(i)), liveCell(ts, v))
	}
	for i := 0; i < 3000; i++ {
		put(a, i, 1, "base")
		put(b, i, 1, "base")
	}
	// Divergence: some writes reached only one replica.
	for i := 0; i < 3000; i += 97 {
		put(a, i, 2, "a-only")
	}
	for i := 50; i < 3000; i += 131 {
		put(b, i, 3, "b-only")
	}
	// A deletion both replicas have, then purged on A only.
	putCell(t, a, string(key(5)), tombCell(10, clock.Load()))
	putCell(t, b, string(key(5)), tombCell(10, clock.Load()))
	require.NoError(t, ta.Flush())
	require.NoError(t, tb.Flush())
	clock.Add(200)
	compactAll(t, a, ta, tableFileList(a, ta))

	const depth = 8
	trees := func() (*MerkleTree, *MerkleTree) {
		ma, err := ta.MerkleTree(FullRing, depth)
		require.NoError(t, err)
		mb, err := tb.MerkleTree(FullRing, depth)
		require.NoError(t, err)
		return ma, mb
	}
	ma, mb := trees()
	diff, err := ma.Diff(mb)
	require.NoError(t, err)
	require.NotEmpty(t, diff)
	require.Less(t, len(diff), 1<<depth, "only some ranges differ")
	stream := func(from, to *Table, fs vfs.FS, r TokenRange, n int) {
		dir := fmt.Sprintf("stream-%d", n)
		require.NoError(t, fs.MkdirAll(dir, 0755))
		lo, hi := r.Bounds()
		paths, err := from.ExportRange(lo, hi, fs, dir)
		require.NoError(t, err)
		require.NoError(t, to.Import(fs, paths))
	}
	xfs := vfs.NewMem()
	for i, r := range diff {
		stream(ta, tb, xfs, r, 2*i)
		stream(tb, ta, xfs, r, 2*i+1)
	}
	ma, mb = trees()
	require.Equal(t, ma.Leaves, mb.Leaves)
	require.Equal(t, ma.Root(), mb.Root())
	ra, rb := scanAll(t, ta, nil, nil), scanAll(t, tb, nil, nil)
	require.Equal(t, ra, rb)
	require.Equal(t, "b-only", ra[string(key(50))])
	require.Equal(t, "a-only", ra[string(key(97))])
	_, ok := ra[string(key(5))]
	require.False(t, ok)
}

// TestMerkleLeafRanges checks that leaf ranges tile the tree's range and
// agree with leafOf.
func TestMerkleLeafRanges(t *testing.T) {
	for _, r := range []TokenRange{FullRing, {100, 1099}, {math.MaxUint64 - 9, math.MaxUint64}, {7, 7}} {
		for _, depth := range []int{0, 1, 3, 5} {
			m := &MerkleTree{Range: r, Depth: depth, Leaves: make([]uint64, 1<<depth)}
			next := r.First
			for i := range m.Leaves {
				lr := m.leafRange(i)
				if lr.Last+1 == lr.First { // empty leaf (fewer tokens than leaves)
					continue
				}
				require.Equal(t, next, lr.First, "%+v depth %d leaf %d", r, depth, i)
				require.EqualValues(t, i, m.leafOf(lr.First))
				require.EqualValues(t, i, m.leafOf(lr.Last))
				next = lr.Last + 1
			}
			require.Equal(t, r.Last+1, next)
		}
	}
}
