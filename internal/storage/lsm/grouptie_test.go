package lsm

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// partitionFilter mimics the database's deletion filter on keys
// "<partition>/h" (a partition deletion, as a live cell of column 99 whose
// timestamp is the deletion's and value its local deletion time) and "<partition>/r<n>" (rows): rows the
// deletion shadows are dropped, and the deletion itself once purgeable.
type partitionFilter struct {
	prefix []byte
	del    int64
}

func (f *partitionFilter) Filter(key, value []byte, env *FilterEnv) ([]byte, bool, error) {
	slash := bytes.IndexByte(key, '/')
	prefix, rest := key[:slash+1], key[slash+1:]
	if !bytes.Equal(prefix, f.prefix) {
		f.prefix, f.del = append(f.prefix[:0], prefix...), row.NoTimestamp
		if env.Group != nil {
			env.Group()
		}
	}
	r, err := row.Decode(value)
	if err != nil {
		return nil, false, err
	}
	if rest[0] == 'h' {
		// The deletion's local deletion time is the cell's value.
		f.del = r.Cells[0].Timestamp
		ldt, err := strconv.ParseInt(string(r.Cells[0].Value), 10, 64)
		if err != nil || ldt >= env.GCBefore {
			return nil, false, err
		}
		limit, err := env.MaxPurgeable(prefix, append(bytes.Clone(prefix), 0xff), f.del, f.del)
		return nil, f.del < limit, err
	}
	for _, c := range r.Cells {
		if c.Timestamp > f.del {
			return nil, false, nil
		}
	}
	return nil, true, nil
}

// TestIncrementalReleasePartitionDeletion: a small sstable A holds a
// partition deletion; a large one, B, holds the rows it shadows and many
// other partitions. The compaction purges the deletion and drops the rows;
// those are different keys, held by different inputs. If A were released
// early (B still live), the rows would come back: the inputs holding a
// partition's entries are released together.
func TestIncrementalReleasePartitionDeletion(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	fs := vfs.NewMem()
	filter := func() RowFilter { return &partitionFilter{} }
	// The sstables are written whole (B over every partition), then
	// compacted in small fragments.
	e, tbl := purgeEngine(t, fs, &clock, TableOptions{GCGraceSeconds: -1, RowFilter: filter})
	put := func(k string, c row.Cell) { putCell(t, e, k, c) }
	for p := 0; p < 10; p++ {
		for i := 0; i < 100; i++ {
			put(fmt.Sprintf("p%03d/r%04d", p, i), liveCell(1, "old"))
		}
	}
	require.NoError(t, tbl.Flush()) // B
	put("p000/h", row.Cell{Column: 99, Timestamp: 2, LocalDeletionTime: row.NoDeletionTime, Value: []byte(strconv.FormatInt(clock.Load()-10, 10))})
	require.NoError(t, tbl.Flush()) // A
	require.Len(t, tableFileList(e, tbl), 2)
	require.NoError(t, e.Close())
	e, tbl = purgeEngine(t, fs, &clock, TableOptions{FragmentSize: 2 << 10, GCGraceSeconds: -1, RowFilter: filter})
	defer e.Close()
	deleted := func() {
		for i := 0; i < 100; i += 9 {
			require.False(t, liveAt(t, tbl, fmt.Sprintf("p000/r%04d", i), clock.Load()), "p000/r%04d resurrected", i)
		}
	}
	releases := 0
	e.testAfterEarlyRelease = func() {
		releases++
		for _, f := range tableFileList(e, tbl) {
			t.Logf("release %d: file %s..%s compacting=%v", releases, f.smallest.UserKey, f.largest.UserKey, f.compacting)
		}
		deleted()
	}
	compactAll(t, e, tbl, tableFileList(e, tbl))
	deleted()
	keys := rawRows(t, tbl)
	require.Len(t, keys, 900, "the deletion and the rows it shadows are purged")
	require.False(t, strings.HasPrefix(keys[0], "p000/"))
	t.Logf("%d early releases", releases)
}

// TestMerkleTreeAfterCompaction: a replica that compacted away the rows a
// partition deletion shadows hashes the same as one that did not (the
// validation applies the table's row filter, as compaction does).
func TestMerkleTreeAfterCompaction(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	opts := TableOptions{GCGraceSeconds: 1 << 30, RowFilter: func() RowFilter { return &partitionFilter{} }}
	var tables []*Table
	for r := 0; r < 2; r++ {
		e, tbl := purgeEngine(t, vfs.NewMem(), &clock, opts)
		defer e.Close()
		for p := 0; p < 3; p++ {
			for i := 0; i < 20; i++ {
				putCell(t, e, fmt.Sprintf("p%03d/r%04d", p, i), liveCell(1, "old"))
			}
		}
		require.NoError(t, tbl.Flush())
		putCell(t, e, "p001/h", row.Cell{Column: 99, Timestamp: 2, LocalDeletionTime: row.NoDeletionTime, Value: []byte(strconv.FormatInt(clock.Load()-10, 10))})
		require.NoError(t, tbl.Flush())
		if r == 0 {
			compactAll(t, e, tbl, tableFileList(e, tbl)) // drops p001's rows
			require.Len(t, rawRows(t, tbl), 41)
		}
		tables = append(tables, tbl)
	}
	a, err := tables[0].MerkleTree(FullRing, 4)
	require.NoError(t, err)
	b, err := tables[1].MerkleTree(FullRing, 4)
	require.NoError(t, err)
	diff, err := a.Diff(b)
	require.NoError(t, err)
	require.Empty(t, diff, "the same data, compacted or not")
}
