package lsm

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestWALArchive: with an archive directory, every segment the engine moves
// on from (rotated, obsolete after flushes, or recovered after a restart)
// is kept there, never reused, and reads back every write in order.
func TestWALArchive(t *testing.T) {
	fs := vfs.NewMem()
	o := &Options{FS: fs, WALArchiveDir: "archive", WALSegmentSize: 4 << 10}
	e, err := Open("db", o)
	require.NoError(t, err)
	_, err = e.CreateTable(1)
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		set(t, e, 1, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%d", i), NoSync)
		if i%100 == 99 {
			require.NoError(t, e.Flush()) // segments become obsolete
		}
	}
	require.NoError(t, e.ArchiveWAL())
	for i := 500; i < 520; i++ {
		set(t, e, 1, fmt.Sprintf("k%04d", i), fmt.Sprintf("v%d", i), NoSync)
	}
	require.NoError(t, e.Close())
	// The last writes are archived when the engine reopens.
	e, err = Open("db", o)
	require.NoError(t, err)
	require.NoError(t, e.Close())

	names, err := fs.List("archive")
	require.NoError(t, err)
	require.Greater(t, len(names), 3)
	var keys []string
	var last uint64
	for _, n := range sortedWALs(names) {
		require.NoError(t, ReadWAL(fs, fs.PathJoin("archive", n), func(w WALEntry) error {
			require.Greater(t, w.Seq, last)
			last = w.Seq
			if w.Table == 1 {
				keys = append(keys, string(w.Key))
			}
			return nil
		}))
	}
	require.Len(t, keys, 520)
	for i, k := range keys {
		require.Equal(t, fmt.Sprintf("k%04d", i), k)
	}
	require.Error(t, ReadWAL(fs, "archive/x.sst", func(WALEntry) error { return nil }))
}

func sortedWALs(names []string) []string {
	var out []string
	for num := uint64(0); num < 10000 && len(out) < len(names); num++ {
		for _, n := range names {
			if n == walFileName(num) {
				out = append(out, n)
			}
		}
	}
	return out
}
