package lsm

import (
	"bytes"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestOwnership: densities and shards are measured over the owned token
// space. A node owning an eighth of the ring in one range sees a flush of
// all its data span 1 (not 1/8, which made it 8 times denser), and shards
// cut its range, not the whole ring.
func TestOwnership(t *testing.T) {
	eighth := uint64(1) << 61
	o := newOwnership([]TokenRange{{First: 3 * eighth, Last: 4*eighth - 1}})
	all := &fileMeta{size: 1 << 20}
	all.smallest.UserKey, all.largest.UserKey = tokenKey(3*eighth), tokenKey(4*eighth-1)
	require.InDelta(t, 1.0, o.span(all.smallest.UserKey, all.largest.UserKey), 1e-9)
	require.InDelta(t, float64(1<<20), o.density(all), 1)
	require.InDelta(t, 8*float64(1<<20), (*ownership)(nil).density(all), 1e-3*float64(1<<20), "the whole-ring measure")

	bs := o.boundaries(4)
	require.Len(t, bs, 3)
	for i, b := range bs {
		want := 3*eighth + uint64(i+1)*eighth/4
		require.InDelta(t, float64(want), float64(keyToken(b)), float64(eighth)/1e6, "boundary %d", i)
	}

	// Two ranges: an even cut puts half the owned tokens on each side.
	two := newOwnership([]TokenRange{{First: 0, Last: eighth - 1}, {First: 4 * eighth, Last: 5*eighth - 1}})
	b := two.boundaries(2)
	require.Len(t, b, 1)
	require.InDelta(t, float64(4*eighth), float64(keyToken(b[0])), float64(eighth)/1e6)
	require.Nil(t, newOwnership(nil))
	require.Equal(t, shardBoundaries(4), (*ownership)(nil).boundaries(4))
	require.InDelta(t, 1.0, newOwnership([]TokenRange{{0, math.MaxUint64}}).span(tokenKey(0), tokenKey(math.MaxUint64)), 1e-9)
}

// TestPartitionFiltersAndOldFiles: sstables written before partition
// filters (whole-key filters) still read, and range reads inside one
// partition skip the new ones without it.
func TestPartitionFiltersAndOldFiles(t *testing.T) {
	fs := vfs.NewMem()
	// Keys: 8-byte token, then "p<n>" and a 0 terminator (the partition),
	// then a clustering byte.
	key := func(p, c int) []byte {
		k := tokenKey(uint64(p) << 56)
		k = append(k, 'p', byte('0'+p), 0)
		return append(k, byte(c))
	}
	prefix := func(k []byte) int {
		if i := bytes.IndexByte(k[min(8, len(k)):], 0); i >= 0 {
			return 8 + i + 1
		}
		return len(k)
	}
	e, err := Open("db", &Options{FS: fs})
	require.NoError(t, err)
	tbl, err := e.CreateTable(1)
	require.NoError(t, err)
	put := func(e *Engine, k []byte) { putCell(t, e, string(k), liveCell(1, "v")) }
	put(e, key(1, 1))
	require.NoError(t, tbl.Flush()) // an old file
	require.NoError(t, e.Close())

	e, err = Open("db", &Options{FS: fs, PartitionPrefix: prefix, DisableCompactions: true})
	require.NoError(t, err)
	defer e.Close()
	tbl, err = e.Table(1)
	require.NoError(t, err)
	for p := 2; p <= 5; p++ {
		put(e, key(p, 1))
		put(e, key(9, p)) // so the files' key ranges overlap
		require.NoError(t, tbl.Flush()) // new files
	}
	for p := 1; p <= 5; p++ {
		require.True(t, liveAt(t, tbl, string(key(p, 1)), 0), "partition %d", p)
		lo := key(p, 0)
		hi := key(p, 9)
		it, err := tbl.NewIter(lo, hi)
		require.NoError(t, err)
		n := 0
		for ok := it.First(); ok; ok = it.Next() {
			n++
		}
		require.NoError(t, it.Close())
		require.Equal(t, 1, n, "partition %d", p)
	}
	before := e.Metrics().BloomSkips
	it, err := tbl.NewIter(key(5, 0), key(5, 9))
	require.NoError(t, err)
	require.True(t, it.First())
	require.NoError(t, it.Close())
	require.EqualValues(t, 3, e.Metrics().BloomSkips-before, "the files of partitions 2 to 4 overlap 5's range, without it")
}
