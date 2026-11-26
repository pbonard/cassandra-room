package lsm

import (
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestReverseIter: reverse iteration returns exactly the forward rows,
// backwards, over memtables and sstables, versions merged, for any bounds,
// and does not see writes made after it was created.
func TestReverseIter(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_000_000)
	e, tbl := purgeEngine(t, vfs.NewMem(), &clock, TableOptions{})
	defer e.Close()
	rng := rand.New(rand.NewSource(7))
	key := func() string { return fmt.Sprintf("k%03d", rng.Intn(400)) }
	for round := 0; round < 5; round++ {
		for i := 0; i < 300; i++ {
			if rng.Intn(5) == 0 {
				putCell(t, e, key(), tombCell(int64(round*1000+i), clock.Load()))
			} else {
				putCell(t, e, key(), liveCell(int64(round*1000+i), fmt.Sprint(round, i)))
			}
		}
		if round < 4 {
			require.NoError(t, tbl.Flush()) // the last round stays in the memtable
		}
	}
	type row struct{ k, v string }
	forward := func(lo, hi []byte) []row {
		it, err := tbl.NewIter(lo, hi)
		require.NoError(t, err)
		defer it.Close()
		var out []row
		for ok := it.First(); ok; ok = it.Next() {
			out = append(out, row{string(it.Key()), string(it.Value())})
		}
		require.NoError(t, it.Error())
		return out
	}
	backward := func(lo, hi []byte) []row {
		it, err := tbl.NewReverseIter(lo, hi)
		require.NoError(t, err)
		defer it.Close()
		var out []row
		for ok := it.Last(); ok; ok = it.Prev() {
			out = append(out, row{string(it.Key()), string(it.Value())})
		}
		require.NoError(t, it.Error())
		slices.Reverse(out)
		return out
	}
	bound := func() []byte {
		if rng.Intn(4) == 0 {
			return nil
		}
		return []byte(key())
	}
	for i := 0; i < 200; i++ {
		lo, hi := bound(), bound()
		want := forward(lo, hi)
		require.Equal(t, want, backward(lo, hi), "[%s, %s)", lo, hi)
	}
	require.NotEmpty(t, forward(nil, nil))

	// A consistent view: later writes are not seen.
	it, err := tbl.NewReverseIter(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	putCell(t, e, "k999", liveCell(1<<40, "later"))
	require.True(t, it.Last())
	require.NotEqual(t, "k999", string(it.Key()))
	// SeekLT repositions below a key.
	require.True(t, it.SeekLT([]byte("k200")))
	require.Less(t, string(it.Key()), "k200")
}
