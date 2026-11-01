package lsm

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestSelectPickSize checks the examples in Cassandra's
// UnifiedCompactionStrategy.Bucket.constructPick comments.
func TestSelectPickSize(t *testing.T) {
	tbl := &Table{}
	tbl.opts = TableOptions{ScalingParameters: []int{2}}
	tbl.opts.ensureDefaults()
	// "for w=2 and 55 sstables, pick a compaction of 16 sstables"
	require.Equal(t, 16, tbl.selectPickSize(0, 55, 64))
	tbl.opts.ScalingParameters = []int{-2}
	// "for w=-2 and 55, this means pick a compaction of 48"
	require.Equal(t, 48, tbl.selectPickSize(0, 55, 64))

	require.Equal(t, 4, (&TableOptions{ScalingParameters: []int{2}}).fanout(0))
	require.Equal(t, 4, (&TableOptions{ScalingParameters: []int{2}}).threshold(0))
	require.Equal(t, 4, (&TableOptions{ScalingParameters: []int{-2}}).fanout(0))
	require.Equal(t, 2, (&TableOptions{ScalingParameters: []int{-2}}).threshold(0))
	require.Equal(t, 2, (&TableOptions{ScalingParameters: []int{0}}).threshold(0))
}

func TestMaxOverlap(t *testing.T) {
	f := func(a, b string) *fileMeta {
		return &fileMeta{smallest: makeKey(a), largest: makeKey(b)}
	}
	files := []*fileMeta{f("a", "c"), f("b", "d"), f("c", "e"), f("x", "z")}
	n, set := maxOverlap(files)
	require.Equal(t, 3, n) // a-c, b-d and c-e all contain "c"
	require.Len(t, set, 3)
}

// TestCompactionUCS writes many small flushes and checks that UCS keeps the
// number of overlapping sstables below the threshold, for tiered and leveled
// settings, without losing data.
func TestCompactionUCS(t *testing.T) {
	for _, w := range []int{2, 0, -2} {
		t.Run(fmt.Sprintf("W=%d", w), func(t *testing.T) {
			e, err := Open("db", &Options{
				FS:           vfs.NewMem(),
				TableOptions: func(TableID) TableOptions { return TableOptions{ScalingParameters: []int{w}} },
			})
			require.NoError(t, err)
			defer e.Close()
			tbl, _ := e.CreateTable(1)
			for f := 0; f < 40; f++ {
				for i := 0; i < 50; i++ {
					set(t, e, 1, fmt.Sprintf("k%04d", (f*50+i)%700), fmt.Sprint(f), NoSync)
				}
				// Every flush also covers the whole key range, so all
				// flushes overlap.
				set(t, e, 1, "a-first", fmt.Sprint(f), NoSync)
				set(t, e, 1, "z-last", fmt.Sprint(f), NoSync)
				require.NoError(t, tbl.Flush())
			}
			require.NoError(t, e.WaitForCompactions())
			// UCS compacts sets of overlapping sstables (its trigger is the
			// maximum overlap, not the file count), so afterwards no key is in
			// threshold or more sstables of a level.
			threshold := (&TableOptions{ScalingParameters: []int{w}}).threshold(0)
			e.mu.Lock()
			overlap, _ := maxOverlap(slices.Clone(e.mu.tables[1].files))
			e.mu.Unlock()
			require.Less(t, overlap, threshold)
			require.Positive(t, e.Metrics().Compactions)
			for i := 0; i < 700; i++ {
				_, ok := getVal(t, tbl, fmt.Sprintf("k%04d", i))
				require.True(t, ok)
			}
			// The last flush that wrote a key wins: k0699 was last written
			// by flush 13 (i = 699 - 650 + 700), and k0000 by flush 28.
			mustGet(t, tbl, "k0699", "27")
			mustGet(t, tbl, "k0000", "28")
		})
	}
}

func scanAll(t *testing.T, tbl *Table, lower, upper []byte) map[string]string {
	t.Helper()
	it, err := tbl.NewIter(lower, upper)
	require.NoError(t, err)
	defer it.Close()
	out := map[string]string{}
	var prev []byte
	for ok := it.First(); ok; ok = it.Next() {
		require.True(t, prev == nil || bytes.Compare(prev, it.Key()) < 0, "keys out of order")
		prev = append(prev[:0], it.Key()...)
		r, err := row.Decode(it.Value())
		require.NoError(t, err)
		for _, c := range r.Live(0) {
			out[string(it.Key())] = string(c.Value)
		}
	}
	require.NoError(t, it.Error())
	return out
}

// TestIterator checks scans, bounds and SeekGE against a model, across
// memtables and compacted sstables.
func TestIterator(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 64 << 10})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	model := map[string]string{}
	rng := rand.New(rand.NewPCG(5, 6))
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("k%04d", rng.IntN(1000))
		if rng.IntN(6) == 0 {
			del(t, e, 1, key)
			delete(model, key)
		} else {
			set(t, e, 1, key, fmt.Sprint(i), NoSync)
			model[key] = fmt.Sprint(i)
		}
		if i%1000 == 999 {
			require.NoError(t, tbl.Flush())
		}
	}
	require.Equal(t, model, scanAll(t, tbl, nil, nil))
	sub := map[string]string{}
	for k, v := range model {
		if k >= "k0200" && k < "k0300" {
			sub[k] = v
		}
	}
	require.Equal(t, sub, scanAll(t, tbl, []byte("k0200"), []byte("k0300")))

	it, err := tbl.NewIter(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	keys := make([]string, 0, len(model))
	for k := range model {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, target := range []string{"k0000", "k0500", "k0999", "k1"} {
		i, _ := slices.BinarySearch(keys, target)
		for it.SeekGE([]byte(target)); it.Valid(); it.Next() {
			if r, _ := row.Decode(it.Value()); len(r.Live(0)) > 0 {
				break
			}
		}
		if i == len(keys) {
			require.False(t, it.Valid())
		} else {
			require.True(t, it.Valid())
			require.Equal(t, keys[i], string(it.Key()))
		}
	}
}

// TestReadsDuringCompaction runs point reads and scans while compactions
// replace and delete sstables, under the race detector: readers hold
// references, so files stay open until they finish.
func TestReadsDuringCompaction(t *testing.T) {
	e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 32 << 10})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	for i := 0; i < 500; i++ {
		set(t, e, 1, fmt.Sprintf("k%04d", i), "0", NoSync)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 2))
			for {
				select {
				case <-done:
					return
				default:
				}
				if r%2 == 0 {
					if _, err := tbl.Get([]byte(fmt.Sprintf("k%04d", rng.IntN(500)))); err != nil {
						t.Error(err)
						return
					}
					continue
				}
				it, err := tbl.NewIter(nil, nil)
				if err != nil {
					t.Error(err)
					return
				}
				n := 0
				for ok := it.First(); ok; ok = it.Next() {
					n++
				}
				if it.Error() != nil || n != 500 {
					t.Errorf("scan saw %d rows, err %v", n, it.Error())
				}
				it.Close()
			}
		}(r)
	}
	for i := 0; i < 20000; i++ {
		set(t, e, 1, fmt.Sprintf("k%04d", i%500), fmt.Sprint(i), NoSync)
	}
	require.NoError(t, e.WaitForCompactions())
	close(done)
	wg.Wait()
	require.Positive(t, e.Metrics().Compactions)
}

// TestModel is a randomized model test in the style of Pebble's metamorphic
// tests: random row fragments with out-of-order timestamps, cell tombstones
// and row deletions, interleaved with flushes, compactions, reopens and
// crashes. Every row read back must equal, byte for byte, the merge of all
// fragments the model knows were written (and, after a crash, synced).
//
// LSM_MODEL_SEEDS=n runs n seeds instead of 10.
func TestModel(t *testing.T) {
	seeds := uint64(10)
	if v, err := strconv.ParseUint(os.Getenv("LSM_MODEL_SEEDS"), 10, 64); err == nil && v > 0 {
		seeds = v
	}
	for seed := uint64(0); seed < seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runModel(t, seed) })
	}
}

func runModel(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 7))
	fs := vfs.NewCrashableMem()
	opts := func(fs vfs.FS) *Options {
		return &Options{
			FS:             fs,
			MemTableBudget: int64(16<<10 + rng.IntN(64<<10)),
			WALSegmentSize: 8 << 10,
			WALSyncPeriod:  -1,
			TableOptions: func(TableID) TableOptions {
				return TableOptions{ScalingParameters: []int{[]int{-2, 0, 2, 4}[seed%4]}}
			},
		}
	}
	e, err := Open("db", opts(fs))
	require.NoError(t, err)
	_, _ = e.CreateTable(1)

	type frag struct {
		key string
		v   []byte
	}
	var written, synced []frag
	merged := func(frags []frag) map[string][]byte {
		m := map[string][]byte{}
		for _, f := range frags {
			if prev, ok := m[f.key]; ok {
				v, err := row.Merge(nil, prev, f.v)
				require.NoError(t, err)
				m[f.key] = v
			} else {
				m[f.key] = f.v
			}
		}
		return m
	}
	check := func(e *Engine, frags []frag) {
		want := merged(frags)
		tbl, err := e.Table(1)
		require.NoError(t, err)
		got := map[string][]byte{}
		it, err := tbl.NewIter(nil, nil)
		require.NoError(t, err)
		for ok := it.First(); ok; ok = it.Next() {
			got[string(it.Key())] = append([]byte(nil), it.Value()...)
		}
		require.NoError(t, it.Error())
		require.NoError(t, it.Close())
		require.Equal(t, len(want), len(got))
		for k, v := range want {
			require.True(t, bytes.Equal(v, got[k]), "scan: key %s", k)
			g, err := tbl.Get([]byte(k))
			require.NoError(t, err)
			require.True(t, bytes.Equal(v, g), "get: key %s", k)
		}
	}
	randomFrag := func() []byte {
		r := row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}}
		if rng.IntN(15) == 0 {
			r.Deletion = row.Deletion{MarkedForDeleteAt: int64(rng.IntN(100)), LocalDeletionTime: int64(rng.IntN(5))}
		}
		for col := uint32(0); col < 4; col++ {
			if rng.IntN(2) == 0 {
				continue
			}
			ts := int64(rng.IntN(100)) // out of order on purpose
			if rng.IntN(5) == 0 {
				r.Cells = append(r.Cells, row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: int64(rng.IntN(5))})
			} else {
				r.Cells = append(r.Cells, row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime,
					Value: []byte(fmt.Sprint(rng.IntN(1000)))})
			}
		}
		return row.Append(nil, &r)
	}

	for op := 0; op < 3000; op++ {
		switch p := rng.IntN(100); {
		case p < 85:
			f := frag{fmt.Sprintf("k%03d", rng.IntN(300)), randomFrag()}
			sync := rng.IntN(20) == 0
			wo := NoSync
			if sync {
				wo = Sync
			}
			require.NoError(t, e.Put(1, []byte(f.key), f.v, wo))
			written = append(written, f)
			if sync {
				synced = slices.Clone(written)
			}
		case p < 92:
			tbl, _ := e.Table(1)
			require.NoError(t, tbl.Flush())
		case p < 96:
			require.NoError(t, e.WaitForCompactions())
			check(e, written)
		case p < 98:
			require.NoError(t, e.Close())
			e, err = Open("db", opts(fs))
			require.NoError(t, err)
			synced = slices.Clone(written) // Close syncs the WAL
			check(e, written)
		default:
			crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
			require.NoError(t, e.Close())
			fs = crashed
			e, err = Open("db", opts(fs))
			require.NoError(t, err)
			// What survives is a prefix of the writes: everything synced, plus
			// whatever flushes and WAL segment rotations made durable. Write i
			// has sequence number i+1, so the recovered last sequence number
			// is the prefix's length.
			k := int(e.lastSeq)
			require.GreaterOrEqual(t, k, len(synced), "synced writes lost")
			require.LessOrEqual(t, k, len(written))
			written = written[:k]
			synced = slices.Clone(written) // recovery flushes everything
			check(e, written)
		}
	}
	check(e, written)
	require.NoError(t, e.Close())
}

func makeKey(s string) pebble.InternalKey { return pebble.InternalKey{UserKey: []byte(s)} }
