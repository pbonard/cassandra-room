package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cespare/xxhash/v2"
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
	// Odd seeds use keys spread over the token space, with small shards.
	sharded := seed%2 == 1
	modelKey := func(i int) string {
		k := fmt.Sprintf("k%03d", i)
		if sharded {
			return string(tokenKey(xxhash.Sum64String(k))) + k
		}
		return k
	}
	// A controlled clock. Seeds with purge (seed%3 != 0) use a short
	// gc_grace_seconds, so deletions and expired cells are purged, expired
	// sstables dropped and sstables compacted for their tombstones; the
	// others keep the default 10 days, so nothing is purged and rows must
	// match the model byte for byte.
	var clock atomic.Int64
	clock.Store(1000)
	purge := seed%3 != 0
	fs := vfs.NewCrashableMem()
	opts := func(fs vfs.FS) *Options {
		return &Options{
			Now:            func() time.Time { return time.Unix(clock.Load(), 0) },
			FS:             fs,
			MemTableBudget: int64(16<<10 + rng.IntN(64<<10)),
			WALSegmentSize: 8 << 10,
			WALSyncPeriod:  -1,
			TableOptions: func(TableID) TableOptions {
				o := TableOptions{ScalingParameters: []int{[]int{-2, 0, 2, 4}[seed%4]}}
				if sharded {
					// Small targets, so flushes and compactions split.
					o.BaseShardCount = []int{1, 2, 4}[seed%3]
					o.TargetSSTableSize = 4 << 10
					o.MinSSTableSize = -1
					o.SSTableGrowth = []float64{-1, 0.333, 0.5}[seed%3]
					o.SerialOutputShards = seed%4 == 3
				}
				if seed%5 == 4 {
					o.TimeWindow = 20 * time.Microsecond // timestamps are 0..99
					o.TimeWindowCassandra = seed%10 == 9
				}
				// Incremental release with small fragments, or none.
				o.FragmentSize = []int64{-1, 1 << 10, 4 << 10}[(seed/2)%3]
				if seed%7 == 0 {
					o.SpaceAmplificationGoal = 1.5
				}
				if purge {
					o.GCGraceSeconds = []int64{-1, 5}[seed%2]
					o.TombstoneCompactionInterval = 2 * time.Second
					o.ExpiredCheckInterval = -1
				}
				return o
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
	// checkReader compares a table or a snapshot with the model's rows as of
	// time now.
	checkReader := func(tbl modelReader, want map[string][]byte, now int64) {
		got := map[string][]byte{}
		it, err := tbl.NewIter(nil, nil)
		require.NoError(t, err)
		for ok := it.First(); ok; ok = it.Next() {
			got[string(it.Key())] = append([]byte(nil), it.Value()...)
		}
		require.NoError(t, it.Error())
		require.NoError(t, it.Close())
		if purge {
			// Compare what reads see now: purging changes the bytes but
			// never the live cells.
			visible := func(v []byte) []row.Cell {
				if v == nil {
					return nil
				}
				r, err := row.Decode(v)
				require.NoError(t, err)
				return r.Live(now)
			}
			for k := range got {
				if _, ok := want[k]; !ok {
					require.Empty(t, visible(got[k]), "scan: key %x not in the model", k)
				}
			}
			for k, v := range want {
				require.Equal(t, visible(v), visible(got[k]), "scan: key %x", k)
				g, err := tbl.Get([]byte(k))
				if errors.Is(err, ErrNotFound) {
					g, err = nil, nil
				}
				require.NoError(t, err)
				require.Equal(t, visible(v), visible(g), "get: key %x", k)
			}
			return
		}
		// Without purging, rows match byte for byte. Empty rows (no cells,
		// no deletion) carry nothing and may be dropped: an sstable of them
		// counts as fully expired.
		empty := func(v []byte) bool {
			r, err := row.Decode(v)
			require.NoError(t, err)
			return len(r.Cells) == 0 && r.Deletion.IsLive()
		}
		for k, v := range got {
			if _, ok := want[k]; !ok {
				require.True(t, empty(v), "scan: key %s not in the model", k)
			}
		}
		for k, v := range want {
			g, err := tbl.Get([]byte(k))
			if empty(v) {
				require.True(t, errors.Is(err, ErrNotFound) || (err == nil && bytes.Equal(v, g)), "get: key %s", k)
				if gv, ok := got[k]; ok {
					require.True(t, bytes.Equal(v, gv), "scan: key %s", k)
				}
				continue
			}
			require.True(t, bytes.Equal(v, got[k]), "scan: key %s", k)
			require.NoError(t, err)
			require.True(t, bytes.Equal(v, g), "get: key %s", k)
		}
	}
	check := func(e *Engine, frags []frag) {
		tbl, err := e.Table(1)
		require.NoError(t, err)
		checkReader(tbl, merged(frags), clock.Load())
	}
	// A read snapshot held across operations, with the model's rows and the
	// time when it was taken.
	var snap *Snapshot
	var snapWant map[string][]byte
	var snapNow int64
	closeSnap := func() {
		if snap != nil {
			checkReader(snap, snapWant, snapNow)
			require.NoError(t, snap.Close())
			snap = nil
		}
	}
	scratch := vfs.NewMem()
	exports := 0
	// gc_grace_seconds is Cassandra's bound on how late a write may arrive
	// relative to a deletion: once a deletion (or expired cell) is past it,
	// it may be purged, and a write arriving afterwards with an older
	// timestamp is not shadowed. So with purge, a new write's timestamps
	// exceed those of the key's deletions already past gc grace; they are
	// still out of order otherwise.
	grace := int64(864000)
	if purge {
		grace = max([]int64{-1, 5}[seed%2], 0)
	}
	tsFloor := func(key string) int64 {
		if !purge {
			return 0
		}
		now := clock.Load()
		floor := int64(0)
		for _, f := range written {
			if f.key != key {
				continue
			}
			r, err := row.Decode(f.v)
			require.NoError(t, err)
			if d := r.Deletion; !d.IsLive() && d.LocalDeletionTime < now-grace {
				floor = max(floor, d.MarkedForDeleteAt+1)
			}
			for _, c := range r.Cells {
				if c.LocalDeletionTime != row.NoDeletionTime && c.LocalDeletionTime < now-grace && c.LocalDeletionTime <= now {
					floor = max(floor, c.Timestamp+1)
				}
			}
		}
		return floor
	}
	randomFrag := func(key string) []byte {
		now := clock.Load()
		floor := tsFloor(key)
		r := row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}}
		if rng.IntN(15) == 0 {
			r.Deletion = row.Deletion{MarkedForDeleteAt: floor + int64(rng.IntN(100)), LocalDeletionTime: now - int64(rng.IntN(5))}
		}
		for col := uint32(0); col < 4; col++ {
			if rng.IntN(2) == 0 {
				continue
			}
			ts := floor + int64(rng.IntN(100)) // out of order on purpose
			switch rng.IntN(10) {
			case 0, 1:
				r.Cells = append(r.Cells, row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: now - int64(rng.IntN(5))})
			case 2:
				// Expiring, some already expired.
				ttl := uint32(1 + rng.IntN(20))
				r.Cells = append(r.Cells, row.Cell{Column: col, Timestamp: ts, TTL: ttl,
					LocalDeletionTime: now - 5 + int64(rng.IntN(15)), Value: []byte(fmt.Sprint(rng.IntN(1000)))})
			default:
				r.Cells = append(r.Cells, row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime,
					Value: []byte(fmt.Sprint(rng.IntN(1000)))})
			}
		}
		return row.Append(nil, &r)
	}

	for op := 0; op < 3000; op++ {
		if rng.IntN(40) == 0 {
			clock.Add(int64(1 + rng.IntN(3)))
		}
		switch {
		case snap == nil && rng.IntN(150) == 0:
			tbl, err := e.Table(1)
			require.NoError(t, err)
			snap, err = tbl.NewSnapshot()
			require.NoError(t, err)
			snapWant, snapNow = merged(written), clock.Load()
		case snap != nil && rng.IntN(60) == 0:
			closeSnap()
		case rng.IntN(150) == 0:
			// Streaming a range out and back in changes nothing: the rows
			// reconcile with themselves. Imported sstables are durable at
			// once, so the sender's writes are made durable first (Cassandra
			// flushes before streaming); otherwise a crash could keep
			// exported writes and lose earlier ones.
			require.NoError(t, e.SyncWAL())
			synced = slices.Clone(written)
			tbl, err := e.Table(1)
			require.NoError(t, err)
			a, b := []byte(modelKey(rng.IntN(300))), []byte(modelKey(rng.IntN(300)))
			if bytes.Compare(a, b) > 0 {
				a, b = b, a
			}
			exports++
			dir := fmt.Sprintf("x%d", exports)
			require.NoError(t, scratch.MkdirAll(dir, 0755))
			paths, err := tbl.ExportRange(a, b, scratch, dir)
			require.NoError(t, err)
			require.NoError(t, tbl.Import(scratch, paths))
		}
		switch p := rng.IntN(100); {
		case p < 85:
			key := modelKey(rng.IntN(300))
			f := frag{key, randomFrag(key)}
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
			closeSnap()
			require.NoError(t, e.Close())
			e, err = Open("db", opts(fs))
			require.NoError(t, err)
			synced = slices.Clone(written) // Close syncs the WAL
			check(e, written)
		default:
			closeSnap()
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
	closeSnap()
	check(e, written)
	m := e.Metrics()
	t.Logf("compactions %d, tombstone compactions %d, expired sstables dropped %d, early released inputs %d",
		m.Compactions, m.TombstoneCompactions, m.ExpiredSSTablesDropped, m.EarlyReleasedInputs)
	require.NoError(t, e.Close())
}

func makeKey(s string) pebble.InternalKey { return pebble.InternalKey{UserKey: []byte(s)} }

// TestNumShards checks the examples in UnifiedCompactionStrategy.md.
func TestNumShards(t *testing.T) {
	const MiB = 1 << 20
	// Basic scheme: b=4, t=100MiB, λ=0, no minimum.
	o := TableOptions{BaseShardCount: 4, TargetSSTableSize: 100 * MiB, MinSSTableSize: -1, SSTableGrowth: -1}
	o.ensureDefaults()
	require.Equal(t, 4, o.numShards(200*MiB))   // "a 200 MiB memtable will be split in four"
	require.Equal(t, 16, o.numShards(1200*MiB)) // "rounded to 2^2 · 4 shards"
	require.Equal(t, 4, o.numShards(0))
	// Minimum size: m=100MiB, b=4.
	o.MinSSTableSize = 100 * MiB
	require.Equal(t, 1, o.numShards(50*MiB))
	require.Equal(t, 2, o.numShards(250*MiB))
	require.Equal(t, 4, o.numShards(450*MiB))
	// λ=1: always b shards above the minimum.
	o = TableOptions{BaseShardCount: 10, MinSSTableSize: 100 * MiB, SSTableGrowth: 1}
	o.ensureDefaults()
	require.Equal(t, 10, o.numShards(1<<40))
	require.Equal(t, 2, o.numShards(250*MiB)) // largest power of 2 dividing 10
	// λ=0.5: quadrupling the density doubles the shard count.
	o = TableOptions{BaseShardCount: 1, TargetSSTableSize: MiB, MinSSTableSize: -1, SSTableGrowth: 0.5}
	o.ensureDefaults()
	require.Equal(t, 2*o.numShards(64*MiB), o.numShards(256*MiB))
}

func TestShardBoundaries(t *testing.T) {
	require.Empty(t, shardBoundaries(1))
	b := shardBoundaries(4)
	require.Equal(t, [][]byte{tokenKey(1 << 62), tokenKey(1 << 63), tokenKey(3 << 62)}, b)
	// Boundaries of n shards are boundaries of 2n shards.
	b8 := shardBoundaries(8)
	for i, k := range b {
		require.Equal(t, k, b8[2*i+1])
	}
	// Spans and densities.
	require.Equal(t, 1.0, tokenSpan([]byte("abcdefg1"), []byte("abcdefg2"))) // too narrow: whole space
	require.Equal(t, 1.0/256, tokenSpan([]byte("a"), []byte("a\xff\xff\xff\xff\xff\xff\xff")))
	require.InDelta(t, 0.25, tokenSpan(tokenKey(0), tokenKey(1<<62-1)), 1e-12)
}

// TestOverlapSets checks the example in UnifiedCompactionStrategy.md: A, B,
// C and D cover 0-3, 2-7, 6-9 and 1-8; the overlap sets are ABD and BCD.
func TestOverlapSets(t *testing.T) {
	f := func(a, b string) *fileMeta { return &fileMeta{smallest: makeKey(a), largest: makeKey(b)} }
	A, B, C, D := f("0", "3"), f("2", "7"), f("6", "9"), f("1", "8")
	sets := overlapSets([]*fileMeta{A, B, C, D})
	require.Len(t, sets, 2)
	require.ElementsMatch(t, []*fileMeta{A, B, D}, sets[0])
	require.ElementsMatch(t, []*fileMeta{B, C, D}, sets[1])
	// The sets share files, so a transitive bucket holds all four.
	buckets := assignBuckets(3, sets)
	require.Len(t, buckets, 1)
	require.Len(t, buckets[0].files(), 4)
	require.Equal(t, 3, buckets[0].maxOverlap)
	// Below the threshold nothing is picked.
	require.Empty(t, assignBuckets(4, sets))
	// Disjoint groups form separate buckets.
	E, F := f("a", "b"), f("a", "c")
	buckets = assignBuckets(2, overlapSets([]*fileMeta{A, B, E, F}))
	require.Len(t, buckets, 2)
}

// TestPicksAreDisjoint checks that one table can run several compactions:
// a pick excludes files other compactions use, and picks never share files.
func TestPicksAreDisjoint(t *testing.T) {
	tbl := &Table{opts: TableOptions{ScalingParameters: []int{0}}}
	tbl.opts.ensureDefaults()
	f := func(a, b string, ts int64) *fileMeta {
		return &fileMeta{smallest: makeKey(a), largest: makeKey(b), size: 1000, maxTS: ts}
	}
	tbl.files = []*fileMeta{f("a", "c", 1), f("b", "d", 2), f("m", "o", 3), f("n", "p", 4)}
	p1 := tbl.pickCompactionLocked()
	require.Len(t, p1, 2)
	for _, x := range p1 {
		x.compacting = true
	}
	p2 := tbl.pickCompactionLocked()
	require.Len(t, p2, 2)
	for _, x := range p2 {
		require.False(t, slices.Contains(p1, x))
		x.compacting = true
	}
	require.Nil(t, tbl.pickCompactionLocked())
}

// TestShardedCompaction writes rows spread over the token space with small
// shard targets and checks that flushes and compactions split their output
// at shard boundaries (no sstable crosses a base shard, and every
// partition's rows stay in one sstable of a compaction's output), that
// compactions run, and that every row reads back.
func TestShardedCompaction(t *testing.T) {
	const base = 4
	e, err := Open("db", &Options{
		FS:                       vfs.NewMem(),
		MemTableBudget:           256 << 10,
		MaxConcurrentCompactions: 4,
		TableOptions: func(TableID) TableOptions {
			return TableOptions{BaseShardCount: base, TargetSSTableSize: 32 << 10, MinSSTableSize: -1}
		},
	})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	key := func(p, c int) string {
		pk := fmt.Sprintf("p%04d", p)
		return string(tokenKey(xxhash.Sum64String(pk))) + pk + fmt.Sprintf("/c%02d", c)
	}
	want := map[string]string{}
	for round := 0; round < 30; round++ {
		for i := 0; i < 400; i++ {
			p, c := (round*400+i)%2000, round%5
			k, v := key(p, c), fmt.Sprintf("v%d-%d", round, i)
			set(t, e, 1, k, v, NoSync)
			want[k] = v
		}
		require.NoError(t, tbl.Flush())
	}
	require.NoError(t, e.WaitForCompactions())
	require.Positive(t, e.Metrics().Compactions)

	bounds := shardBoundaries(base)
	shardOf := func(k []byte) int {
		i := 0
		for i < len(bounds) && bytes.Compare(k, bounds[i]) >= 0 {
			i++
		}
		return i
	}
	e.mu.Lock()
	files := slices.Clone(e.mu.tables[1].files)
	e.mu.Unlock()
	require.Greater(t, len(files), base)
	for _, f := range files {
		require.Equal(t, shardOf(f.smallest.UserKey), shardOf(f.largest.UserKey),
			"file %d crosses a shard boundary", f.num)
	}
	// Boundaries are whole tokens (8-byte keys), so a file within one shard
	// never splits a partition with its neighbour in another shard.
	for _, b := range bounds {
		require.Len(t, b, tokenLen)
	}
	got := scanAll(t, tbl, nil, nil)
	require.Equal(t, want, got)
	for k, v := range want {
		mustGet(t, tbl, k, v)
	}
}

// TestTimeWindowCompaction writes rows into three time windows, several
// flushes each, and checks that every older window ends as one sstable and
// that no compaction mixes windows.
func TestTimeWindowCompaction(t *testing.T) {
	window := time.Hour
	e, err := Open("db", &Options{
		FS: vfs.NewMem(),
		TableOptions: func(TableID) TableOptions {
			return TableOptions{TimeWindow: window}
		},
	})
	require.NoError(t, err)
	defer e.Close()
	tbl, _ := e.CreateTable(1)
	hour := window.Microseconds()
	want := map[string]string{}
	for w := int64(0); w < 3; w++ {
		for f := 0; f < 6; f++ {
			for i := 0; i < 20; i++ {
				// Each flush of a window rewrites the window's keys, so its
				// sstables overlap.
				k := fmt.Sprintf("w%d-%02d", w, i)
				val := fmt.Sprintf("f%d", f)
				ts := w*hour + int64(f*100+i)
				v := row.Append(nil, &row.Row{
					Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
					Cells:    []row.Cell{{Column: 1, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: []byte(val)}},
				})
				require.NoError(t, e.Put(1, []byte(k), v, NoSync))
				want[k] = val
			}
			require.NoError(t, tbl.Flush())
		}
		require.NoError(t, e.WaitForCompactions())
	}
	require.NoError(t, e.WaitForCompactions())
	perWindow := map[int64]int{}
	e.mu.Lock()
	for _, f := range e.mu.tables[1].files {
		require.Equal(t, f.minTS/hour, f.maxTS/hour, "file %d mixes windows", f.num)
		perWindow[f.maxTS/hour]++
	}
	e.mu.Unlock()
	require.Equal(t, 1, perWindow[0])
	require.Equal(t, 1, perWindow[1])
	// The newest window follows UCS (T4): 6 flushes leave fewer than 4.
	require.Less(t, perWindow[2], 4)
	require.Equal(t, want, scanAll(t, tbl, nil, nil))
}

// modelReader is what the model test reads: a table or a snapshot.
type modelReader interface {
	Get(key []byte) ([]byte, error)
	NewIter(lower, upper []byte) (*Iterator, error)
}
