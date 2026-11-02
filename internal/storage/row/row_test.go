package row

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func live(col uint32, ts int64, v string) Cell {
	return Cell{Column: col, Timestamp: ts, LocalDeletionTime: NoDeletionTime, Value: []byte(v)}
}

func tombstone(col uint32, ts, ldt int64) Cell {
	return Cell{Column: col, Timestamp: ts, LocalDeletionTime: ldt}
}

func expiring(col uint32, ts, expiresAt int64, ttl uint32, v string) Cell {
	return Cell{Column: col, Timestamp: ts, LocalDeletionTime: expiresAt, TTL: ttl, Value: []byte(v)}
}

func enc(r Row) []byte {
	if r.Deletion == (Deletion{}) {
		r.Deletion.MarkedForDeleteAt = NoTimestamp
	}
	return Append(nil, &r)
}

func merge(t *testing.T, a, b []byte) *Row {
	t.Helper()
	m, err := Merge(nil, a, b)
	require.NoError(t, err)
	r, err := Decode(m)
	require.NoError(t, err)
	return r
}

// TestReconcile checks Cassandra's Cells.resolveRegular rules.
func TestReconcile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		a, b  Cell
		wantA bool
	}{
		{"higher timestamp wins", live(1, 10, "a"), live(1, 9, "b"), true},
		{"tombstone beats live at same timestamp", tombstone(1, 10, 100), live(1, 10, "z"), true},
		{"expiring beats live at same timestamp", expiring(1, 10, 200, 60, "a"), live(1, 10, "z"), true},
		{"tombstone beats expiring at same timestamp", tombstone(1, 10, 100), expiring(1, 10, 200, 60, "a"), true},
		{"later local deletion time wins", tombstone(1, 10, 101), tombstone(1, 10, 100), true},
		{"lower TTL wins for equal expiry", expiring(1, 10, 200, 30, "a"), expiring(1, 10, 200, 60, "a"), true},
		{"greater value wins otherwise", live(1, 10, "b"), live(1, 10, "a"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, swap := range []bool{false, true} {
				a, b := tc.a, tc.b
				if swap {
					a, b = b, a
				}
				got := reconcile(&a, &b)
				want := tc.a
				require.Equal(t, want, *got)
			}
		})
	}
}

func TestMergeRowDeletion(t *testing.T) {
	a := enc(Row{Cells: []Cell{live(1, 5, "old"), live(2, 20, "new")}})
	b := enc(Row{Deletion: Deletion{MarkedForDeleteAt: 10, LocalDeletionTime: 1000}})
	r := merge(t, a, b)
	require.Equal(t, int64(10), r.Deletion.MarkedForDeleteAt)
	require.Equal(t, []Cell{live(2, 20, "new")}, r.Cells, "cells at or below the deletion are shadowed")

	// The superseding deletion wins (DeletionTime.supersedes).
	c := enc(Row{Deletion: Deletion{MarkedForDeleteAt: 10, LocalDeletionTime: 2000}})
	require.Equal(t, int64(2000), merge(t, b, c).Deletion.LocalDeletionTime)
}

func TestLiveAndTimestamps(t *testing.T) {
	r := Row{Deletion: Deletion{MarkedForDeleteAt: NoTimestamp}, Cells: []Cell{
		live(1, 3, "a"), tombstone(2, 7, 50), expiring(3, 5, 100, 10, "c"),
	}}
	require.Equal(t, []Cell{live(1, 3, "a"), expiring(3, 5, 100, 10, "c")}, r.Live(99))
	require.Equal(t, []Cell{live(1, 3, "a")}, r.Live(100))
	minTS, maxTS, ok, err := Timestamps(Append(nil, &r))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(3), minTS)
	require.Equal(t, int64(7), maxTS)
}

func randomRow(rng *rand.Rand) []byte {
	r := Row{Deletion: Deletion{MarkedForDeleteAt: NoTimestamp}}
	if rng.IntN(5) == 0 {
		r.Deletion = Deletion{MarkedForDeleteAt: int64(rng.IntN(20)), LocalDeletionTime: int64(rng.IntN(3))}
	}
	for col := uint32(0); col < 6; col++ {
		if rng.IntN(2) == 0 {
			continue
		}
		ts := int64(rng.IntN(20))
		switch rng.IntN(4) {
		case 0:
			r.Cells = append(r.Cells, tombstone(col, ts, int64(rng.IntN(3))))
		case 1:
			r.Cells = append(r.Cells, expiring(col, ts, int64(rng.IntN(3)), uint32(1+rng.IntN(2)), string(rune('a'+rng.IntN(3)))))
		default:
			r.Cells = append(r.Cells, live(col, ts, string(rune('a'+rng.IntN(3)))))
		}
	}
	return Append(nil, &r)
}

func mustMerge(t *testing.T, a, b []byte) []byte {
	m, err := Merge(nil, a, b)
	require.NoError(t, err)
	return m
}

// TestMergeAlgebra checks that Merge is commutative and associative, which
// lets the engine combine row fragments in any order.
func TestMergeAlgebra(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		a, b, c := randomRow(rng), randomRow(rng), randomRow(rng)
		require.True(t, bytes.Equal(mustMerge(t, a, b), mustMerge(t, b, a)), "commutative")
		l := mustMerge(t, mustMerge(t, a, b), c)
		r := mustMerge(t, a, mustMerge(t, b, c))
		require.True(t, bytes.Equal(l, r), "associative")
		require.True(t, bytes.Equal(mustMerge(t, l, l), l), "idempotent")
	}
}

func TestCorrupt(t *testing.T) {
	_, err := Merge(nil, []byte{9}, enc(Row{}))
	require.ErrorIs(t, err, ErrCorrupt)
	_, err = Decode([]byte{version, 0, 1, 1})
	require.ErrorIs(t, err, ErrCorrupt)
}

// TestCanonical checks that a fragment's own row deletion shadows its older
// cells, in its encoding and in Live, so a single fragment and its merge
// with itself are identical.
func TestCanonical(t *testing.T) {
	r := Row{Deletion: Deletion{MarkedForDeleteAt: 10, LocalDeletionTime: 1},
		Cells: []Cell{live(1, 5, "old"), live(2, 20, "new")}}
	enc := Append(nil, &r)
	d, err := Decode(enc)
	require.NoError(t, err)
	require.Equal(t, []Cell{live(2, 20, "new")}, d.Cells)
	require.True(t, bytes.Equal(enc, mustMerge(t, enc, enc)))
	require.Equal(t, []Cell{live(2, 20, "new")}, r.Live(0))
}

// TestPurgeInvisible checks the purge rule on random rows: purging a
// compaction's merged row a, against data b stored elsewhere (maxPurgeable
// is b's smallest timestamp), never changes the cells reads see once a and b
// are merged, at any time from now on.
func TestPurgeInvisible(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	visible := func(v []byte, now int64) []Cell {
		if v == nil {
			return nil
		}
		r, err := Decode(v)
		require.NoError(t, err)
		return r.Live(now)
	}
	purged := 0
	for i := 0; i < 50000; i++ {
		a, b := randomRow(rng), randomRow(rng)
		hasB := rng.IntN(3) != 0
		now := int64(rng.IntN(6))
		gcBefore := now - int64(rng.IntN(3))
		limit := int64(math.MaxInt64)
		if hasB {
			lo, _, ok, err := Timestamps(b)
			require.NoError(t, err)
			if ok {
				limit = lo
			}
		}
		calls := 0
		p, changed, empty, err := Purge(nil, a, now, gcBefore, func(minTS, maxTS int64) (int64, error) {
			calls++
			// Exercise the allowed shortcut: nothing purgeable.
			if limit <= minTS && rng.IntN(2) == 0 {
				return math.MinInt64, nil
			}
			return limit, nil
		})
		require.NoError(t, err)
		require.LessOrEqual(t, calls, 1)
		if !changed {
			require.True(t, bytes.Equal(p, a))
		} else {
			purged++
		}
		if empty {
			p = nil
		}
		for _, at := range []int64{now, now + 1, now + 10} {
			want, got := visible(a, at), visible(p, at)
			if hasB {
				want = visible(mustMerge(t, a, b), at)
				if p == nil {
					got = visible(b, at)
				} else {
					got = visible(mustMerge(t, p, b), at)
				}
			}
			require.Equal(t, want, got, "row %d at %d", i, at)
		}
		s, err := Summarize(a, nil)
		require.NoError(t, err)
		if changed {
			require.Less(t, s.MinLDT, gcBefore, "purged before the earliest deletion time")
		}
	}
	require.Greater(t, purged, 1000)
}

// TestPurgeAndSummarize checks that the stats PurgeAndSummarize returns are
// Summarize's of its output, with the same reported deletion times.
func TestPurgeAndSummarize(t *testing.T) {
	rng := rand.New(rand.NewPCG(8, 9))
	for i := 0; i < 20000; i++ {
		a := randomRow(rng)
		now := int64(rng.IntN(6))
		limit := int64(rng.IntN(25))
		var got []int64
		out, _, empty, st, err := PurgeAndSummarize(nil, a, now, now-int64(rng.IntN(3)),
			func(int64, int64) (int64, error) { return limit, nil }, func(t int64) { got = append(got, t) })
		require.NoError(t, err)
		if empty {
			require.Zero(t, st.Cells)
			require.Empty(t, got)
			continue
		}
		var want []int64
		ws, err := Summarize(out, func(t int64) { want = append(want, t) })
		require.NoError(t, err)
		require.Equal(t, ws, st)
		require.Equal(t, want, got)
	}
}

// TestSplitByTimestamp checks that a row's parts land in the buckets of
// their timestamps and merge back to the row, byte for byte.
func TestSplitByTimestamp(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	bucket := func(ts int64) int64 {
		if ts == math.MinInt64 {
			return math.MinInt64
		}
		return ts / 5
	}
	split := 0
	for i := 0; i < 20000; i++ {
		v := randomRow(rng)
		parts, err := SplitByTimestamp(v, bucket)
		require.NoError(t, err)
		require.NotEmpty(t, parts)
		if len(parts) > 1 {
			split++
		}
		merged := parts[0].Value
		for _, p := range parts {
			lo, hi, ok, err := Timestamps(p.Value)
			require.NoError(t, err)
			if ok {
				require.Equal(t, p.Bucket, bucket(lo))
				require.Equal(t, p.Bucket, bucket(hi))
			}
		}
		for _, p := range parts[1:] {
			merged = mustMerge(t, merged, p.Value)
		}
		require.True(t, bytes.Equal(v, merged), "row %d", i)
	}
	require.Greater(t, split, 5000)
}
