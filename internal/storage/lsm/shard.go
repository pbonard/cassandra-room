package lsm

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"sort"
)

// Keys begin with the partition's token: 8 bytes, big-endian, ordered as
// the token ring is (the schema layer maps Murmur3's signed tokens by
// flipping the sign bit). Then come the partition key and clustering key.
// Sharding cuts the token space at fixed points, so all keys of a partition
// (indeed of a token) fall in one shard, and a partition is never split
// across the sstables of a shard set. Keys shorter than 8 bytes are treated
// as padded with zeros.
const tokenLen = 8

// keyToken returns the token of key.
func keyToken(key []byte) uint64 {
	var b [tokenLen]byte
	copy(b[:], key)
	return binary.BigEndian.Uint64(b[:])
}

// tokenKey returns the smallest key with token tok: a shard boundary.
func tokenKey(tok uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, tok)
}

// minTokenCoverage is ShardManager.MINIMUM_TOKEN_COVERAGE: a smaller span
// comes from a single partition (or a test's narrow keys), for which
// density and sharding make no sense, so it counts as the whole space.
const minTokenCoverage = 1.0 / (1 << 48)

// tokenSpan returns the fraction of the token space covered by keys from
// lo to hi, inclusive (ShardManager.rangeSpanned).
func tokenSpan(lo, hi []byte) float64 {
	span := (float64(keyToken(hi)-keyToken(lo)) + 1) / 0x1p64
	if span >= minTokenCoverage {
		return span
	}
	return 1
}

// density is an sstable's size divided by the fraction of the token space
// it covers (UCS's density, ShardManager.density).
func density(f *fileMeta) float64 {
	return float64(f.size) / tokenSpan(f.smallest.UserKey, f.largest.UserKey)
}

// combinedDensity is ShardManager.calculateCombinedDensity: the total size
// of files divided by the span from the smallest key to the largest.
func combinedDensity(files []*fileMeta) float64 {
	if len(files) == 0 {
		return 0
	}
	var size uint64
	lo, hi := files[0].smallest.UserKey, files[0].largest.UserKey
	for _, f := range files {
		size += f.size
		if bytes.Compare(f.smallest.UserKey, lo) < 0 {
			lo = f.smallest.UserKey
		}
		if bytes.Compare(f.largest.UserKey, hi) > 0 {
			hi = f.largest.UserKey
		}
	}
	return float64(size) / tokenSpan(lo, hi)
}

// maxShardShift is Controller.MAX_SHARD_SHIFT.
const maxShardShift = 20

// numShards follows Controller.getNumShards: the number of equal shards of
// the token space for output of the given density.
func (o *TableOptions) numShards(density float64) int {
	base := o.BaseShardCount
	if o.MinSSTableSize > 0 {
		count := density / float64(o.MinSSTableSize)
		if !(count >= float64(base)) { // also true for NaN
			// A power of two, rounded down, that divides the base count, so
			// boundaries match those of higher levels.
			c := 1
			if count >= 1 {
				c = 1 << (bits.Len64(uint64(count)) - 1)
			}
			return min(c, base&-base)
		}
	}
	growth := o.sstableGrowth()
	if growth == 1 {
		return base
	}
	count := density / (float64(o.TargetSSTableSize) * float64(base))
	pow := math.Log2(count)*(1-growth) + 0.5
	switch {
	case pow >= maxShardShift:
		return base << maxShardShift
	case pow >= 0:
		return base << int(pow)
	default:
		return base // also NaN
	}
}

// shardBoundaries returns the n-1 keys that cut the token space into n
// equal shards.
func shardBoundaries(n int) [][]byte {
	out := make([][]byte, 0, max(n-1, 0))
	for i := 1; i < n; i++ {
		// i·2⁶⁴/n without overflow.
		hi, lo := bits.Mul64(uint64(i), math.MaxUint64)
		q, _ := bits.Div64(hi, lo, uint64(n))
		out = append(out, tokenKey(q+1))
	}
	return out
}

// ownership is the part of the token space this node owns (its local
// ranges), over which densities and shards are measured, as
// ShardManagerNoDisks does: a node owning an eighth of the ring in one
// contiguous range would otherwise see its flushes 8 times denser than
// they are, place them levels too high, and put all its data into one of
// the shards of the whole space. nil (or nothing owned) measures the whole
// space.
type ownership struct {
	ranges []TokenRange // sorted, disjoint
	cum    []float64    // tokens owned before each range
	total  float64
}

// newOwnership builds the ownership of ranges (in any order, possibly
// overlapping).
func newOwnership(ranges []TokenRange) *ownership {
	rs := slices.Clone(ranges)
	slices.SortFunc(rs, func(a, b TokenRange) int { return cmp.Compare(a.First, b.First) })
	var merged []TokenRange
	for _, r := range rs {
		if r.Last < r.First {
			continue
		}
		if n := len(merged); n > 0 && (r.First <= merged[n-1].Last || merged[n-1].Last != math.MaxUint64 && r.First == merged[n-1].Last+1) {
			merged[n-1].Last = max(merged[n-1].Last, r.Last)
			continue
		}
		merged = append(merged, r)
	}
	o := &ownership{ranges: merged}
	for _, r := range merged {
		o.cum = append(o.cum, o.total)
		o.total += float64(r.Last-r.First) + 1
	}
	if o.total/0x1p64 < minTokenCoverage {
		return nil
	}
	return o
}

// owned returns the tokens owned in [lo, hi].
func (o *ownership) owned(lo, hi uint64) float64 {
	var n float64
	for _, r := range o.ranges {
		a, b := max(r.First, lo), min(r.Last, hi)
		if a <= b {
			n += float64(b-a) + 1
		}
	}
	return n
}

// span is tokenSpan over the owned space.
func (o *ownership) span(lo, hi []byte) float64 {
	if o == nil {
		return tokenSpan(lo, hi)
	}
	span := o.owned(keyToken(lo), keyToken(hi)) / o.total
	if span >= minTokenCoverage {
		return span
	}
	return 1
}

func (o *ownership) density(f *fileMeta) float64 {
	return float64(f.size) / o.span(f.smallest.UserKey, f.largest.UserKey)
}

func (o *ownership) combinedDensity(files []*fileMeta) float64 {
	if len(files) == 0 {
		return 0
	}
	var size uint64
	lo, hi := files[0].smallest.UserKey, files[0].largest.UserKey
	for _, f := range files {
		size += f.size
		if bytes.Compare(f.smallest.UserKey, lo) < 0 {
			lo = f.smallest.UserKey
		}
		if bytes.Compare(f.largest.UserKey, hi) > 0 {
			hi = f.largest.UserKey
		}
	}
	return float64(size) / o.span(lo, hi)
}

// boundaries cuts the owned space into n equal shards (shardBoundaries
// over the whole space when nil).
func (o *ownership) boundaries(n int) [][]byte {
	if o == nil {
		return shardBoundaries(n)
	}
	out := make([][]byte, 0, max(n-1, 0))
	for i := 1; i < n; i++ {
		target := o.total * float64(i) / float64(n)
		j := sort.Search(len(o.cum), func(j int) bool { return o.cum[j] > target }) - 1
		r := o.ranges[j]
		off := uint64(target - o.cum[j])
		if off > r.Last-r.First {
			off = r.Last - r.First
		}
		b := tokenKey(r.First + off)
		if len(out) == 0 || bytes.Compare(out[len(out)-1], b) < 0 {
			out = append(out, b)
		}
	}
	return out
}

// SetOwnedRanges sets the token ranges this node owns of the table (lsm
// token space: sign bit flipped), for measuring densities and shards over
// them. Empty means the whole space.
func (t *Table) SetOwnedRanges(ranges []TokenRange) {
	t.own.Store(newOwnership(ranges))
}
