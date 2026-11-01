package lsm

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
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
