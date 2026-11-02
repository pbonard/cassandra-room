package db

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"

	"github.com/cassandra-room/cassandra-room/internal/types"
)

// Storage keys. Rows of a table are stored in token order, as Cassandra's
// Murmur3Partitioner orders partitions, with each key in byte-comparable
// form (types.AppendComponent):
//
//	partition prefix P = token (8 bytes, sign bit flipped) | escaped partition key
//	table without clustering columns:  row       = P
//	table with clustering columns:     header    = P | 0x10   (static row, partition deletion)
//	                                   tombstone = P | 0x18 | start bound | end bound
//	                                   row       = P | clustering components | 0x38
//
// The escaped partition key is the partition key's serialized form (a
// single column's value, or CompositeType's for several), escaped as a
// variable-length byte-comparable value, so that partitions with the same
// token sort by key bytes, as DecoratedKey does. Header and range tombstone
// keys sort before every row and every clustering bound (0x20 and above).
const (
	headerSuffix    = 0x10
	tombstoneSuffix = 0x18
)

// murmur3 is Cassandra's MurmurHash.hash3_x64_128, which differs from the
// reference MurmurHash3 in sign-extending the tail bytes, and returns the
// first 64 bits.
func murmur3(key []byte) int64 {
	const c1, c2 = 0x87c37b91114253d5, 0x4cf5ad432745937f
	var h1, h2 uint64
	n := len(key)
	nblocks := n / 16
	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint64(key[i*16:])
		k2 := binary.LittleEndian.Uint64(key[i*16+8:])
		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729
		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}
	tail := key[nblocks*16:]
	// Java's (long) key.get(i) sign-extends the byte.
	b := func(i int) uint64 { return uint64(int64(int8(tail[i]))) }
	var k1, k2 uint64
	switch n & 15 {
	case 15:
		k2 ^= b(14) << 48
		fallthrough
	case 14:
		k2 ^= b(13) << 40
		fallthrough
	case 13:
		k2 ^= b(12) << 32
		fallthrough
	case 12:
		k2 ^= b(11) << 24
		fallthrough
	case 11:
		k2 ^= b(10) << 16
		fallthrough
	case 10:
		k2 ^= b(9) << 8
		fallthrough
	case 9:
		k2 ^= b(8)
		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= b(7) << 56
		fallthrough
	case 7:
		k1 ^= b(6) << 48
		fallthrough
	case 6:
		k1 ^= b(5) << 40
		fallthrough
	case 5:
		k1 ^= b(4) << 32
		fallthrough
	case 4:
		k1 ^= b(3) << 24
		fallthrough
	case 3:
		k1 ^= b(2) << 16
		fallthrough
	case 2:
		k1 ^= b(1) << 8
		fallthrough
	case 1:
		k1 ^= b(0)
		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}
	h1 ^= uint64(n)
	h2 ^= uint64(n)
	h1 += h2
	h2 += h1
	h1 = fmix(h1)
	h2 = fmix(h2)
	h1 += h2
	return int64(h1)
}

func fmix(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

// Token is Murmur3Partitioner.getToken: the hash of the serialized
// partition key, with Long.MIN_VALUE (the minimum token, which no key may
// have) mapped to Long.MAX_VALUE; an empty key has the minimum token.
func Token(key []byte) int64 {
	if len(key) == 0 {
		return math.MinInt64
	}
	if h := murmur3(key); h != math.MinInt64 {
		return h
	}
	return math.MaxInt64
}

// tokenBytes appends a token so that unsigned byte order is signed token
// order.
func appendToken(dst []byte, tok int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(tok)^(1<<63))
}

func tokenOf(key []byte) int64 { return int64(binary.BigEndian.Uint64(key) ^ (1 << 63)) }

// serializePartitionKey returns the partition key's serialized form: the
// value of a single-column key, or CompositeType.build's for several.
func serializePartitionKey(vals [][]byte) []byte {
	if len(vals) == 1 {
		return vals[0]
	}
	n := 0
	for _, v := range vals {
		n += 3 + len(v)
	}
	out := make([]byte, 0, n)
	for _, v := range vals {
		out = binary.BigEndian.AppendUint16(out, uint16(len(v)))
		out = append(out, v...)
		out = append(out, 0)
	}
	return out
}

var errBadKey = errors.New("db: corrupt storage key")

// splitPartitionKey is the inverse of serializePartitionKey for n columns.
func splitPartitionKey(pk []byte, n int) ([][]byte, error) {
	if n == 1 {
		return [][]byte{pk}, nil
	}
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if len(pk) < 3 {
			return nil, errBadKey
		}
		l := int(binary.BigEndian.Uint16(pk))
		if len(pk) < 3+l {
			return nil, errBadKey
		}
		out = append(out, pk[2:2+l])
		pk = pk[3+l:]
	}
	return out, nil
}

// partitionPrefix returns P for a serialized partition key.
func partitionPrefix(pk []byte) []byte {
	out := appendToken(make([]byte, 0, 8+len(pk)+2), Token(pk))
	return appendEscaped(out, pk)
}

// appendEscaped escapes a byte string as a variable-length byte-comparable
// value (see types.AppendComparable for blobs).
func appendEscaped(dst, v []byte) []byte {
	out, _ := types.AppendComparable(dst, types.BlobType, v)
	if len(v) == 0 {
		// AppendComparable is for non-empty values; the empty key (only
		// possible for tokens) is a lone terminator.
		return append(dst, 0x00)
	}
	return out
}

// parsePartition splits a storage key into its token, serialized partition
// key and the rest (the suffix after P).
func parsePartition(key []byte) (tok int64, pk, rest []byte, err error) {
	if len(key) < 9 {
		return 0, nil, nil, errBadKey
	}
	tok = tokenOf(key)
	// Unescape until the end of the value: a 0x00 that is not followed by
	// FE (another zero) or FF (a zero, then more bytes).
	b := key[8:]
	var out []byte
	for i := 0; i < len(b); {
		c := b[i]
		i++
		if c != 0 {
			out = append(out, c)
			continue
		}
		for {
			if i < len(b) && b[i] == 0xFE {
				out = append(out, 0)
				i++
				continue
			}
			if i < len(b) && b[i] == 0xFF {
				out = append(out, 0)
				i++
				break
			}
			return tok, out, b[i:], nil
		}
	}
	return 0, nil, nil, errBadKey
}

// clusteringKey appends the clustering components of a row and the
// terminator.
func appendClustering(dst []byte, t *Table, vals [][]byte) ([]byte, error) {
	var err error
	for i, c := range t.Clustering {
		if dst, err = types.AppendComponent(dst, c.Type, vals[i], c.Desc); err != nil {
			return nil, err
		}
	}
	return append(dst, types.Terminator), nil
}

// parseClustering decodes the clustering values of a row key suffix.
func parseClustering(t *Table, suffix []byte) ([][]byte, error) {
	out := make([][]byte, len(t.Clustering))
	var err error
	for i, c := range t.Clustering {
		if out[i], suffix, err = types.ReadComponent(suffix, c.Type, c.Desc); err != nil {
			return nil, err
		}
	}
	if len(suffix) != 1 || suffix[0] != types.Terminator {
		return nil, errBadKey
	}
	return out, nil
}

// prefixEnd returns the smallest key greater than every key with prefix p.
func prefixEnd(p []byte) []byte {
	out := append([]byte(nil), p...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
