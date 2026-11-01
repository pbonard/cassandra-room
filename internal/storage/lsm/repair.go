package lsm

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
)

// TokenRange is a range of tokens, both ends inclusive, so the whole ring
// is {0, math.MaxUint64}.
type TokenRange struct{ First, Last uint64 }

// FullRing is every token.
var FullRing = TokenRange{0, math.MaxUint64}

// size returns the number of tokens in the range, or 0 for the full ring
// (2^64).
func (r TokenRange) size() uint64 { return r.Last - r.First + 1 }

// Bounds returns the key bounds [lower, upper) of the range's rows, nil
// for open ends.
func (r TokenRange) Bounds() (lower, upper []byte) {
	if r.First > 0 {
		lower = tokenKey(r.First)
	}
	if r.Last < math.MaxUint64 {
		upper = tokenKey(r.Last + 1)
	}
	return lower, upper
}

// Hash is a 256-bit digest.
type Hash [32]byte

func (h *Hash) xor(o *Hash) {
	for i := range h {
		h[i] ^= o[i]
	}
}

// MerkleTree is a hash tree of a table's rows over a token range, split into
// 2^Depth equal leaves: the perfect tree of Cassandra's MerkleTree, which
// Cassandra's trees of the same depth can always be compared against. As in
// Cassandra, every row (Cassandra: partition) is hashed with a 256-bit
// digest (Cassandra concatenates two murmur3_128; the engine uses
// SHA-256), a leaf is the XOR of its rows' digests, and an inner node the
// XOR of its children. Two replicas compare trees and stream only the
// ranges whose leaves differ.
type MerkleTree struct {
	Range TokenRange
	Depth int
	// Leaves hold one hash per leaf, of the rows whose token falls in it.
	Leaves []Hash
}

// maxMerkleDepth bounds a tree to a million leaves.
const maxMerkleDepth = 20

// MerkleTree computes the tree of the table's rows in r, as of the call.
// Rows are hashed as reads see them after purging what gc_grace_seconds
// allows, so a replica that has already purged a deletion agrees with one
// that has not (Cassandra's validation compaction also purges).
func (t *Table) MerkleTree(r TokenRange, depth int) (*MerkleTree, error) {
	if depth < 0 || depth > maxMerkleDepth {
		return nil, fmt.Errorf("lsm: merkle tree depth %d out of [0, %d]", depth, maxMerkleDepth)
	}
	if r.Last < r.First {
		return nil, fmt.Errorf("lsm: empty token range %+v", r)
	}
	sn, err := t.NewSnapshot()
	if err != nil {
		return nil, err
	}
	defer sn.Close()
	lower, upper := r.Bounds()
	it, err := sn.NewIter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	n := uint64(1) << depth
	m := &MerkleTree{Range: r, Depth: depth, Leaves: make([]Hash, n)}
	purger, _ := t.e.merger.(Purger)
	nowSec := t.e.opts.Now().Unix()
	gcBefore := t.opts.gcBefore(nowSec)
	noOther := func(int64, int64) (int64, error) { return math.MaxInt64, nil }
	var buf, msg []byte
	for ok := it.First(); ok; ok = it.Next() {
		v := it.Value()
		if purger != nil {
			out, _, empty, _, err := purger.PurgeAndSummarize(buf[:0], v, nowSec, gcBefore, noOther, nil)
			if err != nil {
				return nil, err
			}
			if empty {
				continue
			}
			buf, v = out, out
		}
		// The row's digest covers its key and its (purged) value, each
		// length-prefixed.
		msg = binary.AppendUvarint(msg[:0], uint64(len(it.Key())))
		msg = append(msg, it.Key()...)
		msg = binary.AppendUvarint(msg, uint64(len(v)))
		msg = append(msg, v...)
		d := Hash(sha256.Sum256(msg))
		m.Leaves[m.leafOf(keyToken(it.Key()))].xor(&d)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return m, nil
}

// leafOf returns the leaf holding token tok: floor((tok-First)·n/size).
func (m *MerkleTree) leafOf(tok uint64) uint64 {
	hi, lo := bits.Mul64(tok-m.Range.First, uint64(len(m.Leaves)))
	if size := m.Range.size(); size != 0 {
		q, _ := bits.Div64(hi, lo, size)
		return q
	}
	return hi // size 2^64
}

// leafRange returns the tokens of leaf i: those whose leafOf is i.
func (m *MerkleTree) leafRange(i int) TokenRange {
	start := func(i int) *big.Int {
		// First + ceil(i·size/n)
		size := new(big.Int).SetUint64(m.Range.size())
		if m.Range.size() == 0 {
			size.Lsh(big.NewInt(1), 64)
		}
		x := new(big.Int).Mul(size, big.NewInt(int64(i)))
		n := big.NewInt(int64(len(m.Leaves)))
		x.Add(x, new(big.Int).Sub(n, big.NewInt(1)))
		x.Quo(x, n)
		return x.Add(x, new(big.Int).SetUint64(m.Range.First))
	}
	first, next := start(i), start(i+1)
	return TokenRange{First: first.Uint64(), Last: next.Sub(next, big.NewInt(1)).Uint64()}
}

// Root returns the tree's root hash. Inner nodes are the XOR of their
// children, as in Cassandra, so the root is the XOR of all leaves.
func (m *MerkleTree) Root() Hash {
	var root Hash
	for i := range m.Leaves {
		root.xor(&m.Leaves[i])
	}
	return root
}

// Diff returns the token ranges where the trees differ, adjacent leaves
// merged: the ranges repair streams in both directions. The trees must
// cover the same range at the same depth.
func (m *MerkleTree) Diff(o *MerkleTree) ([]TokenRange, error) {
	if m.Range != o.Range || m.Depth != o.Depth || len(m.Leaves) != len(o.Leaves) {
		return nil, errors.New("lsm: merkle trees of different shapes")
	}
	var out []TokenRange
	for i := range m.Leaves {
		if m.Leaves[i] == o.Leaves[i] {
			continue
		}
		r := m.leafRange(i)
		if n := len(out); n > 0 && out[n-1].Last+1 == r.First {
			out[n-1].Last = r.Last
			continue
		}
		out = append(out, r)
	}
	return out, nil
}
