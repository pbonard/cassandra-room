package lsm

import (
	"bytes"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

const (
	maxHeight = 20
	// pBranch is the inverse probability of a node reaching the next level.
	pBranch = 4
	// arenaChunkSize is the size of the chunks keys and values are copied
	// into, which keeps small allocations off the garbage collector's path.
	arenaChunkSize = 64 << 10
	// nodeSlabSize and towerSlabSize are how many nodes and tower slots are
	// allocated at once.
	nodeSlabSize  = 256
	towerSlabSize = 512
	// nodeOverhead approximates the memory a node uses besides its key,
	// value and tower, for size accounting.
	nodeOverhead = 64
)

// memNode is a skiplist node. key, value and trailer never change after the
// node is published; tower entries are only ever set by the single writer.
type memNode struct {
	key     []byte
	value   []byte
	trailer uint64 // seqnum << 8 | kind, as in Pebble's internal keys
	tower   []atomic.Pointer[memNode]
}

func (n *memNode) next() *memNode { return n.tower[0].Load() }

func (n *memNode) kind() pebble.InternalKeyKind { return pebble.InternalKeyKind(n.trailer & 0xff) }

// memTable is a skiplist ordered by user key ascending, then trailer
// descending (newest version first), matching Pebble's internal key order.
//
// Nodes are allocated one at a time (newNode, serialized by the engine's
// commitMu) and linked with compare-and-swap (insert), which is safe for
// concurrent writers; the engine currently links under commitMu too. Readers
// are lock-free.
type memTable struct {
	head   *memNode
	height atomic.Int32
	size   atomic.Int64
	// count is the number of entries allocated, linked or not.
	count atomic.Int64
	// used is the engine-wide memtable memory counter this memtable adds to.
	used *atomic.Int64
	// firstSeg is the WAL segment of the memtable's first entry, or 0 while
	// it is empty. Segments from firstSeg on are needed until the memtable is
	// flushed.
	firstSeg atomic.Uint64

	// Allocator state, used only by newNode under commitMu.
	rnd    uint64
	chunk  []byte
	nodes  []memNode
	towers []atomic.Pointer[memNode]
}

var memTableSeed atomic.Uint64

func newMemTable(used *atomic.Int64) *memTable {
	m := &memTable{
		head: &memNode{tower: make([]atomic.Pointer[memNode], maxHeight)},
		used: used,
		rnd:  memTableSeed.Add(1)*0x9E3779B97F4A7C15 | 1,
	}
	m.height.Store(1)
	return m
}

// empty reports whether no entry was ever allocated, so entries still being
// linked count as present.
func (m *memTable) empty() bool { return m.count.Load() == 0 }

func (m *memTable) approximateSize() int64 { return m.size.Load() }

// compare orders node n against (key, trailer).
func compareNode(n *memNode, key []byte, trailer uint64) int {
	if c := bytes.Compare(n.key, key); c != 0 {
		return c
	}
	switch {
	case n.trailer > trailer:
		return -1
	case n.trailer < trailer:
		return 1
	}
	return 0
}

func (m *memTable) randomHeight() int {
	h := 1
	for h < maxHeight {
		// xorshift64*
		m.rnd ^= m.rnd >> 12
		m.rnd ^= m.rnd << 25
		m.rnd ^= m.rnd >> 27
		if (m.rnd*2685821657736338717)%pBranch != 0 {
			break
		}
		h++
	}
	return h
}

// alloc copies b into the arena.
func (m *memTable) alloc(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	if len(b) > arenaChunkSize/4 {
		return append([]byte(nil), b...)
	}
	if cap(m.chunk)-len(m.chunk) < len(b) {
		m.chunk = make([]byte, 0, arenaChunkSize)
	}
	start := len(m.chunk)
	m.chunk = append(m.chunk, b...)
	return m.chunk[start:len(m.chunk):len(m.chunk)]
}

// newNode allocates an unlinked node holding copies of key and value. Calls
// must be serialized (the engine holds commitMu).
func (m *memTable) newNode(key []byte, trailer uint64, value []byte) *memNode {
	h := m.randomHeight()
	if len(m.nodes) == 0 {
		m.nodes = make([]memNode, nodeSlabSize)
	}
	n := &m.nodes[0]
	m.nodes = m.nodes[1:]
	if len(m.towers) < h {
		m.towers = make([]atomic.Pointer[memNode], max(towerSlabSize, h))
	}
	n.key = m.alloc(key)
	n.value = m.alloc(value)
	n.trailer = trailer
	n.tower = m.towers[:h:h]
	m.towers = m.towers[h:]
	sz := int64(len(key) + len(value) + nodeOverhead + 8*h)
	m.size.Add(sz)
	m.used.Add(sz)
	m.count.Add(1)
	return n
}

// findSplice returns the nodes before and after (key, trailer) at level,
// starting the search at start.
func findSplice(start *memNode, level int, key []byte, trailer uint64) (prev, next *memNode) {
	prev = start
	for {
		next = prev.tower[level].Load()
		if next == nil || compareNode(next, key, trailer) >= 0 {
			return prev, next
		}
		prev = next
	}
}

// insert links n into the list. It may run concurrently with other inserts
// and with readers. Nodes are linked bottom-up, each level with a
// compare-and-swap, so a node reachable at a level is already linked at every
// level below, and readers always see a well-formed list.
func (m *memTable) insert(n *memNode) {
	h := len(n.tower)
	for {
		cur := m.height.Load()
		if int(cur) >= h || m.height.CompareAndSwap(cur, int32(h)) {
			break
		}
	}
	var prev, next [maxHeight]*memNode
	x := m.head
	for level := maxHeight - 1; level >= 0; level-- {
		prev[level], next[level] = findSplice(x, level, n.key, n.trailer)
		x = prev[level]
	}
	for level := 0; level < h; level++ {
		for {
			n.tower[level].Store(next[level])
			if prev[level].tower[level].CompareAndSwap(next[level], n) {
				break
			}
			// Another writer linked a node here first; find the splice again
			// from the old predecessor, which still precedes n.
			prev[level], next[level] = findSplice(prev[level], level, n.key, n.trailer)
		}
	}
}

// add allocates and links an entry. Calls must be serialized (the engine
// holds commitMu), so plain stores suffice: each new pointer is set before
// the node is published at that level, and nodes are linked bottom-up, so
// readers always see a well-formed list.
func (m *memTable) add(key []byte, trailer uint64, value []byte) {
	n := m.newNode(key, trailer, value)
	h := len(n.tower)
	listHeight := int(m.height.Load())
	var prev [maxHeight]*memNode
	x := m.head
	for level := listHeight - 1; level >= 0; level-- {
		x, _ = findSplice(x, level, key, trailer)
		prev[level] = x
	}
	if h > listHeight {
		for level := listHeight; level < h; level++ {
			prev[level] = m.head
		}
		m.height.Store(int32(h))
	}
	for level := 0; level < h; level++ {
		n.tower[level].Store(prev[level].tower[level].Load())
		prev[level].tower[level].Store(n)
	}
}

// seekGE returns the first node at or after (key, trailer), or nil.
func (m *memTable) seekGE(key []byte, trailer uint64) *memNode {
	x := m.head
	var nx *memNode
	for level := int(m.height.Load()) - 1; level >= 0; level-- {
		for {
			nx = x.tower[level].Load()
			if nx == nil || compareNode(nx, key, trailer) >= 0 {
				break
			}
			x = nx
		}
	}
	// Return the node that was compared, not a fresh load of x.tower[0]: a
	// concurrent insert may have linked a smaller node after x since then.
	return nx
}

// get returns the newest entry for key with a sequence number at or below
// seq, or nil.
func (m *memTable) get(key []byte, seq uint64) *memNode {
	n := m.seekGE(key, makeTrailer(seq, pebble.InternalKeyKindMax))
	if n == nil || !bytes.Equal(n.key, key) {
		return nil
	}
	return n
}

// getMerged merges every version of key with a sequence number at or below
// seq into acc (which may be nil) and returns the result. found reports
// whether any version exists.
func (m *memTable) getMerged(merger Merger, key []byte, seq uint64, acc []byte) (_ []byte, found bool, err error) {
	for n := m.get(key, seq); n != nil && bytes.Equal(n.key, key); n = n.next() {
		if acc, err = mergeInto(merger, acc, n.value); err != nil {
			return nil, false, err
		}
		found = true
	}
	return acc, found, nil
}

// mergeInto merges v into acc; a nil acc is a copy of v.
func mergeInto(merger Merger, acc, v []byte) ([]byte, error) {
	if acc == nil {
		return append([]byte(nil), v...), nil
	}
	return merger.Merge(nil, acc, v)
}

// first returns the first node, or nil if the memtable is empty.
func (m *memTable) first() *memNode { return m.head.next() }

func makeTrailer(seq uint64, kind pebble.InternalKeyKind) uint64 {
	return seq<<8 | uint64(kind)
}
