package lsm

import (
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
)

// batchHeaderLen is the size of a batch's header: the sequence number of its
// first entry (8 bytes) and its entry count (4 bytes), little-endian.
const batchHeaderLen = 12

var errCorruptBatch = errors.New("lsm: corrupt batch")

// Batch is a set of writes, possibly to several tables, applied atomically by
// Engine.Apply. Every write is a row fragment (a Pebble MERGE entry) that is
// reconciled with the row's other fragments by the engine's Merger. Its
// encoding is also the WAL record format:
//
//	header: seqnum uint64 | count uint32
//	entry:  kind byte | uvarint table | uvarint keyLen | key | uvarint valueLen | value
type Batch struct {
	data   []byte
	count  uint32
	tables []TableID // distinct tables written, in first-use order
	// ps is used by Engine.Apply for a synced write, saving an allocation.
	ps pendingSync
}

// NewBatch returns an empty batch.
func NewBatch() *Batch {
	return &Batch{data: make([]byte, batchHeaderLen, 256)}
}

var batchPool = sync.Pool{New: func() any { return NewBatch() }}

// Put adds a row fragment for key in table. Deletions are fragments too
// (tombstone cells or a row deletion); Pebble's DELETE kinds are never used
// for user data, because they shadow by arrival order, not by timestamp.
func (b *Batch) Put(table TableID, key, value []byte) {
	b.data = append(b.data, byte(pebble.InternalKeyKindMerge))
	b.data = binary.AppendUvarint(b.data, uint64(table))
	b.data = binary.AppendUvarint(b.data, uint64(len(key)))
	b.data = append(b.data, key...)
	b.data = binary.AppendUvarint(b.data, uint64(len(value)))
	b.data = append(b.data, value...)
	b.count++
	if !slices.Contains(b.tables, table) {
		b.tables = append(b.tables, table)
	}
}

// Count returns the number of entries in the batch.
func (b *Batch) Count() int { return int(b.count) }

// Reset empties the batch so it can be reused.
func (b *Batch) Reset() {
	b.data = b.data[:batchHeaderLen]
	clear(b.data)
	b.count = 0
	b.tables = b.tables[:0]
}

// setSeqNum fills in the header before the batch is written to the WAL.
func (b *Batch) setSeqNum(seq uint64) {
	binary.LittleEndian.PutUint64(b.data[0:8], seq)
	binary.LittleEndian.PutUint32(b.data[8:12], b.count)
}

// batchEntry is one decoded batch entry. key and value alias the batch.
type batchEntry struct {
	kind  pebble.InternalKeyKind
	table TableID
	key   []byte
	value []byte
}

// batchReader decodes an encoded batch (a WAL record).
type batchReader struct {
	seq   uint64
	count uint32
	data  []byte
}

func newBatchReader(data []byte) (batchReader, error) {
	if len(data) < batchHeaderLen {
		return batchReader{}, errCorruptBatch
	}
	return batchReader{
		seq:   binary.LittleEndian.Uint64(data[0:8]),
		count: binary.LittleEndian.Uint32(data[8:12]),
		data:  data[batchHeaderLen:],
	}, nil
}

// next returns the next entry. ok is false at the end of the batch.
func (r *batchReader) next() (e batchEntry, ok bool, err error) {
	if len(r.data) == 0 {
		return batchEntry{}, false, nil
	}
	e.kind = pebble.InternalKeyKind(r.data[0])
	r.data = r.data[1:]
	table, n := binary.Uvarint(r.data)
	if n <= 0 {
		return batchEntry{}, false, errCorruptBatch
	}
	e.table = TableID(table)
	r.data = r.data[n:]
	if e.kind != pebble.InternalKeyKindMerge {
		return batchEntry{}, false, errCorruptBatch
	}
	if e.key, err = r.readBytes(); err != nil {
		return batchEntry{}, false, err
	}
	if e.value, err = r.readBytes(); err != nil {
		return batchEntry{}, false, err
	}
	return e, true, nil
}

func (r *batchReader) readBytes() ([]byte, error) {
	n, w := binary.Uvarint(r.data)
	if w <= 0 || uint64(len(r.data)-w) < n {
		return nil, errCorruptBatch
	}
	b := r.data[w : w+int(n)]
	r.data = r.data[w+int(n):]
	return b, nil
}
