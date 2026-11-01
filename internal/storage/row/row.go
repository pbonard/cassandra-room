// Package row encodes Cassandra rows as storage-engine values and reconciles
// them with Cassandra's rules (db/rows/Cells.java, db/DeletionTime.java in
// Cassandra 5.0).
//
// A row value holds an optional row deletion and a list of cells sorted by
// column. Every write is a row fragment; fragments of the same row are merged
// with Merge, which is commutative and associative, so the storage engine can
// combine them in any order.
//
// Encoding:
//
//	row  := version(1) | flags(1) | [deletion] | uvarint numCells | cell...
//	deletion := varint markedForDeleteAt | varint localDeletionTime
//	cell := uvarint column | varint timestamp | varint localDeletionTime
//	        | uvarint ttl | (live or expiring only) uvarint len | value
//
// localDeletionTime is NoDeletionTime for live cells, the expiry time for
// expiring cells, and the deletion time for tombstones.
package row

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const (
	version         = 1
	flagHasDeletion = 1
	// NoDeletionTime marks a live cell, as Cassandra's Cell.NO_DELETION_TIME.
	NoDeletionTime = math.MaxInt64
	// NoTimestamp means "no deletion" for a row deletion.
	NoTimestamp = math.MinInt64
)

// ErrCorrupt is returned for a value that is not a valid row.
var ErrCorrupt = errors.New("row: corrupt value")

// Cell is one column value of a row.
type Cell struct {
	Column            uint32
	Timestamp         int64
	LocalDeletionTime int64 // NoDeletionTime for live cells
	TTL               uint32
	Value             []byte
}

// IsTombstone reports whether the cell is a deletion.
func (c *Cell) IsTombstone() bool { return c.LocalDeletionTime != NoDeletionTime && c.TTL == 0 }

// IsExpiring reports whether the cell has a TTL.
func (c *Cell) IsExpiring() bool { return c.TTL != 0 }

// IsLive reports whether the cell is visible at nowInSec (Cassandra's
// AbstractCell.isLive).
func (c *Cell) IsLive(nowInSec int64) bool {
	return c.LocalDeletionTime == NoDeletionTime || (c.TTL != 0 && nowInSec < c.LocalDeletionTime)
}

// Deletion is a row deletion: every cell with a timestamp at or below
// MarkedForDeleteAt is deleted.
type Deletion struct {
	MarkedForDeleteAt int64 // NoTimestamp if the row is not deleted
	LocalDeletionTime int64
}

// IsLive reports whether there is no deletion.
func (d Deletion) IsLive() bool { return d.MarkedForDeleteAt == NoTimestamp }

// Deletes reports whether the deletion shadows data written at ts.
func (d Deletion) Deletes(ts int64) bool { return ts <= d.MarkedForDeleteAt }

// supersedes is Cassandra's DeletionTime.supersedes.
func (d Deletion) supersedes(o Deletion) bool {
	return d.MarkedForDeleteAt > o.MarkedForDeleteAt ||
		(d.MarkedForDeleteAt == o.MarkedForDeleteAt && d.LocalDeletionTime > o.LocalDeletionTime)
}

// Row is a decoded row.
type Row struct {
	Deletion Deletion
	Cells    []Cell // sorted by Column, unique
}

// Live returns the cells visible at nowInSec: live, and not shadowed by the
// row deletion.
func (r *Row) Live(nowInSec int64) []Cell {
	var out []Cell
	for i := range r.Cells {
		if r.Cells[i].IsLive(nowInSec) && !r.Deletion.Deletes(r.Cells[i].Timestamp) {
			out = append(out, r.Cells[i])
		}
	}
	return out
}

// Append encodes r and appends it to dst. Cells must be sorted by column.
// Cells shadowed by the row deletion are dropped, so every row has one
// encoding and Merge(a, a) == a.
func Append(dst []byte, r *Row) []byte {
	flags := byte(0)
	if !r.Deletion.IsLive() {
		flags |= flagHasDeletion
	}
	dst = append(dst, version, flags)
	if flags&flagHasDeletion != 0 {
		dst = binary.AppendVarint(dst, r.Deletion.MarkedForDeleteAt)
		dst = binary.AppendVarint(dst, r.Deletion.LocalDeletionTime)
	}
	n := 0
	for i := range r.Cells {
		if !r.Deletion.Deletes(r.Cells[i].Timestamp) {
			n++
		}
	}
	dst = binary.AppendUvarint(dst, uint64(n))
	for i := range r.Cells {
		if !r.Deletion.Deletes(r.Cells[i].Timestamp) {
			dst = appendCell(dst, &r.Cells[i])
		}
	}
	return dst
}

func appendCell(dst []byte, c *Cell) []byte {
	dst = binary.AppendUvarint(dst, uint64(c.Column))
	dst = binary.AppendVarint(dst, c.Timestamp)
	dst = binary.AppendVarint(dst, c.LocalDeletionTime)
	dst = binary.AppendUvarint(dst, uint64(c.TTL))
	if !c.IsTombstone() {
		dst = binary.AppendUvarint(dst, uint64(len(c.Value)))
		dst = append(dst, c.Value...)
	}
	return dst
}

// decoder reads an encoded row without allocating; cell values alias the
// input.
type decoder struct {
	b        []byte
	deletion Deletion
	n        uint64
	err      error
}

func newDecoder(b []byte) decoder {
	d := decoder{deletion: Deletion{MarkedForDeleteAt: NoTimestamp}}
	if len(b) < 2 || b[0] != version {
		d.err = ErrCorrupt
		return d
	}
	flags := b[1]
	d.b = b[2:]
	if flags&flagHasDeletion != 0 {
		d.deletion.MarkedForDeleteAt = d.varint()
		d.deletion.LocalDeletionTime = d.varint()
	}
	d.n = d.uvarint()
	return d
}

func (d *decoder) varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

// next decodes the next cell into c. It returns false at the end or on error.
func (d *decoder) next(c *Cell) bool {
	if d.err != nil || d.n == 0 {
		return false
	}
	d.n--
	c.Column = uint32(d.uvarint())
	c.Timestamp = d.varint()
	c.LocalDeletionTime = d.varint()
	c.TTL = uint32(d.uvarint())
	c.Value = nil
	if !c.IsTombstone() {
		l := d.uvarint()
		if d.err == nil && uint64(len(d.b)) < l {
			d.err = ErrCorrupt
		}
		if d.err != nil {
			return false
		}
		c.Value = d.b[:l:l]
		d.b = d.b[l:]
	}
	return d.err == nil
}

// Decode decodes a row. Cell values alias b.
func Decode(b []byte) (*Row, error) {
	d := newDecoder(b)
	r := &Row{Deletion: d.deletion}
	var c Cell
	for d.next(&c) {
		r.Cells = append(r.Cells, c)
	}
	if d.err != nil {
		return nil, d.err
	}
	return r, nil
}

// reconcile returns the cell that wins between two cells of the same column
// (Cassandra's Cells.resolveRegular).
func reconcile(l, r *Cell) *Cell {
	if l.Timestamp != r.Timestamp {
		if l.Timestamp > r.Timestamp {
			return l
		}
		return r
	}
	lDel, rDel := l.LocalDeletionTime != NoDeletionTime, r.LocalDeletionTime != NoDeletionTime
	if lDel || rDel {
		// Tombstones and expiring cells win over live cells of the same
		// timestamp, regardless of the clock (CASSANDRA-14592).
		if lDel != rDel {
			if lDel {
				return l
			}
			return r
		}
		lTomb, rTomb := !l.IsExpiring(), !r.IsExpiring()
		if lTomb != rTomb {
			if lTomb {
				return l
			}
			return r
		}
		if l.LocalDeletionTime != r.LocalDeletionTime {
			if l.LocalDeletionTime > r.LocalDeletionTime {
				return l
			}
			return r
		}
		if !lTomb && l.TTL != r.TTL {
			if l.TTL < r.TTL {
				return l
			}
			return r
		}
	}
	if bytes.Compare(l.Value, r.Value) >= 0 {
		return l
	}
	return r
}

// Merge reconciles two fragments of the same row and appends the result to
// dst. It is commutative and associative.
func Merge(dst, a, b []byte) ([]byte, error) {
	da, db := newDecoder(a), newDecoder(b)
	if da.err != nil || db.err != nil {
		return nil, ErrCorrupt
	}
	del := da.deletion
	if db.deletion.supersedes(del) {
		del = db.deletion
	}
	flags := byte(0)
	if !del.IsLive() {
		flags |= flagHasDeletion
	}
	dst = append(dst, version, flags)
	if flags&flagHasDeletion != 0 {
		dst = binary.AppendVarint(dst, del.MarkedForDeleteAt)
		dst = binary.AppendVarint(dst, del.LocalDeletionTime)
	}
	// The cell count is not known until the cells are merged; reserve the
	// largest uvarint the count can need and compact afterwards.
	countAt := len(dst)
	dst = append(dst, make([]byte, binary.MaxVarintLen32)...)
	var count uint64
	emit := func(c *Cell) {
		if del.Deletes(c.Timestamp) {
			return
		}
		dst = appendCell(dst, c)
		count++
	}
	var ca, cb Cell
	okA, okB := da.next(&ca), db.next(&cb)
	for okA || okB {
		switch {
		case okA && (!okB || ca.Column < cb.Column):
			emit(&ca)
			okA = da.next(&ca)
		case okB && (!okA || cb.Column < ca.Column):
			emit(&cb)
			okB = db.next(&cb)
		default:
			emit(reconcile(&ca, &cb))
			okA, okB = da.next(&ca), db.next(&cb)
		}
	}
	if da.err != nil || db.err != nil {
		return nil, ErrCorrupt
	}
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutUvarint(tmp[:], count)
	copy(dst[countAt:], tmp[:n])
	dst = append(dst[:countAt+n], dst[countAt+binary.MaxVarintLen32:]...)
	return dst, nil
}

// Timestamps returns the smallest and largest timestamps in a row value,
// counting the row deletion. ok is false for an empty row.
func Timestamps(v []byte) (minTS, maxTS int64, ok bool, err error) {
	d := newDecoder(v)
	minTS, maxTS = math.MaxInt64, math.MinInt64
	if !d.deletion.IsLive() {
		minTS, maxTS, ok = d.deletion.MarkedForDeleteAt, d.deletion.MarkedForDeleteAt, true
	}
	var c Cell
	for d.next(&c) {
		minTS, maxTS, ok = min(minTS, c.Timestamp), max(maxTS, c.Timestamp), true
	}
	return minTS, maxTS, ok, d.err
}

// DeletedAt returns the row deletion's MarkedForDeleteAt, or NoTimestamp.
func DeletedAt(v []byte) (int64, error) {
	d := newDecoder(v)
	return d.deletion.MarkedForDeleteAt, d.err
}

// Merger implements the storage engine's lsm.Merger with Cassandra's rules.
type Merger struct{}

// Merge implements lsm.Merger.
func (Merger) Merge(dst, a, b []byte) ([]byte, error) { return Merge(dst, a, b) }

// Timestamps implements lsm.Merger.
func (Merger) Timestamps(v []byte) (minTS, maxTS int64, ok bool, err error) {
	return Timestamps(v)
}

// DeletedAt implements lsm.Merger.
func (Merger) DeletedAt(v []byte) (int64, error) { return DeletedAt(v) }

// Stats summarises a row for sstable metadata (Cassandra's
// MetadataCollector: timestamps, local deletion times, tombstones).
type Stats struct {
	// MinTS and MaxTS are the smallest and largest timestamps, counting the
	// row deletion; HasTS is false for an empty row.
	MinTS, MaxTS int64
	HasTS        bool
	// MaxLDT is the largest local deletion time, NoDeletionTime if the row
	// has a live (non-expiring) cell: once MaxLDT is in the past, every part
	// of the row is deleted or expired.
	MaxLDT int64
	// Tombstones counts cell tombstones, expiring cells and the row
	// deletion: the parts that become droppable at their local deletion
	// time (the expiry time, for expiring cells).
	Tombstones int
	// MinLDT is the earliest local deletion time of those parts, or
	// NoDeletionTime if there are none: nothing in the row can be purged
	// before MinLDT.
	MinLDT int64
}

// Summarize returns the row's Stats. If ldt is not nil, it is called with
// the local deletion time of every tombstone, expiring cell and row deletion
// (for the tombstone drop-time histogram).
func Summarize(v []byte, ldt func(int64)) (Stats, error) {
	d := newDecoder(v)
	s := Stats{MinTS: math.MaxInt64, MaxTS: math.MinInt64, MaxLDT: math.MinInt64, MinLDT: NoDeletionTime}
	droppable := func(t int64) {
		s.Tombstones++
		s.MinLDT = min(s.MinLDT, t)
		s.MaxLDT = max(s.MaxLDT, t)
		if ldt != nil {
			ldt(t)
		}
	}
	if !d.deletion.IsLive() {
		s.MinTS, s.MaxTS, s.HasTS = d.deletion.MarkedForDeleteAt, d.deletion.MarkedForDeleteAt, true
		droppable(d.deletion.LocalDeletionTime)
	}
	var c Cell
	for d.next(&c) {
		s.MinTS, s.MaxTS, s.HasTS = min(s.MinTS, c.Timestamp), max(s.MaxTS, c.Timestamp), true
		if c.LocalDeletionTime == NoDeletionTime {
			s.MaxLDT = NoDeletionTime
		} else {
			droppable(c.LocalDeletionTime)
		}
	}
	return s, d.err
}

// Purge drops from row v what Cassandra's compaction purges
// (AbstractCell.purge, DeletionPurger.shouldPurge): tombstones, expired
// cells and a row deletion whose local deletion time is before gcBefore and
// whose timestamp is below maxPurgeable, the smallest timestamp of the row's
// data outside the compaction. Such a deletion cannot shadow anything still
// stored elsewhere, so dropping it never changes what reads return.
// maxPurgeable is called at most once, only if something is a candidate,
// with the smallest and largest candidate timestamps: it may skip data newer
// than maxTS (it decides nothing), and may return math.MinInt64 as soon as
// it finds data at or below minTS (then nothing can be purged).
//
// Unlike Cassandra, expired cells that cannot be purged yet are not turned
// into tombstones: a tombstone beats an expiring cell of the same
// timestamp, so the conversion could hide a later-expiring cell stored
// elsewhere.
//
// It appends the result to dst; empty reports that purging left nothing (no
// cells and no row deletion), so the row can be dropped. If nothing is
// purged, it returns v itself, and changed and empty are false.
func Purge(dst, v []byte, nowSec, gcBefore int64, maxPurgeable func(minTS, maxTS int64) (int64, error)) (out []byte, changed, empty bool, err error) {
	d := newDecoder(v)
	candidate := func(ldt int64) bool { return ldt < gcBefore && ldt <= nowSec }
	cand := false
	minTS, maxTS := int64(math.MaxInt64), int64(math.MinInt64)
	note := func(ts int64) { cand, minTS, maxTS = true, min(minTS, ts), max(maxTS, ts) }
	if !d.deletion.IsLive() && candidate(d.deletion.LocalDeletionTime) {
		note(d.deletion.MarkedForDeleteAt)
	}
	var c Cell
	for d.next(&c) {
		if c.LocalDeletionTime != NoDeletionTime && !c.IsLive(nowSec) && candidate(c.LocalDeletionTime) {
			note(c.Timestamp)
		}
	}
	if d.err != nil {
		return nil, false, false, d.err
	}
	if !cand {
		return v, false, false, nil
	}
	limit, err := maxPurgeable(minTS, maxTS)
	if err != nil {
		return nil, false, false, err
	}
	shouldPurge := func(ts, ldt int64) bool { return candidate(ldt) && ts < limit }
	r, err := Decode(v)
	if err != nil {
		return nil, false, false, err
	}
	if !r.Deletion.IsLive() && shouldPurge(r.Deletion.MarkedForDeleteAt, r.Deletion.LocalDeletionTime) {
		r.Deletion = Deletion{MarkedForDeleteAt: NoTimestamp}
		changed = true
	}
	kept := r.Cells[:0]
	for _, c := range r.Cells {
		if c.LocalDeletionTime != NoDeletionTime && !c.IsLive(nowSec) && shouldPurge(c.Timestamp, c.LocalDeletionTime) {
			changed = true
			continue
		}
		kept = append(kept, c)
	}
	r.Cells = kept
	if !changed {
		return v, false, false, nil
	}
	empty = len(r.Cells) == 0 && r.Deletion.IsLive()
	if empty {
		return dst, true, true, nil
	}
	return Append(dst, r), true, false, nil
}

// Summarize implements lsm.Purger.
func (Merger) Summarize(v []byte, ldt func(int64)) (Stats, error) { return Summarize(v, ldt) }

// Purge implements lsm.Purger.
func (Merger) Purge(dst, v []byte, nowSec, gcBefore int64, maxPurgeable func(minTS, maxTS int64) (int64, error)) ([]byte, bool, bool, error) {
	return Purge(dst, v, nowSec, gcBefore, maxPurgeable)
}
