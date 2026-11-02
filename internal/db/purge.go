package db

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
)

// Purging partition deletions and range tombstones. Both are stored as
// rows of their own (the partition header and the tombstone rows) whose
// cells are live, so the storage engine, which merges and purges row by
// row, keeps them forever. A compaction filter does what Cassandra's
// compaction iterator does for them, partition by partition, in key order:
//
//   - rows, static cells and range tombstones shadowed by the partition
//     deletion or by a range tombstone are dropped (their data can never
//     be read again);
//   - a deletion older than gc_grace_seconds is dropped once no data it
//     could shadow survives outside the compaction (the smallest timestamp
//     of the partition's, or the range's, rows in other sstables and the
//     memtables is newer than the deletion).
//
// A partition never spans two compaction shards (shards split at token
// boundaries), so one filter sees all of a partition's rows a compaction
// writes.

type deletionFilter struct {
	prefix    []byte // the current partition's prefix P
	partition int64  // its deletion's timestamp, or NoTimestamp
	ranges    []rangeTombstone
}

func newDeletionFilter() lsm.RowFilter { return &deletionFilter{partition: row.NoTimestamp} }

// deletionTime returns the local deletion time stored in a deletion cell.
func deletionTime(c *row.Cell) int64 {
	if len(c.Value) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(c.Value))
}

func (f *deletionFilter) Filter(key, value []byte, env *lsm.FilterEnv) ([]byte, bool, error) {
	_, _, rest, err := parsePartition(key)
	if err != nil || len(rest) == 0 {
		return nil, false, nil // not a clustered row: leave it
	}
	prefix := key[:len(key)-len(rest)]
	if !bytes.Equal(prefix, f.prefix) {
		f.prefix = append(f.prefix[:0], prefix...)
		f.partition, f.ranges = row.NoTimestamp, f.ranges[:0]
	}
	// purgeable reports whether a deletion at ts, deleted at ldt, of the
	// keys in [lo, hi) may be dropped.
	purgeable := func(ts, ldt int64, lo, hi []byte) (bool, error) {
		if ldt >= env.GCBefore || ldt > env.NowSec {
			return false, nil
		}
		limit, err := env.MaxPurgeable(lo, hi, ts, ts)
		return ts < limit, err
	}
	r, err := row.Decode(value)
	if err != nil {
		return nil, false, err
	}
	switch rest[0] {
	case headerSuffix:
		del := -1
		for i := range r.Cells {
			if r.Cells[i].Column == deletionColumn {
				f.partition, del = r.Cells[i].Timestamp, i
			}
		}
		if del < 0 {
			return nil, false, nil
		}
		changed := false
		kept := r.Cells[:0]
		for i, c := range r.Cells {
			if i != del && c.Timestamp <= f.partition {
				changed = true // a static cell the deletion shadows
				continue
			}
			kept = append(kept, c)
		}
		r.Cells = kept
		c := r.Cells[slices.IndexFunc(r.Cells, func(c row.Cell) bool { return c.Column == deletionColumn })]
		ok, err := purgeable(c.Timestamp, deletionTime(&c), prefix, prefixEnd(prefix))
		if err != nil {
			return nil, false, err
		}
		if ok {
			r.Cells = slices.DeleteFunc(r.Cells, func(c row.Cell) bool { return c.Column == deletionColumn })
			changed = true
		}
		return rewrite(r, changed)
	case tombstoneSuffix:
		var rt rangeTombstone
		var ldt int64
		for i := range r.Cells {
			switch c := &r.Cells[i]; c.Column {
			case rtStartColumn:
				rt.start = slices.Clone(c.Value)
			case rtEndColumn:
				rt.end = slices.Clone(c.Value)
			case deletionColumn:
				rt.ts, ldt = c.Timestamp, deletionTime(c)
			}
		}
		if rt.ts <= f.partition {
			return nil, true, nil // shadowed by the partition deletion
		}
		f.ranges = append(f.ranges, rt)
		lo := append(slices.Clip(prefix), rt.start...)
		hi := append(slices.Clip(prefix), rt.end...)
		ok, err := purgeable(rt.ts, ldt, lo, hi)
		return nil, ok, err
	}
	// A row: drop what the partition deletion and the range tombstones
	// covering it shadow.
	d := deletions{partition: f.partition, ranges: f.ranges}
	at := d.deletedAt(rest)
	if at == row.NoTimestamp {
		return nil, false, nil
	}
	changed := false
	if !r.Deletion.IsLive() && r.Deletion.MarkedForDeleteAt <= at {
		r.Deletion = row.Deletion{MarkedForDeleteAt: row.NoTimestamp}
		changed = true
	}
	kept := r.Cells[:0]
	for _, c := range r.Cells {
		if c.Timestamp <= at {
			changed = true
			continue
		}
		kept = append(kept, c)
	}
	r.Cells = kept
	return rewrite(r, changed)
}

func rewrite(r *row.Row, changed bool) ([]byte, bool, error) {
	if !changed {
		return nil, false, nil
	}
	if len(r.Cells) == 0 && r.Deletion.IsLive() {
		return nil, true, nil
	}
	return row.Append(nil, r), false, nil
}
