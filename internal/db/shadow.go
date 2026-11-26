package db

import (
	"bytes"
	"slices"

	"github.com/pbonard/cassandra-room/internal/storage/lsm"
	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// Replicas answer reads with the rows of a partition less what the
// partition's deletion and range tombstones in their data shadow (as a
// Cassandra replica's read applies its tombstones). The coordinator would
// drop those cells anyway, but read repair compares and copies rows as
// they come: a row shadowed on a replica that has not compacted it away,
// missing from one that has, was copied to the latter without the
// deletion, which, once that replica had purged the deletion
// (gc_grace_seconds), resurrected it. Digests of the same data then also
// match whether or not a replica compacted it.
//
// Finding a partition's deletions costs nothing on forward reads that
// start at or before its header (P | 0x10): the header and the range
// tombstones (P | 0x18 ...) sort before its rows, so the scan passes them
// first, and a partition whose first key is a row has none. Other reads
// (reverse ones, and pages that start within a partition) read them with
// one scan of [P | 0x10, P | 0x19): nothing sorts between the two.

// shadower filters a replica's rows by their partitions' deletions, one
// partition's deletions at a time.
type shadower struct {
	lt *lsm.Table
	// forward with lo: the read scans forward from lo, so it passes the
	// header and range tombstones of every partition whose header is at
	// or after lo.
	forward bool
	lo      []byte

	prefix []byte
	dels   deletions
	loaded bool
	// inline: the current partition's deletions are taken from the scan.
	inline bool
	// loads counts the partitions whose deletions were read (tests).
	loads int
}

// newShadower returns the shadower of a read of table id from lt, or nil
// if the table has no clustering columns (its rows are whole partitions,
// with no partition deletion or range tombstone apart from them).
func (p *proxy) newShadower(id lsm.TableID, lt *lsm.Table, forward bool, lo []byte) *shadower {
	if t := p.d.schema.Load().byStorage[id]; t != nil && len(t.Clustering) == 0 {
		return nil
	}
	return &shadower{lt: lt, forward: forward, lo: lo}
}

// filter returns a row as a replica answers it: with the cells shadowed by
// its partition's deletions dropped (nil if nothing is left). Headers,
// range tombstones and rows of tables without clustering columns are
// returned as they are. A nil shadower filters nothing.
func (s *shadower) filter(key, value []byte) ([]byte, error) {
	if s == nil {
		return value, nil
	}
	n := partitionPrefixLen(key)
	if n >= len(key) {
		return value, nil // a partition's only row: no clustering
	}
	prefix, rest := key[:n], key[n:]
	if !s.loaded || !bytes.Equal(prefix, s.prefix) {
		if err := s.enter(prefix); err != nil {
			return nil, err
		}
	}
	if rest[0] == headerSuffix || rest[0] == tombstoneSuffix {
		if s.inline {
			var static []row.Cell
			if err := addPartitionEntry(rest, value, &static, &s.dels); err != nil {
				return nil, err
			}
		}
		return value, nil
	}
	at := s.dels.deletedAt(rest)
	if at == row.NoTimestamp {
		return value, nil
	}
	r, err := row.Decode(value)
	if err != nil {
		return nil, err
	}
	out, drop, err := shadowRow(r, at)
	switch {
	case err != nil:
		return nil, err
	case drop:
		return nil, nil
	case out == nil:
		return value, nil
	}
	return out, nil
}

// enter starts a partition: its deletions are those the scan passes, or
// are read.
func (s *shadower) enter(prefix []byte) error {
	s.prefix = append(s.prefix[:0], prefix...)
	s.dels = deletions{partition: row.NoTimestamp}
	s.loaded = true
	s.inline = s.forward && atOrBeforeHeader(s.lo, s.prefix)
	if s.inline {
		return nil
	}
	return s.load()
}

// atOrBeforeHeader reports whether lo <= prefix | headerSuffix, without
// building that key.
func atOrBeforeHeader(lo, prefix []byte) bool {
	n := min(len(lo), len(prefix))
	if c := bytes.Compare(lo[:n], prefix[:n]); c != 0 {
		return c < 0
	}
	switch {
	case len(lo) <= len(prefix):
		return true // lo is a prefix of P (or P): below P | 0x10
	case lo[len(prefix)] != headerSuffix:
		return lo[len(prefix)] < headerSuffix
	}
	return len(lo) == len(prefix)+1
}

// load reads the current partition's deletion and range tombstones.
func (s *shadower) load() error {
	s.loads++
	var static []row.Cell
	it, err := s.lt.NewIter(append(slices.Clip(s.prefix), headerSuffix), append(slices.Clip(s.prefix), tombstoneSuffix+1))
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		suffix := it.Key()[len(s.prefix):]
		if suffix[0] == headerSuffix && len(suffix) != 1 {
			continue // not a header (no such key is written)
		}
		if err := addPartitionEntry(suffix, it.Value(), &static, &s.dels); err != nil {
			return err
		}
	}
	return it.Error()
}

// shadowRow drops from r what a deletion at ts shadows; out is nil when
// nothing changed, drop when nothing is left.
func shadowRow(r *row.Row, at int64) (out []byte, drop bool, err error) {
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
