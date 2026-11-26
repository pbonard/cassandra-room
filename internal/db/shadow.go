package db

import (
	"bytes"
	"errors"
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

// shadower filters a replica's rows by their partitions' deletions, one
// partition's deletions at a time.
type shadower struct {
	lt     *lsm.Table
	prefix []byte
	dels   deletions
	loaded bool
}

// filter returns a row as a replica answers it: with the cells shadowed by
// its partition's deletions dropped (nil if nothing is left). Headers,
// range tombstones and rows of tables without clustering columns are
// returned as they are.
func (s *shadower) filter(key, value []byte) ([]byte, error) {
	_, _, rest, err := parsePartition(key)
	if err != nil || len(rest) == 0 || rest[0] == headerSuffix || rest[0] == tombstoneSuffix {
		return value, nil
	}
	prefix := key[:len(key)-len(rest)]
	if !s.loaded || !bytes.Equal(prefix, s.prefix) {
		if err := s.load(prefix); err != nil {
			return nil, err
		}
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

// load reads a partition's deletion and range tombstones.
func (s *shadower) load(prefix []byte) error {
	s.prefix = append(s.prefix[:0], prefix...)
	s.dels = deletions{partition: row.NoTimestamp}
	s.loaded = true
	var static []row.Cell
	header := append(slices.Clip(prefix), headerSuffix)
	v, err := s.lt.Get(header)
	switch {
	case errors.Is(err, lsm.ErrNotFound):
	case err != nil:
		return err
	default:
		if err := addPartitionEntry([]byte{headerSuffix}, v, &static, &s.dels); err != nil {
			return err
		}
	}
	lo := append(slices.Clip(prefix), tombstoneSuffix)
	it, err := s.lt.NewIter(lo, append(slices.Clip(prefix), tombstoneSuffix+1))
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if err := addPartitionEntry(it.Key()[len(prefix):], it.Value(), &static, &s.dels); err != nil {
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
