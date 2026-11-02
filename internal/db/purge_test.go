package db

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestPurgeDeletions checks that compaction drops the rows partition
// deletions and range tombstones shadow, and the deletions themselves once
// gc_grace_seconds has passed.
func TestPurgeDeletions(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	d, err := Open("db", Options{FS: vfs.NewMem(), Now: func() time.Time { return time.Unix(0, clock.Load()) }})
	require.NoError(t, err)
	defer d.Close()
	s := &session{t: t, d: d}
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (k int, c int, v text, s text STATIC, PRIMARY KEY (k, c)) WITH gc_grace_seconds = 3600")
	for k := 1; k <= 3; k++ {
		s.ok(fmt.Sprintf("INSERT INTO ks.t (k, c, s) VALUES (%d, 0, 'static')", k))
		for c := 0; c < 50; c++ {
			s.ok(fmt.Sprintf("INSERT INTO ks.t (k, c, v) VALUES (%d, %d, 'v')", k, c))
		}
	}
	tbl := d.schema.Load().Keyspaces["ks"].Tables["t"]
	lt, err := d.engine.Table(tbl.Storage)
	require.NoError(t, err)
	require.NoError(t, lt.Flush())
	clock.Add(int64(time.Second))
	s.ok("DELETE FROM ks.t WHERE k = 1")
	s.ok("DELETE FROM ks.t WHERE k = 2 AND c >= 10 AND c < 20")
	clock.Add(int64(time.Second))
	s.ok("INSERT INTO ks.t (k, c, v) VALUES (1, 7, 'after')")
	require.NoError(t, lt.Flush())

	keys := func(k int) (rows, other int) {
		p := partitionPrefix([]byte{0, 0, 0, byte(k)})
		it, err := lt.NewIter(p, prefixEnd(p))
		require.NoError(t, err)
		defer it.Close()
		for ok := it.First(); ok; ok = it.Next() {
			if it.Key()[len(p)] == headerSuffix || it.Key()[len(p)] == tombstoneSuffix {
				other++
			} else {
				rows++
			}
		}
		return
	}
	check := func() {
		require.Equal(t, []string{"1 7 null after"}, s.rows("SELECT k, c, s, v FROM ks.t WHERE k = 1"))
		require.Len(t, s.rows("SELECT c FROM ks.t WHERE k = 2"), 40)
		require.Len(t, s.rows("SELECT c FROM ks.t WHERE k = 3"), 50)
	}
	check()

	// Within gc_grace: the shadowed rows go, the deletions stay.
	require.NoError(t, lt.Compact())
	rows, other := keys(1)
	require.Equal(t, 1, rows)
	require.Equal(t, 1, other, "the partition deletion is kept")
	rows, other = keys(2)
	require.Equal(t, 40, rows)
	require.Equal(t, 2, other, "header and range tombstone")
	check()

	// After gc_grace: the deletions go too.
	clock.Add(int64(2 * time.Hour))
	require.NoError(t, lt.Compact())
	rows, other = keys(1)
	require.Equal(t, 1, rows)
	require.Equal(t, 0, other)
	rows, other = keys(2)
	require.Equal(t, 40, rows)
	require.Equal(t, 1, other, "only the static row remains")
	check()
}

// TestPurgeKeepsDeletionOverOlderData checks that an expired partition
// deletion is kept while data it shadows exists outside the compaction.
func TestPurgeKeepsDeletionOverOlderData(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	d, err := Open("db", Options{FS: vfs.NewMem(), Now: func() time.Time { return time.Unix(0, clock.Load()) }})
	require.NoError(t, err)
	defer d.Close()
	s := &session{t: t, d: d}
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (k int, c int, v text, PRIMARY KEY (k, c)) WITH gc_grace_seconds = 60")
	s.ok("DELETE FROM ks.t USING TIMESTAMP 1000 WHERE k = 1")
	lt, err := d.engine.Table(d.schema.Load().Keyspaces["ks"].Tables["t"].Storage)
	require.NoError(t, err)
	require.NoError(t, lt.Flush())
	// A late write older than the deletion, still in the memtable.
	s.ok("INSERT INTO ks.t (k, c, v) VALUES (1, 1, 'old') USING TIMESTAMP 500")
	clock.Add(int64(time.Hour))
	require.NoError(t, lt.Compact())
	require.Empty(t, s.rows("SELECT c FROM ks.t WHERE k = 1"), "the deletion still shadows the old write")
	// Once the old write is in an sstable compacted with the deletion, both go.
	require.NoError(t, lt.Flush())
	require.NoError(t, lt.Compact())
	require.Empty(t, s.rows("SELECT c FROM ks.t WHERE k = 1"))
	p := partitionPrefix([]byte{0, 0, 0, 1})
	it, err := lt.NewIter(p, prefixEnd(p))
	require.NoError(t, err)
	defer it.Close()
	require.False(t, it.First(), "nothing left of the partition")
}
