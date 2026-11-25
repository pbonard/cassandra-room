package db

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestPointInTimeRestore: a backup (snapshot), then writes at known
// timestamps; the table is truncated, the snapshot imported, and the
// archived commit log replayed up to a point in time between the writes.
func TestPointInTimeRestore(t *testing.T) {
	o := Options{FS: vfs.NewMem()}
	o.Engine.WALArchiveDir = "archive"
	d, err := Open("db", o)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	s := &session{t: t, d: d}
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (p int, c int, v text, PRIMARY KEY (p, c))")
	s.ok("INSERT INTO ks.t (p, c, v) VALUES (1, 1, 'backup') USING TIMESTAMP 100")
	s.ok("INSERT INTO ks.t (p, c, v) VALUES (1, 2, 'backup') USING TIMESTAMP 100")
	snap, err := d.Snapshot("b", []string{"ks"})
	require.NoError(t, err)
	require.Len(t, snap, 1)
	require.NotZero(t, snap[0].Storage)

	// After the backup: before the point in time (200)...
	s.ok("INSERT INTO ks.t (p, c, v) VALUES (1, 3, 'before') USING TIMESTAMP 150")
	s.ok("UPDATE ks.t USING TIMESTAMP 160 SET v = 'updated' WHERE p = 1 AND c = 1")
	s.ok("DELETE FROM ks.t USING TIMESTAMP 170 WHERE p = 1 AND c = 2")
	// ...and after it.
	s.ok("INSERT INTO ks.t (p, c, v) VALUES (1, 4, 'after') USING TIMESTAMP 250")
	s.ok("UPDATE ks.t USING TIMESTAMP 260 SET v = 'later' WHERE p = 1 AND c = 3")
	s.ok("DELETE FROM ks.t USING TIMESTAMP 270 WHERE p = 1 AND c >= 1")
	require.Empty(t, s.rows("SELECT c, v FROM ks.t WHERE p = 1"))
	segs, err := d.ArchiveCommitLog()
	require.NoError(t, err)
	require.NotEmpty(t, segs)

	// Restore: truncate (new storage), import the backup, replay to 200.
	s.ok("TRUNCATE ks.t")
	require.NoError(t, d.Import("ks", "t", snap[0].Files))
	tables := map[string]string{strconv.FormatUint(snap[0].Storage, 10): "ks.t"}
	res, err := d.Replay(ReplayRequest{Files: segs, Tables: tables, Until: 200})
	require.NoError(t, err)
	require.Positive(t, res.Applied)
	require.Positive(t, res.Skipped)
	require.Equal(t, []string{"1 updated", "3 before"}, s.rows("SELECT c, v FROM ks.t WHERE p = 1"))

	// Replaying everything reaches the latest state; the TRUNCATE's new
	// storage is not in the map, so nothing written since comes back.
	_, err = d.Replay(ReplayRequest{Files: segs, Tables: tables})
	require.NoError(t, err)
	require.Empty(t, s.rows("SELECT c, v FROM ks.t WHERE p = 1"))

	_, err = d.Replay(ReplayRequest{Files: segs, Tables: map[string]string{"x": "ks.t"}})
	require.Error(t, err)
	off := openTest(t, nil)
	_, err = off.d.ArchiveCommitLog()
	require.ErrorContains(t, err, "archiving is off")
}
