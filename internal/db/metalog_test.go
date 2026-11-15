package db

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/stretchr/testify/require"
)

// TestMetadataDelta checks that an epoch's entry holds only what changed
// and rebuilds the same metadata, and that applying it to other metadata
// is detected.
func TestMetadataDelta(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.a (k int PRIMARY KEY, v int)")
	s.ok("CREATE TABLE ks.b (k int PRIMARY KEY, v int)")
	before := s.d.schema.Load()
	prev := metadataBytes(before)

	for _, q := range []string{"ALTER TABLE ks.b ADD w text", "DROP TABLE ks.a", "CREATE ROLE r WITH LOGIN = true"} {
		s.ok(q)
		after := s.d.schema.Load()
		value, err := diffMetadata(prev, after)
		require.NoError(t, err)
		var d metadataDelta
		require.NoError(t, json.Unmarshal(value, &d))
		total := 0
		for _, c := range d.Changes {
			total += len(c)
		}
		require.Equal(t, 1, total, "%s: one element changed: %v", q, d.Changes)
		require.Less(t, len(value), len(metadataBytes(after)))

		base, _, err := parseSchemaFile(prev)
		require.NoError(t, err)
		next, err := applyDelta(base, value)
		require.NoError(t, err)
		require.Equal(t, string(metadataBytes(after)), string(metadataBytes(next)), q)
		prev = metadataBytes(after)
	}
	require.Contains(t, string(mustDiff(t, metadataBytes(before), s.d.schema.Load())), `"Tables":{"ks\u0000a":null`)

	// The same entry on metadata that differs from the proposer's.
	base, _, err := parseSchemaFile(metadataBytes(before))
	require.NoError(t, err)
	base.Epoch = s.d.schema.Load().Epoch - 1
	_, err = applyDelta(base, mustDiff(t, metadataBytes(before), s.d.schema.Load()))
	require.ErrorIs(t, err, errDiverged)
}

func mustDiff(t *testing.T, prev []byte, next *Schema) []byte {
	b, err := diffMetadata(prev, next)
	require.NoError(t, err)
	return b
}

// TestMetadataLogReplay checks that a node replays at startup the entries
// it logged but did not install before stopping.
func TestMetadataLogReplay(t *testing.T) {
	src := openTest(t, nil)
	fs := vfs.NewMem()
	d, err := Open("db", Options{FS: fs})
	require.NoError(t, err)
	require.Equal(t, string(metadataBytes(src.d.schema.Load())), string(metadataBytes(d.schema.Load())))

	var entries []cluster.Entry
	prev := metadataBytes(src.d.schema.Load())
	for _, q := range []string{
		"CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		"CREATE TABLE ks.t (k int PRIMARY KEY, v int)",
	} {
		src.ok(q)
		s := src.d.schema.Load()
		entries = append(entries, cluster.Entry{Epoch: s.Epoch, Value: mustDiff(t, prev, s)})
		prev = metadataBytes(s)
	}
	// The first entry was installed, the second only logged.
	d.ddlMu.Lock()
	require.NoError(t, d.appendLocked(entries[0]))
	d.metaLog = append(d.metaLog, entries[1])
	require.NoError(t, d.saveMetadataLog())
	d.ddlMu.Unlock()
	require.Equal(t, int64(1), d.schema.Load().Epoch)
	require.NoError(t, d.Close())

	d, err = Open("db", Options{FS: fs})
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.replayMetadataLog())
	s := d.schema.Load()
	require.Equal(t, int64(2), s.Epoch)
	require.NotNil(t, s.Keyspaces["ks"].Tables["t"])
	require.Equal(t, string(metadataBytes(src.d.schema.Load())), string(metadataBytes(s)))
	_, err = d.engine.Table(s.Keyspaces["ks"].Tables["t"].Storage)
	require.NoError(t, err, "storage created by the replayed entry")
	require.Len(t, d.metaLog, 2)
}

func TestCMSStep(t *testing.T) {
	var steps []string
	cur := []string{"a", "b", "c"}
	for {
		step, next, ok := cmsStep(cur, []string{"c", "d", "e"})
		if !ok {
			break
		}
		steps = append(steps, step)
		cur = next
	}
	require.Equal(t, []string{"+d", "+e", "-a", "-b"}, steps)
	require.Equal(t, []string{"c", "d", "e"}, cur)
}
