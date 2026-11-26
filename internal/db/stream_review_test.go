package db

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAbandonedStreamExpires: the files of an outbound stream session
// whose receiver never finished (it crashed) are removed once the session
// has been idle long enough, not only at the next restart.
func TestAbandonedStreamExpires(t *testing.T) {
	c := newTestCluster(t, 1)
	c.ok(0, one, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, one, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	c.ok(0, one, "INSERT INTO ks.kv (k, v) VALUES (1, 'x')")
	d := c.nodes[0]
	require.NoError(t, d.engine.Flush())
	tbl := d.schema.Load().Keyspaces["ks"].Tables["kv"]
	prepare := func(session string) {
		b, _ := json.Marshal(streamRequest{Op: "prepare", Session: session, Table: uint64(tbl.Storage)})
		_, err := d.proxy.handleStreamFile(context.Background(), "", b)
		require.NoError(t, err)
	}
	exists := func(session string) bool {
		_, err := d.fs.Stat(d.streamDir("out", session))
		return err == nil
	}
	prepare("aa")
	require.True(t, exists("aa"))
	d.proxy.streamMu.Lock()
	d.proxy.streamUsed["aa"] = time.Now().Add(-2 * streamIdle)
	d.proxy.streamMu.Unlock()
	prepare("bb")
	require.False(t, exists("aa"), "the abandoned session's files are gone")
	require.True(t, exists("bb"))
}
