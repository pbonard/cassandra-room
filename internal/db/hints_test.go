package db

import (
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// TestHintForDroppedTable: a hint for a table dropped since is discarded,
// and the hints after it are delivered. The replica refused it, the replay
// stopped at it for that host, and kept it: every later hint to the host
// waited for it to expire, for gc_grace_seconds.
func TestHintForDroppedTable(t *testing.T) {
	c := newTestCluster(t, 2)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.gone (k int PRIMARY KEY, v text)")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(100 * time.Millisecond)
	c.stop(1)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[1]) }, 5*time.Second, 10*time.Millisecond)
	c.ok(0, one, "INSERT INTO ks.gone (k, v) VALUES (1, 'poison')")
	c.ok(0, one, "INSERT INTO ks.kv (k, v) VALUES (1, 'hinted')")
	c.ok(0, one, "DROP TABLE ks.gone")
	c.start(1)
	c.waitConverged()
	local := func() int {
		res := c.nodes[1].Execute(mustPrepare(t, c.nodes[1], "SELECT k FROM ks.kv WHERE k = 1"), &RequestOptions{Consistency: one, local: true})
		return len(res.(*message.RowsResult).Data)
	}
	require.Eventually(t, func() bool { c.nodes[0].proxy.replayHints(); return local() == 1 }, 10*time.Second, 100*time.Millisecond,
		"the hint after the dropped table's is delivered")
	require.Eventually(t, func() bool {
		c.nodes[0].proxy.replayHints()
		lt, err := c.nodes[0].engine.Table(hintsStorage)
		require.NoError(t, err)
		it, err := lt.NewIter(nil, nil)
		require.NoError(t, err)
		defer it.Close()
		n := 0
		for ok := it.First(); ok; ok = it.Next() {
			if r, err := row.Decode(it.Value()); err == nil && len(r.Live(time.Now().Unix())) > 0 {
				n++
			}
		}
		return n == 0
	}, 10*time.Second, 100*time.Millisecond, "no hint is left")
}

// TestConsistencyAnyNeedsAHint: a write at ANY whose replicas are all down
// succeeds only if a hint was stored; with hints disabled, it fails.
func TestConsistencyAnyNeedsAHint(t *testing.T) {
	c := &testCluster{t: t, tweak: func(o *ClusterOptions) { o.DisableHints = true }}
	for i := 0; i < 2; i++ {
		c.add()
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, d := range c.nodes {
			if d != nil {
				d.Close()
			}
		}
	})
	c.waitConverged()
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	k := c.nodes[0].schema.Load().Keyspaces["ks"]
	key := -1
	for i := 0; i < 1000 && key < 0; i++ {
		pk := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if c.nodes[0].proxy.ring.Load().Replicas(k.Replication, Token(pk))[0].Addr == c.addrs[1] {
			key = i
		}
	}
	require.GreaterOrEqual(t, key, 0)
	c.stop(1)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[1]) }, 10*time.Second, 10*time.Millisecond)
	res := c.exec(0, primitive.ConsistencyLevelAny, "INSERT INTO ks.kv (k, v) VALUES (?, 'x')", key)
	_, failed := res.(message.Error)
	require.True(t, failed, "no replica and no hint: the write is nowhere")
}
