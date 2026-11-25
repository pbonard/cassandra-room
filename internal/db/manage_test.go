package db

import (
	"math"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

// TestClusterManagement: the operations of the management API on a
// cluster: node and ring information, range repairs covering the ring in
// segments, and a restore by snapshot, truncate and import.
func TestClusterManagement(t *testing.T) {
	c := &testCluster{t: t, tweak: func(o *ClusterOptions) { o.DisableHints = true }}
	for i := 0; i < 3; i++ {
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
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")

	nodes := c.nodes[0].Nodes()
	require.Len(t, nodes, 3)
	self := 0
	for _, n := range nodes {
		require.Equal(t, "NORMAL", n.Status)
		require.True(t, n.Up)
		if n.Self {
			self++
		}
	}
	require.Equal(t, 1, self)
	require.True(t, c.nodes[1].Ready())
	ring, err := c.nodes[0].Ring("ks")
	require.NoError(t, err)
	require.Len(t, ring, 24) // 3 nodes × 8 tokens
	for _, r := range ring {
		require.Len(t, r.Replicas, 3)
	}

	// Node 2 misses writes (no hints); range repairs in four segments
	// covering the ring bring it back.
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) }, 5*time.Second, 10*time.Millisecond)
	for i := 0; i < 20; i++ {
		c.ok(i%2, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, 'x')", i)
	}
	c.start(2)
	c.waitConverged()
	q := int64(math.MaxInt64 / 2)
	segments := [][2]int64{{math.MinInt64, -q}, {-q, 0}, {0, q}, {q, math.MinInt64}} // the last wraps
	repaired := 0
	for _, s := range segments {
		st, err := c.nodes[2].RepairRange("ks", "kv", s[0], s[1])
		require.NoError(t, err)
		repaired += st.Ranges
	}
	require.Positive(t, repaired)
	local := func(i int) int {
		res := c.nodes[i].Execute(mustPrepare(t, c.nodes[i], "SELECT k FROM ks.kv"), &RequestOptions{Consistency: one, local: true})
		return len(rowsOf(t, res))
	}
	require.Equal(t, 20, local(2), "repaired")
	_, err = c.nodes[2].RepairRange("ks", "nope", 0, 1)
	require.Error(t, err)

	// Restore: snapshot every node, truncate (new storage on every node),
	// import each node's own snapshot.
	snaps := map[int][]SnapshotTable{}
	for i, d := range c.nodes {
		require.NoError(t, d.Flush())
		s, err := d.Snapshot("b1", []string{"ks"})
		require.NoError(t, err)
		require.Len(t, s, 1)
		require.Equal(t, "kv", s[0].Table)
		snaps[i] = s
	}
	c.ok(0, primitive.ConsistencyLevelAll, "TRUNCATE ks.kv")
	require.Eventually(t, func() bool { return local(0) == 0 && local(1) == 0 && local(2) == 0 }, 5*time.Second, 20*time.Millisecond)
	for i, d := range c.nodes {
		require.NoError(t, d.Import("ks", "kv", snaps[i][0].Files))
	}
	for i := range c.nodes {
		require.Equal(t, 20, local(i), "node %d restored", i)
		require.NoError(t, c.nodes[i].ClearSnapshot("b1"))
	}
	require.Len(t, c.values(1, primitive.ConsistencyLevelAll, "SELECT k FROM ks.kv"), 20)
}
