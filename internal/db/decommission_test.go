package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room/internal/cluster"
)

// TestDecommissionNeedsTargetsUp: a node does not leave while nodes that
// would take over its ranges are down (it used to skip streaming to them
// and leave anyway: with one replica, the data was gone). Once they are
// back, it leaves and every row is still there.
func TestDecommissionNeedsTargetsUp(t *testing.T) {
	c := newTestCluster(t, 4)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	for i := 0; i < 200; i++ {
		c.ok(i%4, one, "INSERT INTO ks.kv (k, v) VALUES (?, 'x')", i)
	}
	c.stop(1)
	c.stop(2)
	require.Eventually(t, func() bool {
		return !c.nodes[3].proxy.g.IsAlive(c.addrs[1]) && !c.nodes[3].proxy.g.IsAlive(c.addrs[2])
	}, 10*time.Second, 10*time.Millisecond)
	err := c.nodes[3].Decommission()
	require.ErrorContains(t, err, "are down")
	require.Equal(t, cluster.StatusNormal, c.nodes[0].schema.Load().Nodes[c.nodes[3].local.HostID].Status, "it did not start leaving")

	c.start(1)
	c.start(2)
	c.waitConverged()
	require.NoError(t, c.nodes[3].Decommission())
	require.Equal(t, cluster.StatusLeft, c.nodes[0].schema.Load().Nodes[c.nodes[3].local.HostID].Status)
	c.stop(3)
	require.Len(t, c.values(0, one, "SELECT k FROM ks.kv"), 200)
}
