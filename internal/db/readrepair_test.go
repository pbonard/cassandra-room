package db

import (
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
)

// TestReadRepairBlocking: a read that finds replicas disagreeing repairs
// them before returning, and fails (timeout) if it cannot: a later read at
// the same consistency must see at least as much. read_repair 'NONE'
// leaves them as they are.
func TestReadRepairBlocking(t *testing.T) {
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
	c.ok(0, quorum, "CREATE TABLE ks.nr (k int PRIMARY KEY, v text) WITH read_repair = 'NONE'")
	time.Sleep(100 * time.Millisecond)
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) }, 10*time.Second, 10*time.Millisecond)
	c.ok(0, quorum, "INSERT INTO ks.kv (k, v) VALUES (1, 'x')")
	c.ok(0, quorum, "INSERT INTO ks.nr (k, v) VALUES (1, 'x')")
	c.start(2)
	c.waitConverged()
	local := func(q string) int {
		res := c.nodes[2].Execute(mustPrepare(t, c.nodes[2], q), &RequestOptions{Consistency: one, local: true})
		return len(res.(*message.RowsResult).Data)
	}
	require.Zero(t, local("SELECT v FROM ks.kv WHERE k = 1"), "node 2 missed the write")

	// The repair cannot be applied: the read fails.
	yes, no := true, false
	c.nodes[2].proxy.testRefuseMutations.Store(&yes)
	_, failed := c.exec(0, all, "SELECT v FROM ks.kv WHERE k = 1").(message.Error)
	require.True(t, failed, "a read whose repair failed does not succeed")
	c.nodes[2].proxy.testRefuseMutations.Store(&no)
	require.Equal(t, []string{"x"}, c.values(0, all, "SELECT v FROM ks.kv WHERE k = 1"))
	require.Equal(t, 1, local("SELECT v FROM ks.kv WHERE k = 1"), "repaired")

	// read_repair 'NONE': the read answers, node 2 stays behind.
	require.Equal(t, []string{"x"}, c.values(0, all, "SELECT v FROM ks.nr WHERE k = 1"))
	require.Zero(t, local("SELECT v FROM ks.nr WHERE k = 1"), "not repaired")
}
