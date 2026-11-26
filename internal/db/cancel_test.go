package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room/internal/cluster"
)

// addDeadNode commits a node in a range movement that is not running (it
// died, for good, mid-movement).
func addDeadNode(t *testing.T, c *testCluster, status string) *cluster.Node {
	n := &cluster.Node{Addr: freeAddr(t), HostID: fmt.Sprintf("dead-%d", time.Now().UnixNano()), DataCenter: "datacenter1", Rack: "rack1",
		Tokens: cluster.RandomTokens(8), Status: status, ReleaseVersion: ReleaseVersion}
	_, err := c.nodes[0].change("REGISTER", func(next *Schema) (message.Message, bool, error) {
		m := *n
		m.Ordinal = cluster.NextOrdinal(next.Nodes)
		next.Nodes[m.HostID] = &m
		next.logDetail = fmt.Sprintf("%s %s as %s", m.HostID, m.Addr, m.Status)
		return nil, true, nil
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, d := range c.nodes {
			if d != nil && d.schema.Load().Nodes[n.HostID] == nil {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	return n
}

// TestClusterCancelJoin: a node that died for good while joining no longer
// wedges every other movement: removenode refuses it (it would make it an
// owner reads go to), and cancelling its join, from JOINING in one commit,
// from MID_JOIN back through JOINING, leaves it LEFT; another node then
// joins, and the data is where it should be.
func TestClusterCancelJoin(t *testing.T) {
	for _, status := range []string{cluster.StatusJoining, cluster.StatusMidJoin} {
		t.Run(status, func(t *testing.T) {
			c := newTestCluster(t, 3)
			c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
			c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
			time.Sleep(100 * time.Millisecond)
			for i := 0; i < 100; i++ {
				c.ok(i%3, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
			}
			dead := addDeadNode(t, c, status)

			require.Error(t, c.nodes[1].Decommission(), "wedged")
			require.ErrorIs(t, c.nodes[1].proxy.startMovement("PREPARE_LEAVE", c.nodes[1].local.HostID, cluster.StatusLeaving), errRangeMovement)
			err := c.nodes[0].RemoveNode(dead.HostID)
			require.ErrorContains(t, err, "cancel its join")
			require.Equal(t, status, c.nodes[0].schema.Load().Nodes[dead.HostID].Status, "removenode left it alone")
			require.ErrorContains(t, c.nodes[0].CancelMovement(c.addrs[1]), "alive")

			require.NoError(t, c.nodes[0].CancelMovement(dead.HostID))
			require.Equal(t, cluster.StatusLeft, c.nodes[0].schema.Load().Nodes[dead.HostID].Status)
			kinds := logKinds(c.nodes[0], dead.HostID)
			want := []string{"REGISTER", "CANCEL_JOIN"}
			if status == cluster.StatusMidJoin {
				want = []string{"REGISTER", "CANCEL_MID_JOIN", "CANCEL_JOIN"}
			}
			require.Equal(t, want, kinds)

			n := c.add()
			c.start(n)
			require.Equal(t, cluster.StatusNormal, c.nodes[0].schema.Load().Nodes[c.nodes[n].local.HostID].Status)
			require.Eventually(t, func() bool { return len(c.nodes[0].proxy.ring.Load().Nodes()) == 4 }, 5*time.Second, 10*time.Millisecond)
			c.checkReplicas(100)
		})
	}
}

// TestClusterReplicaCatchesUp: a replica that has not heard of a table
// yet (the metadata push did not reach it) catches up when a write to it
// arrives, planned with the newer metadata, instead of failing it.
func TestClusterReplicaCatchesUp(t *testing.T) {
	c := newTestCluster(t, 2)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	time.Sleep(100 * time.Millisecond)
	behind := c.nodes[1]
	behind.proxy.testNoGossipCatchUp.Store(true)
	behind.proxy.m.SetBlocked(func(v cluster.Verb) bool { return v == cluster.VerbMetadataPush })
	before := behind.schema.Load().Epoch
	c.ok(0, quorum, "CREATE TABLE ks.fresh (k int PRIMARY KEY, v text)")
	require.Equal(t, before, behind.schema.Load().Epoch, "the push did not reach it")

	c.ok(0, all, "INSERT INTO ks.fresh (k, v) VALUES (?, ?)", 1, "x")
	require.Greater(t, behind.schema.Load().Epoch, before, "caught up with the write")
	res := behind.Execute(mustPrepare(t, behind, "SELECT v FROM ks.fresh WHERE k = 1"), &RequestOptions{Consistency: one, local: true})
	require.Len(t, res.(*message.RowsResult).Data, 1, "the write is here")
}
