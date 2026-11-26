package db

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/stretchr/testify/require"
)

// logKinds returns the kinds of the metadata log's entries about a host.
func logKinds(d *DB, hostID string) []string {
	var out []string
	for _, e := range d.schema.Load().Log {
		if strings.Contains(e.Detail, hostID) {
			out = append(out, e.Kind)
		}
	}
	return out
}

// TestClusterTransitionSteps: a join, a decommission and their steps
// (Prepare, Mid, Finish); while one node moves, another cannot start.
func TestClusterTransitionSteps(t *testing.T) {
	c := newTestCluster(t, 2)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 100; i++ {
		c.ok(i%2, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	n := c.add()
	var during []string
	var refused error
	d, err := c.open(n, "", func(op, unit string) error {
		if during == nil {
			// Streaming: every node writes to the joining node already.
			for _, o := range c.nodes[:n] {
				for _, m := range o.schema.Load().Nodes {
					if m.Addr == c.addrs[n] {
						during = append(during, m.Status)
					}
				}
			}
			refused = c.nodes[1].proxy.startMovement("PREPARE_LEAVE", c.nodes[1].local.HostID, cluster.StatusLeaving)
		}
		return nil
	})
	require.NoError(t, err)
	c.nodes[n] = d
	require.Equal(t, []string{cluster.StatusJoining, cluster.StatusJoining}, during, "the barrier waits for every node to know the join")
	require.ErrorIs(t, refused, errRangeMovement)
	require.Equal(t, cluster.StatusNormal, c.nodes[1].schema.Load().Nodes[c.nodes[1].local.HostID].Status)
	require.Equal(t, []string{"REGISTER", "MID_JOIN", "FINISH_JOIN"}, logKinds(c.nodes[0], d.local.HostID))
	c.checkReplicas(100)

	require.NoError(t, d.Decommission())
	kinds := logKinds(c.nodes[0], d.local.HostID)
	require.Equal(t, []string{"PREPARE_LEAVE", "MID_LEAVE", "FINISH_LEAVE"}, kinds[len(kinds)-3:])
}

// TestClusterJoinWaitsForMovement: a node joining while another leaves
// waits for it, and a decommission is refused.
func TestClusterJoinWaitsForMovement(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 100; i++ {
		c.ok(0, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	// Node 2 is in the middle of leaving.
	leaving := c.nodes[2].local.HostID
	require.NoError(t, c.nodes[0].proxy.setNodeStatus("PREPARE_LEAVE", leaving, cluster.StatusLeaving))
	err := c.nodes[1].Decommission()
	require.ErrorIs(t, err, errRangeMovement)

	n := c.add()
	joined := make(chan error, 1)
	go func() {
		d, err := c.open(n, "", nil)
		if err == nil {
			c.nodes[n] = d
		}
		joined <- err
	}()
	select {
	case err := <-joined:
		t.Fatalf("joined while a node leaves: %v", err)
	case <-time.After(3 * time.Second):
	}
	require.False(t, slices.ContainsFunc(nodeList(c.nodes[0].schema.Load()), func(m cluster.Node) bool { return m.Addr == c.addrs[n] }), "not registered yet")
	require.NoError(t, c.nodes[0].proxy.setNodeStatus("CANCEL_LEAVE", leaving, cluster.StatusNormal))
	select {
	case err := <-joined:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the join did not resume")
	}
	require.Equal(t, cluster.StatusNormal, c.nodes[0].schema.Load().Nodes[c.nodes[n].local.HostID].Status)
	c.checkReplicas(100)
}
