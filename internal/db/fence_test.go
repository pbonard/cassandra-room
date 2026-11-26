package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/stretchr/testify/require"
)

// TestFence: requests planned before the fence's epoch are refused; a
// drain waits for the ones admitted before it advanced, not later ones.
func TestFence(t *testing.T) {
	var f fence
	f.advance(5)
	_, err := f.enter(4)
	require.True(t, isBehind(err), "planned before the placements: refused")
	doneUnfenced, err := f.enter(0)
	require.NoError(t, err, "hints and streaming are not fenced")
	doneUnfenced()

	old, err := f.enter(5)
	require.NoError(t, err)
	f.advance(6)
	later, err := f.enter(6)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	require.Error(t, f.drain(ctx), "a request of epoch 5 is in progress")
	cancel()
	old()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, f.drain(ctx), "requests of the current placements do not hold it up")
	cancel()
	later()
}

// TestBarrierGroups: a barrier needs, of each moving range's old and new
// owners, one more than may be left out of a quorum of them (per data
// center under NetworkTopologyStrategy); ranges that do not move need
// nothing.
func TestBarrierGroups(t *testing.T) {
	var nodes []cluster.Node
	for i := range 4 {
		nodes = append(nodes, cluster.Node{Addr: fmt.Sprint("n", i), HostID: fmt.Sprint("h", i), Status: cluster.StatusNormal, DataCenter: "dc1", Tokens: cluster.RandomTokens(8)})
	}
	schema := func(repl map[string]string, nodes []cluster.Node) *Schema {
		s := &Schema{Nodes: map[string]*cluster.Node{}, Keyspaces: map[string]*Keyspace{"ks": {Name: "ks", Replication: repl}}}
		for i := range nodes {
			s.Nodes[nodes[i].HostID] = &nodes[i]
		}
		return s
	}
	simple := func(rf int) map[string]string {
		return map[string]string{"class": "SimpleStrategy", "replication_factor": fmt.Sprint(rf)}
	}
	require.Empty(t, barrierGroups(schema(simple(3), nodes)), "nothing moves")

	leaving := append([]cluster.Node(nil), nodes...)
	leaving[1].Status = cluster.StatusLeaving
	for rf, need := range map[int]int{1: 1, 2: 1, 3: 2} {
		gs := barrierGroups(schema(simple(rf), leaving))
		require.NotEmpty(t, gs)
		for _, g := range gs {
			require.Len(t, g.hosts, rf)
			require.Equal(t, need, g.need, "RF %d", rf)
		}
	}
	nts := map[string]string{"class": "NetworkTopologyStrategy", "dc1": "3"}
	for _, g := range barrierGroups(schema(nts, leaving)) {
		require.Equal(t, 2, g.need)
	}
}

// TestClusterFencedCoordinator: a node cut off from the metadata log while
// a node joins (paused, or partitioned, not crashed) is not waited for by
// the join's barriers, since a quorum of each moving range answered; when
// it coordinates afterwards with the placements from before the join, the
// replicas refuse its requests instead of letting it write to and read
// from the old owners; once it can reach the log again, it catches up and
// its requests succeed.
func TestClusterFencedCoordinator(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 50; i++ {
		c.ok(0, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	const stale = 2
	c.nodes[stale].proxy.m.SetBlocked(func(v cluster.Verb) bool {
		return v == cluster.VerbMetadataFetch || v == cluster.VerbMetadataPush || v == cluster.VerbMetadataSync
	})
	before := c.nodes[stale].schema.Load().Epoch

	n := c.add()
	start := time.Now()
	c.start(n)
	require.Less(t, time.Since(start), 20*time.Second, "the barriers do not wait for the node cut off")
	require.Equal(t, cluster.StatusNormal, c.nodes[0].schema.Load().Nodes[c.nodes[n].local.HostID].Status)
	require.Equal(t, before, c.nodes[stale].schema.Load().Epoch, "still behind")

	res := c.exec(stale, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", 1000, "stale")
	require.Implements(t, (*message.Error)(nil), res, "a write planned with the old placements is refused")
	res = c.exec(stale, quorum, "SELECT v FROM ks.kv WHERE k = ?", 1)
	require.Implements(t, (*message.Error)(nil), res, "so is a read")

	c.nodes[stale].proxy.m.SetBlocked(nil)
	c.ok(stale, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", 1001, "caught up")
	require.Greater(t, c.nodes[stale].schema.Load().Epoch, before, "it caught up when refused")
	require.Equal(t, []string{"caught up"}, c.values(n, quorum, "SELECT v FROM ks.kv WHERE k = ?", 1001))
	require.Equal(t, []string{"v1"}, c.values(stale, quorum, "SELECT v FROM ks.kv WHERE k = ?", 1))
}
