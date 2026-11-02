package db

import (
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
)

func TestSpeculateAfter(t *testing.T) {
	l := &readLatency{}
	timeout := time.Second
	d, never := speculateAfter("99p", l, timeout)
	require.False(t, never)
	require.Equal(t, 50*time.Millisecond, d, "no samples yet")
	for i := 1; i <= 100; i++ {
		l.add(time.Duration(i) * time.Millisecond)
	}
	d, _ = speculateAfter("99p", l, timeout)
	require.Equal(t, 100*time.Millisecond, d)
	d, _ = speculateAfter("50PERCENTILE", l, timeout)
	require.Equal(t, 51*time.Millisecond, d)
	d, _ = speculateAfter("20ms", l, timeout)
	require.Equal(t, 20*time.Millisecond, d)
	d, never = speculateAfter("ALWAYS", l, timeout)
	require.Zero(t, d)
	require.False(t, never)
	_, never = speculateAfter("NONE", l, timeout)
	require.True(t, never)
	d, _ = speculateAfter("MIN(99p, 30ms)", l, timeout)
	require.Equal(t, 30*time.Millisecond, d)
	d, _ = speculateAfter("MAX(99p, 30ms)", l, timeout)
	require.Equal(t, 100*time.Millisecond, d)
	d, _ = speculateAfter("2000ms", l, timeout)
	require.Equal(t, timeout/2, d, "capped at half the timeout")
}

// TestClusterRangeReadRepair checks that a range read repairs replicas
// that returned less, as single-row reads do.
func TestClusterRangeReadRepair(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text) WITH speculative_retry = 'NONE'")
	time.Sleep(100 * time.Millisecond)
	// A row on node 0 only.
	pr := mustPrepare(t, c.nodes[0], "INSERT INTO ks.kv (k, v) VALUES (5, 'only-here')")
	e := &execCtx{d: c.nodes[0], now: time.Now(), cl: one}
	e.nowSec = e.now.Unix()
	mu := &mutation{}
	require.NoError(t, pr.state.Load().stmt.(*modification).addMutations(e, mu))
	require.NoError(t, c.nodes[0].applyLocal(mu.writes))
	local := func(n int) int {
		res := c.nodes[n].Execute(mustPrepare(t, c.nodes[n], "SELECT k FROM ks.kv"), &RequestOptions{Consistency: one, local: true, forwarded: true})
		return len(res.(*message.RowsResult).Data)
	}
	require.Zero(t, local(1))
	require.Equal(t, []string{"5 only-here"}, c.values(1, all, "SELECT k, v FROM ks.kv"))
	require.Equal(t, 1, local(1), "repaired by the range read")
	require.Equal(t, 1, local(2))
}
