package db

import (
	"sync"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
)

// TestConcurrentCounters: increments from every node at once, on a few
// counters, sum exactly (the leader's per-cell locks, released before
// replicating, lose no update).
func TestConcurrentCounters(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.c (k int PRIMARY KEY, n counter)")
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				res := c.exec(w%3, quorum, "UPDATE ks.c SET n = n + 1 WHERE k = ?", i%3)
				_, failed := res.(message.Error)
				require.False(t, failed, "%v", res)
			}
		}()
	}
	wg.Wait()
	total := int64(0)
	for k := 0; k < 3; k++ {
		v := c.values(0, all, "SELECT n FROM ks.c WHERE k = ?", k)
		require.Len(t, v, 1)
		b := []byte(v[0])
		n := int64(0)
		for _, x := range b {
			n = n<<8 | int64(x)
		}
		total += n
	}
	require.EqualValues(t, 12*25, total)
}
