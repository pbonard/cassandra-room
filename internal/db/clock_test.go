package db

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPlainWriteAfterLWT: an ordinary write right after a transaction on
// the same row wins, also after a burst of transactions. A transaction's
// cells carry its executeAt's HLC as timestamp, which must keep pace with
// the wall clock: a clock that rounded every value up to the node's tag
// (1,024 µs a transaction) ran ahead under load, and the ordinary write,
// stamped with the wall clock, was older and silently lost.
func TestPlainWriteAfterLWT(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.c (k int PRIMARY KEY, v int)")
	// A burst of transactions coordinated by node 0.
	var n atomic.Int64
	var wg sync.WaitGroup
	stop := time.Now().Add(1500 * time.Millisecond)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				c.exec(0, quorum, "INSERT INTO ks.c (k, v) VALUES (?, ?) IF NOT EXISTS", 1000+w*100000+i, i)
				n.Add(1)
			}
		}()
	}
	wg.Wait()
	t.Logf("%d transactions", n.Load())
	for i := 1; i <= 20; i++ {
		c.ok(0, quorum, "INSERT INTO ks.c (k, v) VALUES (?, 0) IF NOT EXISTS", i)
		c.ok(0, quorum, "UPDATE ks.c SET v = ? WHERE k = ?", -i, i)
		require.Equal(t, []string{"-" + strconv.Itoa(i)}, c.values(1, quorum, "SELECT v FROM ks.c WHERE k = ?", i), "row %d: the plain write was lost", i)
	}
}
