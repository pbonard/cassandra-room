package db

import (
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

// TestClusterAccordDecommission: transactions keep running while a node
// leaves. The nodes taking over its ranges get Accord bootstrap points for
// them (sync points taken before its data is streamed to them), as a
// joining node does for its own; balances are conserved, and every replica
// converges.
func TestClusterAccordDecommission(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.acct (id int PRIMARY KEY, balance int) WITH transactional_mode = 'full'")
	const accounts = 8
	for i := 0; i < accounts; i++ {
		require.Eventually(t, func() bool {
			_, failed := c.exec(i%2, quorum, "INSERT INTO ks.acct (id, balance) VALUES (?, 100)", i).(message.Error)
			return !failed
		}, 30*time.Second, 100*time.Millisecond)
	}
	for _, d := range c.nodes {
		require.Zero(t, d.proxy.accord.Stats().Bootstrapped, "founding nodes did not bootstrap")
	}
	transfer := `BEGIN TRANSACTION
		LET src = (SELECT balance FROM ks.acct WHERE id = ?);
		IF src.balance >= ? THEN
			UPDATE ks.acct SET balance -= ? WHERE id = ?;
			UPDATE ks.acct SET balance += ? WHERE id = ?;
		END IF
	COMMIT TRANSACTION`
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ok atomic.Int64
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				from, to := (w+i)%accounts, (w+3*i+1)%accounts
				if from == to {
					continue
				}
				if _, isVoid := c.exec(w%2, quorum, transfer, from, 5, 5, from, 5, to).(*message.VoidResult); isVoid {
					ok.Add(1)
				}
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.nodes[2].Decommission())
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	require.Positive(t, ok.Load())

	gained := 0
	for _, d := range c.nodes[:2] {
		gained += d.proxy.accord.Stats().Bootstrapped
	}
	require.Positive(t, gained, "the nodes taking over the ranges have their bootstrap points")

	total := 0
	for i := 0; i < accounts; i++ {
		v := c.values(i%2, quorum, "SELECT balance FROM ks.acct WHERE id = ?", i)
		b, err := strconv.Atoi(v[0])
		require.NoError(t, err)
		total += b
	}
	require.Equal(t, accounts*100, total)
	ring := c.nodes[0].proxy.ring.Load()
	repl := map[string]string{"class": "SimpleStrategy", "replication_factor": "2"}
	require.Eventually(t, func() bool {
		for i := 0; i < accounts; i++ {
			var seen []string
			for _, r := range ring.Replicas(repl, Token(int32Bytes(i))) {
				node := slices.Index(c.addrs, r.Addr)
				res := c.nodes[node].Execute(mustPrepare(t, c.nodes[node], "SELECT balance FROM ks.acct WHERE id = ?"),
					&RequestOptions{Consistency: one, local: true, Values: []*primitive.Value{primitive.NewValue(int32Bytes(i))}})
				rows, isRows := res.(*message.RowsResult)
				if !isRows || len(rows.Data) != 1 {
					return false
				}
				seen = append(seen, string(rows.Data[0][0]))
			}
			if len(slices.Compact(seen)) != 1 {
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond, "the replicas converge")
}
