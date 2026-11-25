package db

import (
	"fmt"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
)

// TestLoggedBatchAtomic: a LOGGED batch over several partitions is all or
// nothing (there is no batchlog: it runs as an Accord transaction). With
// the replica of one partition down, it fails and writes nothing; an
// UNLOGGED batch writes what it can, as in Cassandra. Client timestamps
// are kept.
func TestLoggedBatchAtomic(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	k := c.nodes[0].schema.Load().Keyspaces["ks"]
	owner := func(i int) string {
		pk := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		return c.nodes[0].proxy.ring.Load().Replicas(k.Replication, Token(pk))[0].Addr
	}
	// a on node 0 or 1, b and b2 on node 2.
	a, b, b2 := -1, -1, -1
	for i := 0; i < 1000 && (a < 0 || b2 < 0); i++ {
		switch o := owner(i); {
		case o == c.addrs[2] && b < 0:
			b = i
		case o == c.addrs[2] && b2 < 0:
			b2 = i
		case o != c.addrs[2] && a < 0:
			a = i
		}
	}
	require.True(t, a >= 0 && b2 >= 0)

	// All up: the batch applies, with its timestamp.
	c.ok(0, one, fmt.Sprintf("BEGIN BATCH USING TIMESTAMP 1000 INSERT INTO ks.kv (k, v) VALUES (%d, 'a') INSERT INTO ks.kv (k, v) VALUES (%d, 'b') APPLY BATCH", a, b))
	require.Equal(t, []string{"a"}, c.values(1, quorum, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", a)))
	require.Equal(t, []string{"b"}, c.values(1, quorum, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", b)))
	wt := c.values(1, quorum, fmt.Sprintf("SELECT WRITETIME(v) FROM ks.kv WHERE k = %d", b))
	require.Equal(t, []string{"\x00\x00\x00\x00\x00\x00\x03\xe8"}, wt, "the batch's timestamp, 1000")

	// b2's replica is up but refuses plain writes (as if it crashed or
	// timed out mid-batch).
	yes := true
	c.nodes[2].proxy.testRefuseMutations.Store(&yes)
	res := c.exec(0, one, fmt.Sprintf("BEGIN BATCH INSERT INTO ks.kv (k, v) VALUES (%d, 'logged') INSERT INTO ks.kv (k, v) VALUES (%d, 'logged') APPLY BATCH", a, b2))
	if _, failed := res.(message.Error); failed {
		require.Equal(t, []string{"a"}, c.values(0, one, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", a)), "a failed logged batch wrote nothing")
	} else {
		require.Equal(t, []string{"logged"}, c.values(0, one, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", a)))
		require.Equal(t, []string{"logged"}, c.values(0, one, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", b2)), "all of the logged batch")
	}
	// An unlogged batch is not atomic, as in Cassandra.
	res = c.exec(0, one, fmt.Sprintf("BEGIN UNLOGGED BATCH INSERT INTO ks.kv (k, v) VALUES (%d, 'unlogged') INSERT INTO ks.kv (k, v) VALUES (%d, 'unlogged') APPLY BATCH", a, b2))
	_, failed := res.(message.Error)
	require.True(t, failed)
	require.Equal(t, []string{"unlogged"}, c.values(0, one, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", a)))
}
