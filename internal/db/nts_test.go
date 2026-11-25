package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNTSReplicationFactor: NetworkTopologyStrategy's replication_factor
// applies to every data center not given its own (it was kept as a data
// center named "replication_factor", and the keyspace had no replicas).
func TestNTSReplicationFactor(t *testing.T) {
	c := newTestCluster(t, 4, "dc1", "dc1", "dc2", "dc2")
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
	k := c.nodes[0].schema.Load().Keyspaces["ks"]
	require.Equal(t, map[string]string{"class": "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1": "2", "dc2": "2"}, k.Replication)
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	c.ok(0, all, "INSERT INTO ks.kv (k, v) VALUES (1, 'x')")
	require.Equal(t, []string{"x"}, c.values(3, all, "SELECT v FROM ks.kv WHERE k = 1"))

	c.ok(0, quorum, "ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1, 'dc2': 2}")
	k = c.nodes[0].schema.Load().Keyspaces["ks"]
	require.Equal(t, "1", k.Replication["dc1"])
	require.Equal(t, "2", k.Replication["dc2"], "a data center given its own keeps it")
	require.NotContains(t, k.Replication, "replication_factor")
}
