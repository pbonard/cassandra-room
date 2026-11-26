package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// TestReplicaReadsShadowed: a replica answers row and range reads, forward
// and reverse, without the cells its partition deletion and range
// tombstones shadow, though they are still stored (not compacted yet):
// read repair then never copies them to a replica that compacted them
// away, where, once it purged the deletion, they would be resurrected.
func TestReplicaReadsShadowed(t *testing.T) {
	c := newTestCluster(t, 1)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
	time.Sleep(50 * time.Millisecond)
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (1, 1, 'old') USING TIMESTAMP 1")
	c.ok(0, one, "DELETE FROM ks.t USING TIMESTAMP 2 WHERE pk = 1")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (1, 2, 'new') USING TIMESTAMP 3")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (2, 1, 'old') USING TIMESTAMP 1")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (2, 9, 'kept') USING TIMESTAMP 1")
	c.ok(0, one, "DELETE FROM ks.t USING TIMESTAMP 2 WHERE pk = 2 AND ck >= 0 AND ck <= 5")

	d := c.nodes[0]
	tb, err := d.schema.Load().table("ks", "t")
	require.NoError(t, err)
	lt, err := d.engine.Table(tb.Storage)
	require.NoError(t, err)
	p := d.proxy
	rows := func(pk int, reverse bool) []string {
		prefix := partitionPrefix([]byte{0, 0, 0, byte(pk)})
		h := p.handleReadRange
		if reverse {
			h = p.handleReadRangeReverse
		}
		b, err := h(context.Background(), "", fenced(0, encodeKey(tb.Storage, append(prefix, 0x20), prefixEnd(prefix))))
		require.NoError(t, err)
		kvs, _, err := decodeKVs(b)
		require.NoError(t, err)
		var out []string
		for _, e := range kvs {
			r, err := decodeRowForTest(e.v)
			require.NoError(t, err)
			out = append(out, r)
		}
		return out
	}
	for _, reverse := range []bool{false, true} {
		require.Equal(t, []string{"new"}, rows(1, reverse), "partition deletion, reverse %v", reverse)
		require.Equal(t, []string{"kept"}, rows(2, reverse), "range tombstone, reverse %v", reverse)
	}
	// Still stored: dropped from the answers, not from the data.
	key := append(partitionPrefix([]byte{0, 0, 0, 1}), 0x20)
	it, err := lt.NewIter(key, prefixEnd(partitionPrefix([]byte{0, 0, 0, 1})))
	require.NoError(t, err)
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	require.NoError(t, it.Close())
	require.Equal(t, 2, n)
	require.Equal(t, []string{"new"}, c.values(0, one, "SELECT v FROM ks.t WHERE pk = 1"))
	require.Equal(t, []string{"kept"}, c.values(0, one, "SELECT v FROM ks.t WHERE pk = 2 ORDER BY ck DESC"))
}

// decodeRowForTest returns a stored row's non-empty cell values.
func decodeRowForTest(v []byte) (string, error) {
	r, err := row.Decode(v)
	if err != nil {
		return "", err
	}
	out := ""
	for _, c := range r.Cells {
		out += string(c.Value)
	}
	return out, nil
}
