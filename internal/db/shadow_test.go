package db

import (
	"bytes"
	"context"
	"slices"
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

// TestShadowerReads: forward reads starting at or before a partition's
// header take its deletions from the scan (no extra read, also for
// partitions with none); reverse reads and pages starting within a
// partition read them once per partition; tables without clustering
// columns are not filtered at all.
func TestShadowerReads(t *testing.T) {
	c := newTestCluster(t, 1)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(50 * time.Millisecond)
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (1, 1, 'old') USING TIMESTAMP 1")
	c.ok(0, one, "DELETE FROM ks.t USING TIMESTAMP 2 WHERE pk = 1")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (1, 2, 'new') USING TIMESTAMP 3")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (2, 1, 'old') USING TIMESTAMP 1")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (2, 9, 'kept') USING TIMESTAMP 1")
	c.ok(0, one, "DELETE FROM ks.t USING TIMESTAMP 2 WHERE pk = 2 AND ck >= 0 AND ck <= 5")
	c.ok(0, one, "INSERT INTO ks.t (pk, ck, v) VALUES (3, 1, 'plain') USING TIMESTAMP 1")
	c.ok(0, one, "INSERT INTO ks.kv (k, v) VALUES (1, 'x')")

	d := c.nodes[0]
	p := d.proxy
	tb, err := d.schema.Load().table("ks", "t")
	require.NoError(t, err)
	lt, err := d.engine.Table(tb.Storage)
	require.NoError(t, err)
	// scan reads [lo, hi) as the range handlers do, returning the rows'
	// values (by partition key) and how many partitions' deletions were
	// read.
	scan := func(forward bool, lo, hi []byte) (map[int][]string, int) {
		sh := p.newShadower(tb.Storage, lt, forward, lo)
		out := map[int][]string{}
		add := func(k, v []byte) {
			v, err := sh.filter(k, v)
			require.NoError(t, err)
			n := partitionPrefixLen(k)
			if v == nil || n >= len(k) || k[n] == headerSuffix || k[n] == tombstoneSuffix {
				return
			}
			_, pk, _, err := parsePartition(k)
			require.NoError(t, err)
			r, err := decodeRowForTest(v)
			require.NoError(t, err)
			out[int(pk[3])] = append(out[int(pk[3])], r)
		}
		if forward {
			it, err := lt.NewIter(lo, hi)
			require.NoError(t, err)
			for ok := it.First(); ok; ok = it.Next() {
				add(it.Key(), it.Value())
			}
			require.NoError(t, it.Close())
		} else {
			it, err := lt.NewReverseIter(lo, hi)
			require.NoError(t, err)
			for ok := it.Last(); ok; ok = it.Prev() {
				add(it.Key(), it.Value())
			}
			require.NoError(t, it.Close())
		}
		return out, sh.loads
	}
	want := map[int][]string{1: {"new"}, 2: {"kept"}, 3: {"plain"}}
	got, loads := scan(true, nil, nil)
	require.Equal(t, want, got)
	require.Zero(t, loads, "a forward scan of whole partitions reads nothing more")

	p1 := partitionPrefix([]byte{0, 0, 0, 1})
	got, loads = scan(true, append(slices.Clip(p1), headerSuffix), append(slices.Clip(p1), 0x60))
	require.Equal(t, map[int][]string{1: {"new"}}, got)
	require.Zero(t, loads, "a single-partition read from its header")

	got, loads = scan(true, append(slices.Clip(p1), 0x20), prefixEnd(p1))
	require.Equal(t, map[int][]string{1: {"new"}}, got)
	require.Equal(t, 1, loads, "a page within the partition reads its deletions")

	got, loads = scan(false, nil, nil)
	require.Equal(t, want, got)
	require.Equal(t, 3, loads, "a reverse scan reads each partition's deletions")

	kv, err := d.schema.Load().table("ks", "kv")
	require.NoError(t, err)
	require.Nil(t, p.newShadower(kv.Storage, nil, true, nil), "no clustering columns: not filtered")
}

func TestAtOrBeforeHeader(t *testing.T) {
	p := []byte{1, 2, 3}
	for _, c := range []struct {
		lo   []byte
		want bool
	}{
		{nil, true},
		{[]byte{1, 2}, true},
		{[]byte{1, 2, 3}, true},
		{[]byte{1, 2, 3, 0x0f, 9}, true},
		{[]byte{1, 2, 3, headerSuffix}, true},
		{[]byte{1, 2, 3, headerSuffix, 0}, false},
		{[]byte{1, 2, 3, tombstoneSuffix}, false},
		{[]byte{1, 2, 3, 0x20}, false},
		{[]byte{1, 2, 2, 0xff}, true},
		{[]byte{1, 2, 4}, false},
	} {
		want := bytes.Compare(c.lo, append(slices.Clip(p), headerSuffix)) <= 0
		require.Equal(t, want, c.want, "%x", c.lo)
		require.Equal(t, c.want, atOrBeforeHeader(c.lo, p), "%x", c.lo)
	}
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
