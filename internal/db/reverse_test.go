package db

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

// TestReversedReads: a query in the opposite of the clustering order
// returns the forward rows reversed, with clustering restrictions, row,
// range and partition deletions, static columns, LIMIT and paging; and it
// reads only about the rows it returns.
func TestReversedReads(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (p int, a int, b int, st int STATIC, v int, PRIMARY KEY (p, a, b))")
	s.ok("CREATE TABLE ks.d (p int, a int, b int, v int, PRIMARY KEY (p, a, b)) WITH CLUSTERING ORDER BY (a DESC, b ASC)")
	rng := rand.New(rand.NewSource(3))
	for _, tbl := range []string{"t", "d"} {
		for p := 0; p < 3; p++ {
			for i := 0; i < 400; i++ {
				s.ok(fmt.Sprintf("INSERT INTO ks.%s (p, a, b, v) VALUES (?, ?, ?, ?)", tbl), p, rng.Intn(40), rng.Intn(10), i)
				if i%97 == 0 {
					require.NoError(t, s.d.engine.Flush())
				}
			}
			s.ok(fmt.Sprintf("DELETE FROM ks.%s WHERE p = ? AND a = 7", tbl), p)
			s.ok(fmt.Sprintf("DELETE FROM ks.%s WHERE p = ? AND a > 20 AND a <= 25", tbl), p)
			s.ok(fmt.Sprintf("DELETE FROM ks.%s WHERE p = ? AND a = 30 AND b = 3", tbl), p)
		}
	}
	s.ok("UPDATE ks.t SET st = 9 WHERE p = 1")
	s.ok("DELETE FROM ks.t WHERE p = 2")
	s.ok("INSERT INTO ks.t (p, a, b, v) VALUES (2, 1, 1, 1)")
	s.ok("UPDATE ks.t SET st = 5 WHERE p = 3") // static only

	natural := map[string]string{"t": "ASC", "d": "DESC"}
	reverse := map[string]string{"t": "DESC", "d": "ASC"}
	for _, tbl := range []string{"t", "d"} {
		for _, where := range []string{"", " AND a = 5", " AND a >= 10 AND a < 30", " AND a IN (3, 9, 30)", " AND (a, b) > (12, 4)", " AND a = 30 AND b IN (1, 3, 5)"} {
			for p := 0; p < 4; p++ {
				cols := "a, b, v"
				if tbl == "t" {
					cols = "a, b, st, v"
				}
				q := func(order string) string {
					return fmt.Sprintf("SELECT %s FROM ks.%s WHERE p = %d%s ORDER BY a %s", cols, tbl, p, where, order)
				}
				fwd := s.rows(q(natural[tbl]))
				want := slices.Clone(fwd)
				slices.Reverse(want)
				require.Equal(t, want, s.rows(q(reverse[tbl])), q(reverse[tbl]))
				// LIMIT, and paging by 7.
				if len(want) > 3 {
					require.Equal(t, want[:3], s.rows(q(reverse[tbl])+" LIMIT 3"))
				}
				require.Equal(t, want, pages(t, s, q(reverse[tbl]), 7), "paged %s", q(reverse[tbl]))
			}
		}
	}

	// Only about the rows returned are read.
	for i := 0; i < 5000; i++ {
		s.ok("INSERT INTO ks.t (p, a, b, v) VALUES (10, ?, ?, ?)", i/10, i%10, i)
	}
	require.NoError(t, s.d.engine.Flush())
	before := rowsReadBackward.Load()
	require.Equal(t, []string{"499 9 4999"}, s.rows("SELECT a, b, v FROM ks.t WHERE p = 10 ORDER BY a DESC LIMIT 1"))
	require.Equal(t, 5, len(s.rows("SELECT a FROM ks.t WHERE p = 10 ORDER BY a DESC LIMIT 5")))
	require.Less(t, rowsReadBackward.Load()-before, int64(64), "rows read for LIMIT 1 and LIMIT 5")
	require.Positive(t, rowsReadBackward.Load()-before, "read backwards")
}

// pages runs a query page by page.
func pages(t *testing.T, s *session, q string, size int32) []string {
	t.Helper()
	p, err := s.d.Prepare("", q)
	require.NoError(t, err)
	var out []string
	var state []byte
	for i := 0; i < 1000; i++ {
		res, ok := s.d.Execute(p, &RequestOptions{PageSize: size, PagingState: state}).(*message.RowsResult)
		require.True(t, ok)
		for _, r := range res.Data {
			var cols []string
			for j, v := range r {
				if v == nil {
					cols = append(cols, "null")
				} else {
					cols = append(cols, formatValue(s.colType(res.Metadata.Columns[j]), v))
				}
			}
			out = append(out, strings.Join(cols, " "))
		}
		state = res.Metadata.PagingState
		if len(state) == 0 {
			return out
		}
	}
	t.Fatal("too many pages")
	return nil
}

// TestClusterReversedReads: reversed reads across replicas (a page per
// replica, merged backwards, read repair mirrored) return the forward rows
// reversed, also when one replica missed writes and with a partition
// larger than a replica's page, and read only about what they return.
func TestClusterReversedReads(t *testing.T) {
	c := &testCluster{t: t, tweak: func(o *ClusterOptions) { o.DisableHints = true }}
	for i := 0; i < 3; i++ {
		c.add()
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, d := range c.nodes {
			if d != nil {
				d.Close()
			}
		}
	})
	c.waitConverged()
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.t (p int, c int, v int, PRIMARY KEY (p, c))")
	for i := 0; i < 1500; i++ {
		c.ok(i%3, quorum, "INSERT INTO ks.t (p, c, v) VALUES (1, ?, ?)", i, i)
	}
	// Node 2 misses some writes and a range deletion.
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) }, 10*time.Second, 10*time.Millisecond)
	for i := 1500; i < 1600; i++ {
		c.ok(0, quorum, "INSERT INTO ks.t (p, c, v) VALUES (1, ?, ?)", i, i)
	}
	c.ok(0, quorum, "DELETE FROM ks.t WHERE p = 1 AND c >= 100 AND c < 200")
	c.start(2)
	c.waitConverged()

	fwd := ordered(c, 0, all, "SELECT c FROM ks.t WHERE p = 1")
	require.Len(t, fwd, 1500)
	want := slices.Clone(fwd)
	slices.Reverse(want)
	require.Equal(t, want, ordered(c, 1, all, "SELECT c FROM ks.t WHERE p = 1 ORDER BY c DESC"))
	require.Equal(t, want[:3], ordered(c, 2, quorum, "SELECT c FROM ks.t WHERE p = 1 ORDER BY c DESC LIMIT 3"))
	require.Equal(t, want, ordered(c, 2, quorum, "SELECT c FROM ks.t WHERE p = 1 ORDER BY c DESC"))
	// Paged, through the coordinator.
	p := mustPrepare(t, c.nodes[0], "SELECT c FROM ks.t WHERE p = 1 ORDER BY c DESC")
	var got []string
	var state []byte
	for {
		res := c.nodes[0].Execute(p, &RequestOptions{Consistency: quorum, PageSize: 300, PagingState: state}).(*message.RowsResult)
		for _, r := range res.Data {
			got = append(got, fmt.Sprint(int32(be32(r[0]))))
		}
		if state = res.Metadata.PagingState; len(state) == 0 {
			break
		}
	}
	require.Equal(t, want, got)
	before := rowsReadBackward.Load()
	require.Len(t, c.values(0, quorum, "SELECT c FROM ks.t WHERE p = 1 ORDER BY c DESC LIMIT 1"), 1)
	require.Less(t, rowsReadBackward.Load()-before, int64(64))
	require.Positive(t, rowsReadBackward.Load()-before, "read backwards")

}

// ordered runs a query of one int column, keeping the rows' order.
func ordered(c *testCluster, node int, cl primitive.ConsistencyLevel, q string) []string {
	c.t.Helper()
	res := c.ok(node, cl, q).(*message.RowsResult)
	var out []string
	for _, r := range res.Data {
		out = append(out, fmt.Sprint(int32(be32(r[0]))))
	}
	return out
}
