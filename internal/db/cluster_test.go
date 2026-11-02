package db

import (
	"errors"
	"fmt"
	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

// testCluster is a cluster of in-process nodes, each with its own
// in-memory file system that survives restarts.
type testCluster struct {
	t     *testing.T
	fs    []vfs.FS
	addrs []string
	nodes []*DB
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().String()
}

func newTestCluster(t *testing.T, n int) *testCluster {
	c := &testCluster{t: t}
	for i := 0; i < n; i++ {
		c.fs = append(c.fs, vfs.NewMem())
		c.addrs = append(c.addrs, freeAddr(t))
		c.nodes = append(c.nodes, nil)
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
	return c
}

func (c *testCluster) start(i int) {
	d, err := Open("db", Options{FS: c.fs[i], Address: net.IPv4(127, 0, 0, byte(1+i)), Cluster: &ClusterOptions{
		ListenAddress: c.addrs[i], Seeds: []string{c.addrs[0]}, NumTokens: 8,
		Membership:   testMembership(),
		WriteTimeout: time.Second, ReadTimeout: time.Second,
	}})
	require.NoError(c.t, err)
	c.nodes[i] = d
}

func (c *testCluster) stop(i int) {
	require.NoError(c.t, c.nodes[i].Close())
	c.nodes[i] = nil
}

// waitConverged waits until every running node sees every running node up.
func (c *testCluster) waitConverged() {
	require.Eventually(c.t, func() bool {
		for i, d := range c.nodes {
			if d == nil {
				continue
			}
			for j, o := range c.nodes {
				if o != nil && i != j && !d.proxy.g.IsAlive(c.addrs[j]) {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond)
}

func (c *testCluster) exec(node int, cl primitive.ConsistencyLevel, q string, args ...any) message.Message {
	c.t.Helper()
	s := &session{t: c.t, d: c.nodes[node]}
	p, err := s.d.Prepare("", q)
	if err != nil {
		return ErrorMessage(err)
	}
	vals := make([]*primitive.Value, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case int:
			vals[i] = primitive.NewValue([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
		case string:
			vals[i] = primitive.NewValue([]byte(v))
		}
	}
	return s.d.Execute(p, &RequestOptions{Values: vals, Consistency: cl})
}

func (c *testCluster) ok(node int, cl primitive.ConsistencyLevel, q string, args ...any) message.Message {
	c.t.Helper()
	res := c.exec(node, cl, q, args...)
	if e, ok := res.(message.Error); ok {
		c.t.Fatalf("node %d: %s: %T %s", node, q, e, e.GetErrorMessage())
	}
	return res
}

func (c *testCluster) values(node int, cl primitive.ConsistencyLevel, q string, args ...any) []string {
	c.t.Helper()
	res := c.ok(node, cl, q, args...).(*message.RowsResult)
	var out []string
	for _, r := range res.Data {
		s := ""
		for i, v := range r {
			if i > 0 {
				s += " "
			}
			if v == nil {
				s += "null"
			} else if len(v) == 4 && res.Metadata.Columns[i].Type.AsCql() == "int" {
				s += fmt.Sprint(int32(be32(v)))
			} else {
				s += string(v)
			}
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

const (
	one    = primitive.ConsistencyLevelOne
	quorum = primitive.ConsistencyLevelQuorum
	all    = primitive.ConsistencyLevelAll
)

func TestClusterReplication(t *testing.T) {
	c := newTestCluster(t, 3)
	// A schema change on any node reaches every node.
	c.ok(1, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(2, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	require.Eventually(t, func() bool {
		for _, d := range c.nodes {
			if _, err := d.schema.Load().table("ks", "kv"); err != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	for i := 0; i < 100; i++ {
		c.ok(i%3, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	// Every node holds every row (RF 3): read locally on each.
	for n := range c.nodes {
		res := c.nodes[n].Execute(mustPrepare(t, c.nodes[n], "SELECT count(*) FROM ks.kv"), &RequestOptions{Consistency: one, local: true, forwarded: true})
		require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 100}, []byte(res.(*message.RowsResult).Data[0][0]), "node %d", n)
	}
	require.Equal(t, []string{"v42"}, c.values(2, all, "SELECT v FROM ks.kv WHERE k = 42"))
	require.Len(t, c.values(0, quorum, "SELECT k FROM ks.kv"), 100)

	// A node goes down: QUORUM still works, ALL is unavailable.
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) && !c.nodes[1].proxy.g.IsAlive(c.addrs[2]) },
		5*time.Second, 20*time.Millisecond)
	c.ok(0, quorum, "UPDATE ks.kv SET v = 'missed' WHERE k = 7")
	res := c.exec(0, all, "SELECT v FROM ks.kv WHERE k = 7")
	ue, ok := res.(*message.Unavailable)
	require.True(t, ok, "%v", res)
	require.EqualValues(t, 3, ue.Required)
	require.EqualValues(t, 2, ue.Alive)
	require.Equal(t, []string{"missed"}, c.values(1, quorum, "SELECT v FROM ks.kv WHERE k = 7"))
	// The metadata moves on without it.
	c.ok(1, quorum, "CREATE TABLE ks.later (k int PRIMARY KEY)")
	epoch := c.nodes[0].schema.Load().Epoch

	// It comes back, catches up with the metadata and receives the hint.
	c.start(2)
	c.waitConverged()
	require.GreaterOrEqual(t, c.nodes[2].schema.Load().Epoch, epoch)
	require.NotNil(t, c.nodes[2].schema.Load().Keyspaces["ks"].Tables["later"])
	require.Eventually(t, func() bool {
		res := c.nodes[2].Execute(mustPrepare(t, c.nodes[2], "SELECT v FROM ks.kv WHERE k = 7"), &RequestOptions{Consistency: one, local: true, forwarded: true})
		rows := res.(*message.RowsResult).Data
		return len(rows) == 1 && string(rows[0][0]) == "missed"
	}, 10*time.Second, 50*time.Millisecond, "hint replayed")

	// Read repair: a write only one replica has is merged at ALL and
	// repaired on the others.
	// Write to node 0's engine only: the other replicas miss it.
	pr := mustPrepare(t, c.nodes[0], "UPDATE ks.kv SET v = 'local-only' WHERE k = 8")
	mod := pr.state.Load().stmt.(*modification)
	e := &execCtx{d: c.nodes[0], now: time.Now(), cl: one}
	e.nowSec = e.now.Unix()
	mu := &mutation{}
	require.NoError(t, mod.addMutations(e, mu))
	require.NoError(t, c.nodes[0].applyLocal(mu.writes))
	res = c.nodes[1].Execute(mustPrepare(t, c.nodes[1], "SELECT v FROM ks.kv WHERE k = 8"), &RequestOptions{Consistency: one, local: true, forwarded: true})
	require.Equal(t, "v8", string(res.(*message.RowsResult).Data[0][0]), "node 1 does not have it yet")
	require.Equal(t, []string{"local-only"}, c.values(1, all, "SELECT v FROM ks.kv WHERE k = 8"))
	for n := range c.nodes {
		res := c.nodes[n].Execute(mustPrepare(t, c.nodes[n], "SELECT v FROM ks.kv WHERE k = 8"), &RequestOptions{Consistency: one, local: true, forwarded: true})
		require.Equal(t, "local-only", string(res.(*message.RowsResult).Data[0][0]), "node %d repaired", n)
	}
}

func mustPrepare(t *testing.T, d *DB, q string) *Prepared {
	p, err := d.Prepare("", q)
	require.NoError(t, err)
	return p
}

func TestClusterCountersLWTAndIndexes(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.cnt (k int PRIMARY KEY, n counter)")
	c.ok(0, quorum, "CREATE TABLE ks.u (k int PRIMARY KEY, email text)")
	c.ok(1, quorum, "CREATE INDEX ON ks.u (email)")
	time.Sleep(200 * time.Millisecond)
	// Counter updates from every node go to the partition's leader.
	for i := 0; i < 30; i++ {
		c.ok(i%3, quorum, "UPDATE ks.cnt SET n = n + 1 WHERE k = 1")
	}
	res := c.ok(2, quorum, "SELECT n FROM ks.cnt WHERE k = 1").(*message.RowsResult)
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 30}, []byte(res.Data[0][0]))
	// LWT from different coordinators.
	res = c.ok(0, quorum, "INSERT INTO ks.u (k, email) VALUES (1, 'a') IF NOT EXISTS").(*message.RowsResult)
	require.Equal(t, []byte{1}, []byte(res.Data[0][0]))
	res = c.ok(1, quorum, "INSERT INTO ks.u (k, email) VALUES (1, 'b') IF NOT EXISTS").(*message.RowsResult)
	require.Equal(t, []byte{0}, []byte(res.Data[0][0]))
	for i := 2; i < 40; i++ {
		c.ok(i%3, quorum, "INSERT INTO ks.u (k, email) VALUES (?, ?)", i, fmt.Sprint("e", i%4))
	}
	// Index queries gather every node's local index, each partition once.
	require.Equal(t, []string{"13", "17", "21", "25", "29", "33", "37", "5", "9"}, c.values(2, one, "SELECT k FROM ks.u WHERE email = 'e1'"))
}

var _ = errors.New

func TestClusterBootstrapDecommissionRepair(t *testing.T) {
	c := newTestCluster(t, 2)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 300; i++ {
		c.ok(i%2, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	// A third node joins: it streams the ranges it now owns.
	c.fs = append(c.fs, vfs.NewMem())
	c.addrs = append(c.addrs, freeAddr(t))
	c.nodes = append(c.nodes, nil)
	c.start(2)
	c.waitConverged()
	require.Eventually(t, func() bool {
		for _, d := range c.nodes {
			if len(d.proxy.ring.Load().Nodes()) != 3 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond, "every ring has the new node")
	local := func(n int) int {
		res := c.nodes[n].Execute(mustPrepare(t, c.nodes[n], "SELECT count(*) FROM ks.kv"), &RequestOptions{Consistency: one, local: true, forwarded: true})
		v := res.(*message.RowsResult).Data[0][0]
		return int(v[7]) | int(v[6])<<8
	}
	require.Greater(t, local(2), 100, "the new node has its share")
	c.checkReplicas(300)
	// The old replicas still hold the ranges they gave away; cleanup
	// leaves each row on its two replicas only.
	require.Greater(t, local(0)+local(1)+local(2), 600)
	for i := range c.nodes {
		n, err := c.nodes[i].Cleanup("ks")
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
	require.Equal(t, 600, local(0)+local(1)+local(2))
	c.checkReplicas(300)
	require.Len(t, c.values(2, all, "SELECT k FROM ks.kv"), 300)

	// Replicas diverge (writes to node 1's engine only, for keys it
	// replicates); repair syncs them.
	k := c.nodes[1].schema.Load().Keyspaces["ks"]
	replicaOf := func(i int, addr string) bool {
		pk := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		return containsAddr(c.nodes[1].proxy.ring.Load().Replicas(k.Replication, Token(pk)), addr)
	}
	var diverged []int
	for i := 0; len(diverged) < 20; i++ {
		if !replicaOf(i, c.addrs[1]) {
			continue
		}
		diverged = append(diverged, i)
		pr := mustPrepare(t, c.nodes[1], fmt.Sprintf("UPDATE ks.kv SET v = 'r%d' WHERE k = %d", i, i))
		e := &execCtx{d: c.nodes[1], now: time.Now(), cl: one}
		e.nowSec = e.now.Unix()
		mu := &mutation{}
		require.NoError(t, pr.state.Load().stmt.(*modification).addMutations(e, mu))
		require.NoError(t, c.nodes[1].applyLocal(mu.writes))
	}
	stats, err := c.nodes[1].Repair("ks")
	require.NoError(t, err)
	require.Positive(t, stats.Mismatches)
	for _, i := range diverged {
		for n := range c.nodes {
			if !replicaOf(i, c.addrs[n]) {
				continue
			}
			res := c.nodes[n].Execute(mustPrepare(t, c.nodes[n], fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", i)), &RequestOptions{Consistency: one, local: true, forwarded: true})
			require.Equal(t, fmt.Sprint("r", i), string(res.(*message.RowsResult).Data[0][0]), "node %d key %d", n, i)
		}
	}

	// Node 0 is the metadata service: it cannot leave.
	require.Error(t, c.nodes[0].Decommission())
	// Node 1 leaves: its data moves to the others.
	require.NoError(t, c.nodes[1].Decommission())
	require.Equal(t, cluster.StatusLeft, c.nodes[0].schema.Load().Nodes[c.nodes[1].local.HostID].Status)
	c.stop(1)
	require.Equal(t, 2, len(c.nodes[0].proxy.ring.Load().Nodes()))
	require.Equal(t, 300, local(0))
	require.Equal(t, 300, local(2))
	require.Len(t, c.values(2, all, "SELECT k FROM ks.kv"), 300)
}

// checkReplicas checks that every row k < n is stored on each of its
// replicas (read locally on each).
func (c *testCluster) checkReplicas(n int) {
	c.t.Helper()
	var p *proxy
	for _, d := range c.nodes {
		if d != nil {
			p = d.proxy
			break
		}
	}
	k := p.d.schema.Load().Keyspaces["ks"]
	for i := 0; i < n; i++ {
		pk := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		for _, r := range p.ring.Load().Replicas(k.Replication, Token(pk)) {
			for j, d := range c.nodes {
				if d == nil || c.addrs[j] != r.Addr {
					continue
				}
				res := d.Execute(mustPrepare(c.t, d, fmt.Sprintf("SELECT v FROM ks.kv WHERE k = %d", i)), &RequestOptions{Consistency: one, local: true, forwarded: true})
				require.Len(c.t, res.(*message.RowsResult).Data, 1, "row %d on replica %d", i, j)
			}
		}
	}
}

// testMembership is a fast failure detector for tests.
func testMembership() *cluster.Config {
	c := cluster.DefaultConfig()
	c.ProbeInterval, c.ProbeTimeout = 100*time.Millisecond, 50*time.Millisecond
	c.GossipInterval, c.PushPullInterval = 30*time.Millisecond, 500*time.Millisecond
	c.MinSuspicionTimeout = 300 * time.Millisecond
	return &c
}

// TestClusterPaxos runs compare-and-set increments of one cell from every
// coordinator at once: with Paxos, every applied update is based on the
// previous one, so the final value is the number applied. It then checks
// that a round accepted by a quorum but never committed is finished by the
// next SERIAL read.
func TestClusterPaxos(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.c (k int PRIMARY KEY, v int)")
	c.ok(1, quorum, "INSERT INTO ks.c (k, v) VALUES (1, 0) IF NOT EXISTS")
	serial := primitive.ConsistencyLevelSerial
	var applied, unknown atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				res := c.exec(w%3, serial, "SELECT v FROM ks.c WHERE k = 1")
				rows, ok := res.(*message.RowsResult)
				if !ok {
					continue // a timeout under contention
				}
				cur := int(int32(be32(rows.Data[0][0])))
				res = c.exec(w%3, quorum, "UPDATE ks.c SET v = ? WHERE k = 1 IF v = ?", cur+1, cur)
				switch r := res.(type) {
				case *message.RowsResult:
					if r.Data[0][0][0] == 1 {
						applied.Add(1)
					}
				case *message.WriteTimeout:
					// The outcome of a CAS that timed out is unknown: it may
					// be completed by a later round.
					require.Equal(t, primitive.WriteTypeCas, r.WriteType)
					unknown.Add(1)
				default:
					t.Errorf("unexpected %T %v", res, res)
				}
			}
		}()
	}
	wg.Wait()
	require.Positive(t, applied.Load())
	final := c.values(2, serial, "SELECT v FROM ks.c WHERE k = 1")
	v, err := strconv.Atoi(final[0])
	require.NoError(t, err)
	t.Logf("applied %d, unknown outcome %d, final %d", applied.Load(), unknown.Load(), v)
	require.GreaterOrEqual(t, int64(v), applied.Load())
	require.LessOrEqual(t, int64(v), applied.Load()+unknown.Load())
	for i := range c.nodes {
		require.Equal(t, final, c.values(i, one, "SELECT v FROM ks.c WHERE k = 1"), "node %d", i)
	}

	// A coordinator that died after its proposal was accepted by two
	// replicas: the next serial read completes the round.
	d := c.nodes[0]
	tbl := d.schema.Load().Keyspaces["ks"].Tables["c"]
	pk := []byte{0, 0, 0, 1}
	pr := mustPrepare(t, d, "UPDATE ks.c SET v = 1000 WHERE k = 1")
	b := newBallot(time.Now().UnixMicro())
	e := &execCtx{d: d, now: time.Now(), ts: b.micros()}
	e.nowSec = e.now.Unix()
	mu := &mutation{}
	require.NoError(t, pr.state.Load().stmt.(*modification).addMutations(e, mu))
	for _, n := range c.nodes[:2] {
		r := &paxosRequest{Table: uint64(tbl.Storage), Key: pk, Ballot: b}
		resp, err := n.paxosPrepare(r)
		require.NoError(t, err)
		require.True(t, resp.OK)
		r.Update = encodeWrites(mu.writes)
		resp, err = n.paxosAccept(r)
		require.NoError(t, err)
		require.True(t, resp.OK)
	}
	require.Equal(t, final, c.values(2, one, "SELECT v FROM ks.c WHERE k = 1"), "not committed yet")
	require.Equal(t, []string{"1000"}, c.values(2, serial, "SELECT v FROM ks.c WHERE k = 1"))
	require.Equal(t, []string{"1000"}, c.values(2, one, "SELECT v FROM ks.c WHERE k = 1"), "committed everywhere")

	// One replica down: quorum still decides; two down: unavailable.
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) }, 5*time.Second, 20*time.Millisecond)
	res := c.ok(0, quorum, "UPDATE ks.c SET v = 1001 WHERE k = 1 IF v = 1000").(*message.RowsResult)
	require.Equal(t, []byte{1}, []byte(res.Data[0][0]))
	c.stop(1)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[1]) }, 5*time.Second, 20*time.Millisecond)
	_, ok := c.exec(0, quorum, "UPDATE ks.c SET v = 1002 WHERE k = 1 IF v = 1001").(*message.Unavailable)
	require.True(t, ok)
}
