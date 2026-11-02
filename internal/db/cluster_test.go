package db

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"net"
	"sort"
	"strconv"
	"strings"
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
	dcs   []string // data center per node ("" for the default)
	tweak func(*ClusterOptions)
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

func newTestCluster(t *testing.T, n int, dcs ...string) *testCluster {
	c := &testCluster{t: t, dcs: dcs}
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

func (c *testCluster) start(i int) { c.startReplacing(i, "") }

// add adds a node (not started).
func (c *testCluster) add() int {
	c.fs = append(c.fs, vfs.NewMem())
	c.addrs = append(c.addrs, freeAddr(c.t))
	c.nodes = append(c.nodes, nil)
	return len(c.nodes) - 1
}

func (c *testCluster) startReplacing(i int, replace string) {
	d, err := c.open(i, replace, nil)
	require.NoError(c.t, err)
	c.nodes[i] = d
}

func (c *testCluster) open(i int, replace string, hook func(op, unit string) error) (*DB, error) {
	o := Options{FS: c.fs[i], Address: net.IPv4(127, 0, 0, byte(1+i)), StreamHook: hook}
	if i < len(c.dcs) {
		o.DataCenter = c.dcs[i]
	}
	co := &ClusterOptions{
		ListenAddress: c.addrs[i], Seeds: []string{c.addrs[0]}, NumTokens: 8, ReplaceAddress: replace, Messaging: cluster.MessagingOptions{Compress: true},
		Membership:   testMembership(),
		WriteTimeout: time.Second, ReadTimeout: time.Second,
	}
	if c.tweak != nil {
		c.tweak(co)
	}
	return Open("db", Options{FS: o.FS, Address: o.Address, DataCenter: o.DataCenter, StreamHook: hook, Cluster: co})
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
	// (QUORUM acknowledges before the third replica has applied a write.)
	for n := range c.nodes {
		require.Eventually(t, func() bool {
			res := c.nodes[n].Execute(mustPrepare(t, c.nodes[n], "SELECT count(*) FROM ks.kv"), &RequestOptions{Consistency: one, local: true, forwarded: true})
			return bytes.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 100}, res.(*message.RowsResult).Data[0][0])
		}, 5*time.Second, 10*time.Millisecond, "node %d", n)
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
	// The commit is acknowledged at QUORUM; the last replica learns it
	// shortly after.
	for i := range c.nodes {
		require.Eventually(t, func() bool {
			got := c.values(i, one, "SELECT v FROM ks.c WHERE k = 1")
			return len(got) == 1 && got[0] == final[0]
		}, 5*time.Second, 10*time.Millisecond, "node %d", i)
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
	require.Eventually(t, func() bool {
		got := c.values(2, one, "SELECT v FROM ks.c WHERE k = 1")
		return len(got) == 1 && got[0] == "1000"
	}, 5*time.Second, 10*time.Millisecond, "committed everywhere")

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

// TestClusterOperations covers the metadata log, reconfiguring the
// metadata service, removing a dead node and replacing one.
func TestClusterOperations(t *testing.T) {
	c := newTestCluster(t, 4)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	for i := 0; i < 200; i++ {
		c.ok(i%4, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i))
	}
	kinds := c.values(1, one, "SELECT kind FROM system_views.cluster_metadata_log")
	require.Contains(t, kinds, "REGISTER")
	require.Contains(t, kinds, "SCHEMA_CHANGE")

	// A three-member metadata service.
	require.NoError(t, c.nodes[1].ReconfigureCMS([]string{c.addrs[0], c.addrs[1], c.addrs[2]}))
	require.Eventually(t, func() bool { return len(c.nodes[3].schema.Load().CMS) == 3 }, 5*time.Second, 10*time.Millisecond)
	require.Len(t, c.values(3, one, "SELECT host_id FROM system_views.cluster_metadata_directory WHERE cms_member = true ALLOW FILTERING"), 3)
	c.ok(2, quorum, "CREATE TABLE ks.other (k int PRIMARY KEY)")

	// Node 3 dies and is removed: its ranges get new replicas.
	c.stop(3)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[3]) }, 5*time.Second, 10*time.Millisecond)
	require.Error(t, c.nodes[0].RemoveNode(c.addrs[1]), "a live node")
	require.NoError(t, c.nodes[0].RemoveNode(c.addrs[3]))
	c.nodes, c.fs, c.addrs = c.nodes[:3], c.fs[:3], c.addrs[:3]
	require.Eventually(t, func() bool { return len(c.nodes[2].proxy.ring.Load().Nodes()) == 3 }, 5*time.Second, 10*time.Millisecond)
	c.checkReplicas(200)

	// Node 2, a metadata service member, dies: the service shrinks to two
	// members (still a quorum of the three), and a new node replaces it.
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, c.nodes[0].ReconfigureCMS([]string{c.addrs[0], c.addrs[1]}))
	oldID := c.nodes[0].schema.Load().nodeByAddr(c.addrs[2]).HostID
	oldTokens := c.nodes[0].schema.Load().nodeByAddr(c.addrs[2]).Tokens
	n := c.add()
	c.startReplacing(n, c.addrs[2])
	c.nodes = []*DB{c.nodes[0], c.nodes[1], c.nodes[n]}
	c.addrs = []string{c.addrs[0], c.addrs[1], c.addrs[n]}
	s := c.nodes[0].schema.Load()
	require.Equal(t, "LEFT", s.Nodes[oldID].Status)
	me := s.Nodes[c.nodes[2].local.HostID]
	require.Equal(t, "NORMAL", me.Status)
	require.ElementsMatch(t, oldTokens, me.Tokens)
	c.checkReplicas(200)
	require.Len(t, c.values(2, all, "SELECT k FROM ks.kv"), 200)
}

// TestClusterEachQuorum checks EACH_QUORUM across two data centers.
func TestClusterEachQuorum(t *testing.T) {
	c := newTestCluster(t, 3, "dc1", "dc1", "dc2")
	each := primitive.ConsistencyLevelEachQuorum
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	for i := 0; i < 20; i++ {
		c.ok(i%3, each, "INSERT INTO ks.kv (k, v) VALUES (?, 'x')", i)
	}
	require.Len(t, c.values(1, each, "SELECT k FROM ks.kv"), 20)
	require.Equal(t, []string{"x"}, c.values(2, each, "SELECT v FROM ks.kv WHERE k = 3"))
	// dc2 down: EACH_QUORUM is unavailable, QUORUM (2 of 3) is not.
	c.stop(2)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[2]) }, 5*time.Second, 10*time.Millisecond)
	_, ok := c.exec(0, each, "INSERT INTO ks.kv (k, v) VALUES (1, 'y')").(*message.Unavailable)
	require.True(t, ok)
	_, ok = c.exec(0, each, "SELECT v FROM ks.kv WHERE k = 1").(*message.Unavailable)
	require.True(t, ok)
	c.ok(0, quorum, "INSERT INTO ks.kv (k, v) VALUES (1, 'y')")
	c.ok(1, primitive.ConsistencyLevelLocalQuorum, "INSERT INTO ks.kv (k, v) VALUES (2, 'y')")
}

// TestClusterCounters updates counters of many partitions per statement
// (IN, counter batches) from every coordinator concurrently.
func TestClusterCounters(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.cnt (k int PRIMARY KEY, n counter)")
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				c.ok(w%3, quorum, "UPDATE ks.cnt SET n = n + 1 WHERE k IN (1, 2, 3, 4, 5, 6, 7, 8)")
				c.ok(w%3, quorum, "BEGIN COUNTER BATCH UPDATE ks.cnt SET n = n + 2 WHERE k = 1; UPDATE ks.cnt SET n = n + 3 WHERE k = 9; UPDATE ks.cnt SET n = n + 1 WHERE k = 1; APPLY BATCH")
			}
		}()
	}
	wg.Wait()
	for n := range c.nodes {
		got := c.values(n, quorum, "SELECT k, n FROM ks.cnt")
		want := []string{}
		for _, k := range []int{5, 1, 2, 8, 9, 3, 4, 7, 6} {
			v := 60
			switch k {
			case 1:
				v = 60 + 180
			case 9:
				v = 180
			}
			want = append(want, fmt.Sprintf("%d %s", k, string([]byte{0, 0, 0, 0, 0, 0, byte(v >> 8), byte(v)})))
		}
		require.ElementsMatch(t, want, got, "node %d", n)
	}
}

// TestClusterVectorSearch runs ANN queries from every node: each node
// searches its own index for the partitions it is the first replica of,
// and the coordinator merges by score.
func TestClusterVectorSearch(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.v (id int PRIMARY KEY, emb vector<float, 4>)")
	c.ok(1, quorum, "CREATE INDEX ON ks.v (emb) USING 'sai' WITH OPTIONS = {'similarity_function': 'euclidean'}")
	time.Sleep(100 * time.Millisecond)
	vecOf := func(i int) []float32 { return []float32{float32(i), float32(i % 7), float32(i % 3), 1} }
	for i := 0; i < 150; i++ {
		c.ok(i%3, quorum, "INSERT INTO ks.v (id, emb) VALUES (?, ?)", i, string(vec(vecOf(i)...)))
	}
	for n := range c.nodes {
		got := c.values(n, one, "SELECT id FROM ks.v ORDER BY emb ANN OF ? LIMIT 3", string(vec(vecOf(40)...)))
		require.ElementsMatch(t, []string{"40", "39", "41"}, got, "node %d", n) // (values sorts)
		rows := c.ok(n, one, "SELECT id FROM ks.v ORDER BY emb ANN OF ? LIMIT 3", string(vec(vecOf(40)...))).(*message.RowsResult)
		require.Equal(t, []byte{0, 0, 0, 40}, []byte(rows.Data[0][0]), "nearest first")
		require.Len(t, rows.Metadata.Columns, 1, "no score column")
	}
}

// TestClusterIndexPaging pages through a secondary index query in a
// cluster: the coordinator walks the token ranges, each page resuming
// where the last stopped, with no row missed or repeated, and LIMIT holds
// across pages.
func TestClusterIndexPaging(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.u (k int, c int, color text, PRIMARY KEY (k, c))")
	c.ok(1, quorum, "CREATE INDEX ON ks.u (color)")
	time.Sleep(100 * time.Millisecond)
	for k := 0; k < 100; k++ {
		for cc := 0; cc < 3; cc++ {
			color := "red"
			if (k+cc)%4 == 0 {
				color = "blue"
			}
			c.ok(k%3, quorum, "INSERT INTO ks.u (k, c, color) VALUES (?, ?, ?)", k, cc, color)
		}
	}
	page := func(node int, q string, size int32) []string {
		d := c.nodes[node]
		pr := mustPrepare(t, d, q)
		var all []string
		var state []byte
		for pages := 0; ; pages++ {
			require.Less(t, pages, 1000)
			res := d.Execute(pr, &RequestOptions{Consistency: one, PageSize: size, PagingState: state})
			rows, ok := res.(*message.RowsResult)
			require.True(t, ok, "%v", res)
			require.LessOrEqual(t, len(rows.Data), int(size))
			for _, r := range rows.Data {
				all = append(all, fmt.Sprintf("%d/%d", be32(r[0]), be32(r[1])))
			}
			if rows.Metadata.PagingState == nil {
				return all
			}
			state = rows.Metadata.PagingState
		}
	}
	want := 0
	for k := 0; k < 100; k++ {
		for cc := 0; cc < 3; cc++ {
			if (k+cc)%4 != 0 {
				want++
			}
		}
	}
	for n := range c.nodes {
		got := page(n, "SELECT k, c FROM ks.u WHERE color = 'red'", 25)
		require.Len(t, got, want, "node %d", n)
		seen := map[string]bool{}
		for _, g := range got {
			require.False(t, seen[g], "node %d: %s twice", n, g)
			seen[g] = true
		}
	}
	got := page(2, "SELECT k, c FROM ks.u WHERE color = 'red' LIMIT 60", 25)
	require.Len(t, got, 60)
}

// TestClusterResumableBootstrap interrupts a bootstrap and restarts the
// node: it streams only the ranges it had not finished, ends with every
// row and indexes them.
func TestClusterResumableBootstrap(t *testing.T) {
	c := newTestCluster(t, 2)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	c.ok(0, quorum, "CREATE INDEX ON ks.kv (v)")
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 300; i++ {
		c.ok(i%2, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint("v", i%10))
	}
	n := c.add()
	var first []string
	_, err := c.open(n, "", func(op, unit string) error {
		if len(first) == 3 {
			return errors.New("injected failure")
		}
		first = append(first, unit)
		return nil
	})
	require.ErrorContains(t, err, "injected failure")
	var second []string
	d, err := c.open(n, "", func(op, unit string) error {
		require.NotContains(t, first, unit, "a finished range is not streamed again")
		second = append(second, unit)
		return nil
	})
	require.NoError(t, err)
	c.nodes[n] = d
	require.NotEmpty(t, second)
	require.Equal(t, "NORMAL", c.nodes[0].schema.Load().Nodes[d.local.HostID].Status)
	require.Eventually(t, func() bool { return len(c.nodes[0].proxy.ring.Load().Nodes()) == 3 }, 5*time.Second, 10*time.Millisecond)
	c.checkReplicas(300)
	// The new node's index covers its rows: index queries through it.
	require.Len(t, c.values(n, one, "SELECT k FROM ks.kv WHERE v = 'v3'"), 30)
	_, err = d.readFile(streamProgressFile)
	require.NoError(t, err)
	require.Empty(t, d.progress().m["bootstrap"], "the progress is cleared once joined")
}

// TestClusterHintWindowAndTTL checks that a node down longer than the hint
// window gets no more hints, and that hints expire with their tables'
// gc_grace_seconds.
func TestClusterHintWindowAndTTL(t *testing.T) {
	c := &testCluster{t: t, tweak: func(o *ClusterOptions) { o.MaxHintWindow = 1500 * time.Millisecond }}
	for i := 0; i < 2; i++ {
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
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	c.ok(0, quorum, "CREATE TABLE ks.short (k int PRIMARY KEY, v text) WITH gc_grace_seconds = 1")
	time.Sleep(100 * time.Millisecond)
	c.stop(1)
	require.Eventually(t, func() bool { return !c.nodes[0].proxy.g.IsAlive(c.addrs[1]) }, 5*time.Second, 10*time.Millisecond)
	c.ok(0, one, "INSERT INTO ks.kv (k, v) VALUES (1, 'hinted')")
	c.ok(0, one, "INSERT INTO ks.short (k, v) VALUES (1, 'expires')")
	time.Sleep(2 * time.Second) // past the window and the short table's gc_grace
	c.ok(0, one, "INSERT INTO ks.kv (k, v) VALUES (2, 'not hinted')")
	c.start(1)
	c.waitConverged()
	local := func(q string) int {
		res := c.nodes[1].Execute(mustPrepare(t, c.nodes[1], q), &RequestOptions{Consistency: one, local: true, forwarded: true})
		return len(res.(*message.RowsResult).Data)
	}
	require.Eventually(t, func() bool { return local("SELECT k FROM ks.kv WHERE k = 1") == 1 }, 10*time.Second, 50*time.Millisecond, "the hint within the window is delivered")
	time.Sleep(300 * time.Millisecond)
	require.Zero(t, local("SELECT k FROM ks.kv WHERE k = 2"), "no hint once the node was down past the window")
	require.Zero(t, local("SELECT k FROM ks.short WHERE k = 1"), "the hint expired with gc_grace_seconds")
}

func TestClusterThreadPools(t *testing.T) {
	c := newTestCluster(t, 2)
	rows := c.values(1, one, "SELECT name, active_tasks_limit FROM system_views.thread_pools")
	require.Contains(t, rows, "ReadStage 64")
	require.Contains(t, rows, "StreamingStage 4")
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	for i := 0; i < 50; i++ {
		c.ok(0, quorum, "INSERT INTO ks.kv (k, v) VALUES (?, 'x')", i)
	}
	// Each node reports its primary ranges: together, the whole ring.
	total := 0
	for n := range c.nodes {
		total += len(c.values(n, one, "SELECT range_start FROM system.size_estimates WHERE keyspace_name = 'ks'"))
	}
	require.Equal(t, len(c.nodes[0].proxy.tokenSegments()), total)
}

// TestClusterTracing traces a write and a read on a coordinator: the
// events name the replicas asked and their answers.
func TestClusterTracing(t *testing.T) {
	c := newTestCluster(t, 3)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(100 * time.Millisecond)
	d := c.nodes[1]
	tr := NewTrace("INSERT", "127.0.0.9", nil)
	res := d.Execute(mustPrepare(t, d, "INSERT INTO ks.kv (k, v) VALUES (1, 'a')"), &RequestOptions{Consistency: quorum, Trace: tr})
	_, isErr := res.(message.Error)
	require.False(t, isErr, "%v", res)
	d.FinishTrace(tr)
	tr2 := NewTrace("SELECT", "127.0.0.9", nil)
	d.Execute(mustPrepare(t, d, "SELECT v FROM ks.kv WHERE k = 1"), &RequestOptions{Consistency: quorum, Trace: tr2})
	d.FinishTrace(tr2)
	events := c.values(1, one, "SELECT activity FROM system_traces.events")
	joined := strings.Join(events, "\n")
	require.Contains(t, joined, "Writing 1 partitions at QUORUM to 3 replicas")
	require.Contains(t, joined, "MUTATION acknowledged by")
	require.Contains(t, joined, "Reading a row at QUORUM")
	require.Len(t, c.values(1, one, "SELECT session_id FROM system_traces.sessions"), 2)
}
