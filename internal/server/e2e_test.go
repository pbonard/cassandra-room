package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/db"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
)

// startServer serves a fresh in-memory database on a local TCP port.
func startServer(t *testing.T) (host string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port = ln.Addr().(*net.TCPAddr).Port
	d, err := db.Open("db", db.Options{FS: vfs.NewMem(), Port: port})
	require.NoError(t, err)
	srv := New(d)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = d.Close()
	})
	return "127.0.0.1", port
}

func connect(t *testing.T, host string, port int, proto int, keyspace string) *gocql.Session {
	t.Helper()
	cluster := gocql.NewCluster(host + ":" + strconv.Itoa(port))
	cluster.ProtoVersion = proto
	cluster.Keyspace = keyspace
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.DisableInitialHostLookup = false
	s, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

// TestGocql runs a workload through the gocql driver: driver handshake and
// metadata queries, DDL, prepared statements of every common type, paging,
// batches, lightweight transactions and errors.
func TestGocql(t *testing.T) {
	host, port := startServer(t)
	for _, proto := range []int{3, 4} {
		t.Run("v"+strconv.Itoa(proto), func(t *testing.T) {
			ks := "e2e_v" + strconv.Itoa(proto)
			s := connect(t, host, port, proto, "")
			require.NoError(t, s.Query("CREATE KEYSPACE "+ks+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
			s = connect(t, host, port, proto, ks)

			require.NoError(t, s.Query(`CREATE TABLE users (
				id uuid PRIMARY KEY, name text, age int, score double, active boolean,
				created timestamp, balance decimal, big varint, tags frozen<set<text>>, attrs frozen<map<text, int>>,
				ip inet, born date, avatar blob)`).Exec())
			id := gocql.TimeUUID()
			created := time.UnixMilli(time.Now().UnixMilli()).UTC()
			born := time.Date(1990, 5, 17, 0, 0, 0, 0, time.UTC)
			require.NoError(t, s.Query(`INSERT INTO users (id, name, age, score, active, created, balance, big, tags, attrs, ip, born, avatar)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				id, "alice", 33, 4.5, true, created, inf.NewDec(12345, 2), int64(1)<<40,
				[]string{"b", "a", "a"}, map[string]int{"x": 1, "y": 2}, net.ParseIP("10.1.2.3"), born, []byte{1, 2, 3}).Exec())

			var (
				name    string
				age     int
				score   float64
				active  bool
				gotTime time.Time
				balance *inf.Dec
				big     int64
				tags    []string
				attrs   map[string]int
				ip      net.IP
				gotBorn time.Time
				avatar  []byte
			)
			require.NoError(t, s.Query(`SELECT name, age, score, active, created, balance, big, tags, attrs, ip, born, avatar FROM users WHERE id = ?`, id).
				Scan(&name, &age, &score, &active, &gotTime, &balance, &big, &tags, &attrs, &ip, &gotBorn, &avatar))
			require.Equal(t, "alice", name)
			require.Equal(t, 33, age)
			require.Equal(t, 4.5, score)
			require.True(t, active)
			require.True(t, created.Equal(gotTime))
			require.Equal(t, "123.45", balance.String())
			require.Equal(t, int64(1)<<40, big)
			require.Equal(t, []string{"a", "b"}, tags)
			require.Equal(t, map[string]int{"x": 1, "y": 2}, attrs)
			require.Equal(t, "10.1.2.3", ip.String())
			require.True(t, born.Equal(gotBorn))
			require.Equal(t, []byte{1, 2, 3}, avatar)

			// Clustering, paging and reversed order.
			require.NoError(t, s.Query(`CREATE TABLE events (sensor text, at timestamp, value double, PRIMARY KEY (sensor, at)) WITH CLUSTERING ORDER BY (at DESC)`).Exec())
			base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			b := s.NewBatch(gocql.UnloggedBatch)
			for i := 0; i < 250; i++ {
				b.Query("INSERT INTO events (sensor, at, value) VALUES (?, ?, ?)", "s1", base.Add(time.Duration(i)*time.Minute), float64(i))
			}
			require.NoError(t, s.ExecuteBatch(b))
			iter := s.Query("SELECT at, value FROM events WHERE sensor = ?", "s1").PageSize(40).Iter()
			var at time.Time
			var value float64
			n := 0
			prev := 1e9
			for iter.Scan(&at, &value) {
				require.Less(t, value, prev, "descending")
				prev = value
				n++
			}
			require.NoError(t, iter.Close())
			require.Equal(t, 250, n)
			var first float64
			require.NoError(t, s.Query("SELECT value FROM events WHERE sensor = ? AND at >= ? ORDER BY at ASC LIMIT 1", "s1", base.Add(100*time.Minute)).Scan(&first))
			require.Equal(t, 100.0, first)
			var count int64
			require.NoError(t, s.Query("SELECT count(*) FROM events WHERE sensor = 's1' AND at < ?", base.Add(10*time.Minute)).Scan(&count))
			require.Equal(t, int64(10), count)

			// Lightweight transactions.
			m := map[string]any{}
			applied, err := s.Query("INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS", id, "bob").MapScanCAS(m)
			require.NoError(t, err)
			require.False(t, applied)
			require.Equal(t, "alice", m["name"])
			applied, err = s.Query("UPDATE users SET name = ? WHERE id = ? IF name = ?", "carol", id, "alice").ScanCAS()
			require.NoError(t, err)
			require.True(t, applied)

			// TTL and writetime.
			require.NoError(t, s.Query("UPDATE users USING TTL 100 SET age = 34 WHERE id = ?", id).Exec())
			var ttl int
			var wt int64
			require.NoError(t, s.Query("SELECT ttl(age), writetime(age) FROM users WHERE id = ?", id).Scan(&ttl, &wt))
			require.InDelta(t, 100, ttl, 1)
			require.InDelta(t, time.Now().UnixMicro(), wt, float64(10*time.Second/time.Microsecond))

			// Errors map to the driver's error types.
			err = s.Query("SELECT nope FROM users").Exec()
			var reqErr gocql.RequestError
			require.ErrorAs(t, err, &reqErr)
			require.Equal(t, gocql.ErrCodeInvalid, reqErr.Code())
			err = s.Query("CREATE TABLE users (id int PRIMARY KEY)").Exec()
			require.ErrorAs(t, err, &reqErr)
			require.Equal(t, gocql.ErrCodeAlreadyExists, reqErr.Code())
			err = s.Query("SELEC x").Exec()
			require.ErrorAs(t, err, &reqErr)
			require.Equal(t, gocql.ErrCodeSyntax, reqErr.Code())

			// Multi-cell columns and counters.
			require.NoError(t, s.Query(`CREATE TYPE addr (street text, zip int)`).Exec())
			require.NoError(t, s.Query(`CREATE TABLE prof (id int PRIMARY KEY, tags set<text>, hist list<int>, kv map<text, int>, home addr)`).Exec())
			require.NoError(t, s.Query(`UPDATE prof SET tags = tags + ?, hist = hist + ?, kv = kv + ?, home = ? WHERE id = 1`,
				[]string{"x", "y"}, []int{1, 2}, map[string]int{"a": 1}, map[string]any{"street": "Main", "zip": 7}).Exec())
			require.NoError(t, s.Query(`UPDATE prof SET hist = [0] + hist, kv['b'] = 2, home.zip = 8 WHERE id = 1`).Exec())
			var gotTags []string
			var hist []int
			var kv map[string]int
			home := map[string]any{}
			require.NoError(t, s.Query(`SELECT tags, hist, kv, home FROM prof WHERE id = 1`).Scan(&gotTags, &hist, &kv, &home))
			require.Equal(t, []string{"x", "y"}, gotTags)
			require.Equal(t, []int{0, 1, 2}, hist)
			require.Equal(t, map[string]int{"a": 1, "b": 2}, kv)
			require.Equal(t, "Main", home["street"])
			require.Equal(t, 8, home["zip"])
			require.NoError(t, s.Query(`CREATE TABLE views (page text PRIMARY KEY, n counter)`).Exec())
			for i := 0; i < 5; i++ {
				require.NoError(t, s.Query(`UPDATE views SET n = n + ? WHERE page = ?`, int64(3), "home").Exec())
			}
			var views int64
			require.NoError(t, s.Query(`SELECT n FROM views WHERE page = 'home'`).Scan(&views))
			require.Equal(t, int64(15), views)

			// Schema metadata through the driver.
			md, err := s.KeyspaceMetadata(ks)
			require.NoError(t, err)
			require.Contains(t, md.Tables, "events")
			require.Equal(t, []string{"sensor"}, columnNames(md.Tables["events"].PartitionKey))
			require.Equal(t, []string{"at"}, columnNames(md.Tables["events"].ClusteringColumns))
		})
	}
}

func columnNames(cols []*gocql.ColumnMetadata) []string {
	var out []string
	for _, c := range cols {
		out = append(out, c.Name)
	}
	return out
}

func TestGocqlAuth(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	d, err := db.Open("db", db.Options{FS: vfs.NewMem(), Port: port, Authenticator: true, Authorizer: true})
	require.NoError(t, err)
	srv := New(d)
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		_ = srv.Shutdown(context.Background())
		_ = d.Close()
	}()
	login := func(user, pass string) (*gocql.Session, error) {
		cluster := gocql.NewCluster("127.0.0.1:" + strconv.Itoa(port))
		cluster.ProtoVersion = 4
		cluster.Timeout = 5 * time.Second
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: user, Password: pass}
		return cluster.CreateSession()
	}
	_, err = login("cassandra", "wrong")
	require.Error(t, err)
	admin, err := login("cassandra", "cassandra")
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.Query("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
	require.NoError(t, admin.Query("CREATE TABLE ks.t (k int PRIMARY KEY)").Exec())
	require.NoError(t, admin.Query("CREATE ROLE app WITH PASSWORD = 'secret' AND LOGIN = true").Exec())
	app, err := login("app", "secret")
	require.NoError(t, err)
	defer app.Close()
	err = app.Query("SELECT * FROM ks.t").Exec()
	var reqErr gocql.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, gocql.ErrCodeUnauthorized, reqErr.Code())
	require.NoError(t, admin.Query("GRANT SELECT ON ks.t TO app").Exec())
	require.NoError(t, app.Query("SELECT * FROM ks.t").Exec())
}

// TestGocqlCluster runs gocql against three nodes on 127.0.0.1-3 (same
// native port), found through system.peers.
func TestGocqlCluster(t *testing.T) {
	var lns []net.Listener
	var port int
	for attempt := 0; attempt < 20 && len(lns) < 3; attempt++ {
		for _, l := range lns {
			l.Close()
		}
		lns = nil
		first, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port = first.Addr().(*net.TCPAddr).Port
		lns = append(lns, first)
		for i := 2; i <= 3; i++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.%d:%d", i, port))
			if err != nil {
				break
			}
			lns = append(lns, l)
		}
	}
	require.Len(t, lns, 3)
	var storage []string
	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.%d:0", i+1))
		require.NoError(t, err)
		storage = append(storage, l.Addr().String())
		l.Close()
	}
	for i := 0; i < 3; i++ {
		d, err := db.Open("db", db.Options{FS: vfs.NewMem(), Address: net.IPv4(127, 0, 0, byte(i+1)), Port: port,
			Cluster: &db.ClusterOptions{ListenAddress: storage[i], Seeds: []string{storage[0]}, NumTokens: 8,
				Membership: fastMembership()}})
		require.NoError(t, err)
		srv := New(d)
		go func(ln net.Listener) { _ = srv.Serve(ln) }(lns[i])
		t.Cleanup(func() {
			_ = srv.Shutdown(context.Background())
			_ = d.Close()
		})
	}
	time.Sleep(500 * time.Millisecond) // gossip settles
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = port
	cluster.ProtoVersion = 4
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	s, err := cluster.CreateSession()
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Query("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").Exec())
	require.NoError(t, s.Query("CREATE TABLE ks.kv (k int PRIMARY KEY, v text)").Exec())
	time.Sleep(300 * time.Millisecond) // schema reaches every node
	for i := 0; i < 200; i++ {
		require.NoError(t, s.Query("INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, fmt.Sprint(i)).Exec())
	}
	var n int
	require.NoError(t, s.Query("SELECT count(*) FROM ks.kv").Scan(&n))
	require.Equal(t, 200, n)
	hosts := map[string]bool{}
	iter := s.Query("SELECT k, v FROM ks.kv WHERE k = 5").Consistency(gocql.All).Iter()
	var k int
	var v string
	for iter.Scan(&k, &v) {
		hosts[iter.Host().ConnectAddress().String()] = true
	}
	require.NoError(t, iter.Close())
	require.Equal(t, "5", v)
	// The driver sees all three nodes and their tokens.
	require.Eventually(t, func() bool {
		seen := map[string]bool{}
		for i := 0; i < 50; i++ {
			it := s.Query("SELECT v FROM ks.kv WHERE k = ?", i).Iter()
			seen[it.Host().ConnectAddress().String()] = true
			it.Close()
		}
		return len(seen) == 3
	}, 5*time.Second, 100*time.Millisecond, "token-aware routing reaches every node")
}

func fastMembership() *cluster.Config {
	c := cluster.DefaultConfig()
	c.ProbeInterval, c.ProbeTimeout = 100*time.Millisecond, 50*time.Millisecond
	c.GossipInterval, c.PushPullInterval = 30*time.Millisecond, 500*time.Millisecond
	c.MinSuspicionTimeout = 500 * time.Millisecond
	return &c
}

// TestGocqlVectors writes and reads vectors and runs an ANN query through
// gocql. (gocql v1.7 has no vector codec: values go as the custom type's
// raw bytes.)
func TestGocqlVectors(t *testing.T) {
	host, port := startServer(t)
	s := connect(t, host, port, 4, "")
	require.NoError(t, s.Query("CREATE KEYSPACE vec WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
	s = connect(t, host, port, 4, "vec")
	require.NoError(t, s.Query("CREATE TABLE docs (id int PRIMARY KEY, emb vector<float, 3>)").Exec())
	require.NoError(t, s.Query("CREATE INDEX ON docs (emb) USING 'sai'").Exec())
	vec := func(xs ...float32) rawValue {
		var b []byte
		for _, x := range xs {
			b = binary.BigEndian.AppendUint32(b, math.Float32bits(x))
		}
		return b
	}
	for i, v := range [][]float32{{1, 0, 0}, {0, 1, 0}, {0.8, 0.2, 0}} {
		require.NoError(t, s.Query("INSERT INTO docs (id, emb) VALUES (?, ?)", i, vec(v...)).Exec())
	}
	var got rawValue
	require.NoError(t, s.Query("SELECT emb FROM docs WHERE id = 2").Scan(&got))
	require.Equal(t, vec(0.8, 0.2, 0), got)
	it := s.Query("SELECT id FROM docs ORDER BY emb ANN OF ? LIMIT 2", vec(1, 0, 0)).Iter()
	var ids []int
	var id int
	for it.Scan(&id) {
		ids = append(ids, id)
	}
	require.NoError(t, it.Close())
	require.Equal(t, []int{0, 2}, ids)
}

// rawValue passes a value's serialized bytes through gocql.
type rawValue []byte

func (r rawValue) MarshalCQL(gocql.TypeInfo) ([]byte, error) { return r, nil }
func (r *rawValue) UnmarshalCQL(_ gocql.TypeInfo, b []byte) error {
	*r = append((*r)[:0], b...)
	return nil
}

// TestGocqlTracing traces queries through gocql, whose tracer reads the
// session and its events back from system_traces, as cqlsh's TRACING ON
// does.
func TestGocqlTracing(t *testing.T) {
	host, port := startServer(t)
	s := connect(t, host, port, 4, "")
	require.NoError(t, s.Query("CREATE KEYSPACE tr WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
	s = connect(t, host, port, 4, "tr")
	require.NoError(t, s.Query("CREATE TABLE kv (k int PRIMARY KEY, v text)").Exec())
	var buf bytes.Buffer
	tracer := gocql.NewTraceWriter(s, &buf)
	require.NoError(t, s.Query("INSERT INTO kv (k, v) VALUES (1, 'a')").Trace(tracer).Exec())
	out := buf.String()
	require.Contains(t, out, "Executing modification of tr.kv at QUORUM")
	require.Contains(t, out, "Applying 1 writes locally")
	require.Contains(t, out, "Request complete")
	buf.Reset()
	var v string
	require.NoError(t, s.Query("SELECT v FROM kv WHERE k = 1").Trace(tracer).Scan(&v))
	require.Equal(t, "a", v)
	require.Contains(t, buf.String(), "Reading tr.kv locally")
}
