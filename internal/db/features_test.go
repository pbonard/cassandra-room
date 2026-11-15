package db

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/datastax/go-cassandra-native-protocol/message"

	"github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
	s := newKS(t)
	s.ok(`CREATE TABLE j (k int, "Ck" text, v double, s set<int>, m map<text, int>, ts timestamp, b blob, PRIMARY KEY (k, "Ck"))`)
	s.ok(`INSERT INTO j JSON '{"k": 1, "\"Ck\"": "a", "v": 1.5, "s": [3, 1, 3], "m": {"x": 1}, "ts": "2020-01-02 03:04:05.000Z", "b": "0xcafe"}'`)
	require.Equal(t, []string{`{"k": 1, "\"Ck\"": "a", "b": "0xcafe", "m": {"x": 1}, "s": [1, 3], "ts": "2020-01-02 03:04:05.000Z", "v": 1.5}`},
		s.rows("SELECT JSON * FROM j"))
	s.ok(`INSERT INTO j JSON '{"k": 1, "\"Ck\"": "a", "v": 2}' DEFAULT UNSET`)
	require.Equal(t, []string{"2.0 {'x': 1}"}, s.cqlRows(`SELECT v, m FROM j WHERE k = 1`))
	s.ok(`INSERT INTO j JSON '{"k": 1, "\"Ck\"": "a"}'`) // DEFAULT NULL
	require.Equal(t, []string{"null null"}, s.cqlRows(`SELECT v, m FROM j WHERE k = 1`))
	s.fails(`INSERT INTO j JSON '{"k": 1, "nope": 2}'`, "unrecognized column")
	s.ok(`UPDATE j SET s = fromJson('[4, 3]') WHERE k = 2 AND "Ck" = 'b'`)
	require.Equal(t, []string{`[3, 4]`}, s.rows(`SELECT toJson(s) FROM j WHERE k = 2`))
}

func TestGroupByAndArithmetic(t *testing.T) {
	s := newKS(t)
	s.ok("CREATE TABLE g (p int, c1 int, c2 int, v bigint, PRIMARY KEY (p, c1, c2))")
	for p := 0; p < 2; p++ {
		for c1 := 0; c1 < 3; c1++ {
			for c2 := 0; c2 < 2; c2++ {
				s.ok("INSERT INTO g (p, c1, c2, v) VALUES (?, ?, ?, ?)", p, c1, c2, p*100+c1*10+c2)
			}
		}
	}
	// Partitions come in token order.
	require.Equal(t, []string{"1 6 663", "0 6 63"}, s.cqlRows("SELECT p, count(*), sum(v) FROM g GROUP BY p"))
	require.Equal(t, []string{"0 0 1", "0 1 21", "0 2 41"}, s.cqlRows("SELECT p, c1, sum(v) FROM g WHERE p = 0 GROUP BY p, c1"))
	require.Equal(t, []string{"0 0 1", "0 1 21"}, s.cqlRows("SELECT p, c1, sum(v) FROM g WHERE p = 0 GROUP BY p, c1 LIMIT 2"))
	s.fails("SELECT count(*) FROM g GROUP BY c1", "following their declared order")
	s.fails("SELECT count(*) FROM g GROUP BY v", "only supported on the columns of the PRIMARY KEY")
	require.Equal(t, []string{"112 22 -12 2 0"}, s.cqlRows("SELECT v + 1, v * 2 - 200, -v + 99, v / 50, v % 3 FROM g WHERE p = 1 AND c1 = 1 AND c2 = 1"))
	require.Equal(t, []string{"55.5"}, s.cqlRows("SELECT v / 2.0 FROM g WHERE p = 1 AND c1 = 1 AND c2 = 1"))
	s.fails("SELECT v / 0 FROM g WHERE p = 1 AND c1 = 1 AND c2 = 1", "Cannot divide by zero")
}

func TestConditionalBatch(t *testing.T) {
	s := newKS(t)
	s.ok("CREATE TABLE acct (owner text, id int, balance int, PRIMARY KEY (owner, id))")
	s.ok("INSERT INTO acct (owner, id, balance) VALUES ('a', 1, 100)")
	s.ok("INSERT INTO acct (owner, id, balance) VALUES ('a', 2, 0)")
	require.Equal(t, []string{"true"}, s.rows(`BEGIN BATCH
		UPDATE acct SET balance = 70 WHERE owner = 'a' AND id = 1 IF balance = 100;
		UPDATE acct SET balance = 30 WHERE owner = 'a' AND id = 2;
		APPLY BATCH`))
	require.Equal(t, []string{"false 70"}, s.rows(`BEGIN BATCH
		UPDATE acct SET balance = 0 WHERE owner = 'a' AND id = 1 IF balance = 100;
		UPDATE acct SET balance = 100 WHERE owner = 'a' AND id = 2;
		APPLY BATCH`))
	require.Equal(t, []string{"70", "30"}, s.cqlRows("SELECT balance FROM acct WHERE owner = 'a'"))
	s.fails(`BEGIN BATCH
		UPDATE acct SET balance = 0 WHERE owner = 'a' AND id = 1 IF balance = 70;
		UPDATE acct SET balance = 0 WHERE owner = 'b' AND id = 1;
		APPLY BATCH`, "cannot span multiple partitions")
}

func TestAuth(t *testing.T) {
	d, err := Open("db", Options{FS: vfs.NewMem(), Authenticator: true, Authorizer: true})
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.Authenticate("cassandra", "cassandra"))
	require.Error(t, d.Authenticate("cassandra", "wrong"))
	as := func(user string) *authSession { return &authSession{t: t, d: d, user: user} }
	admin := as("cassandra")
	admin.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	admin.ok("CREATE TABLE ks.t (k int PRIMARY KEY, v int)")
	admin.ok("CREATE ROLE alice WITH PASSWORD = 'pw' AND LOGIN = true")
	admin.ok("CREATE ROLE readers")
	require.NoError(t, d.Authenticate("alice", "pw"))
	alice := as("alice")
	alice.fails("SELECT * FROM ks.t", "User alice has no SELECT permission on <table ks.t> or any of its parents")
	alice.ok("SELECT * FROM system.local") // drivers' system tables are readable
	admin.ok("GRANT SELECT ON KEYSPACE ks TO readers")
	admin.ok("GRANT readers TO alice")
	alice.ok("SELECT * FROM ks.t")
	alice.fails("INSERT INTO ks.t (k, v) VALUES (1, 1)", "no MODIFY permission")
	admin.ok("GRANT MODIFY ON ks.t TO alice")
	alice.ok("INSERT INTO ks.t (k, v) VALUES (1, 1)")
	alice.fails("CREATE ROLE bob", "no CREATE permission on <all roles>")
	alice.fails("CREATE KEYSPACE k2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", "no CREATE permission on <all keyspaces>")
	admin.ok("GRANT CREATE ON ALL KEYSPACES TO alice")
	alice.ok("CREATE KEYSPACE k2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	alice.ok("CREATE TABLE k2.x (k int PRIMARY KEY)") // the creator owns k2
	alice.ok("DROP TABLE k2.x")
	require.Equal(t, []string{"alice false true", "cassandra true true", "readers false false"}, rowsOf(t, admin.run("LIST ROLES")))
	require.Equal(t, []string{"alice false true", "readers false false"}, rowsOf(t, admin.run("LIST ROLES OF alice")))
	require.Contains(t, rowsOf(t, admin.run("LIST ALL PERMISSIONS OF alice")), "alice alice <table ks.t> MODIFY")
	admin.ok("REVOKE readers FROM alice")
	alice.fails("SELECT * FROM ks.t", "no SELECT permission")
	admin.ok("ALTER ROLE alice WITH PASSWORD = 'new'")
	require.NoError(t, d.Authenticate("alice", "new"))
	admin.ok("DROP ROLE alice")
	require.Error(t, d.Authenticate("alice", "new"))
}

type authSession struct {
	t    *testing.T
	d    *DB
	user string
}

func (s *authSession) run(q string) message.Message {
	p, err := s.d.Prepare("", q)
	if err != nil {
		return ErrorMessage(err)
	}
	return s.d.Execute(p, &RequestOptions{User: s.user})
}

func (s *authSession) ok(q string) {
	s.t.Helper()
	if e, ok := s.run(q).(message.Error); ok {
		s.t.Fatalf("%s: %s", q, e.GetErrorMessage())
	}
}

func (s *authSession) fails(q, substr string) {
	s.t.Helper()
	e, ok := s.run(q).(message.Error)
	require.True(s.t, ok, q)
	require.Contains(s.t, e.GetErrorMessage(), substr)
}

func rowsOf(t *testing.T, m message.Message) []string {
	res, ok := m.(*message.RowsResult)
	require.True(t, ok, "%v", m)
	var out []string
	for _, r := range res.Data {
		var cols []string
		for i, v := range r {
			if res.Metadata.Columns[i].Type.AsCql() == "boolean" {
				cols = append(cols, fmt.Sprint(v[0] == 1))
			} else {
				cols = append(cols, string(v))
			}
		}
		out = append(out, strings.Join(cols, " "))
	}
	return out
}

// TestEstimatesAndViews checks system.size_estimates, table_estimates
// and the system_views node tables.
func TestEstimatesAndViews(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (k int, c int, v text, PRIMARY KEY (k, c))")
	for k := 0; k < 100; k++ {
		for c := 0; c < 5; c++ {
			s.ok(fmt.Sprintf("INSERT INTO ks.t (k, c, v) VALUES (%d, %d, 'value')", k, c))
		}
	}
	rows := s.rows("SELECT table_name, range_start, range_end, partitions_count FROM system.size_estimates WHERE keyspace_name = 'ks'")
	require.Len(t, rows, 1)
	var name, start, end string
	var parts int
	_, err := fmt.Sscanf(rows[0], "%s %s %s %d", &name, &start, &end, &parts)
	require.NoError(t, err)
	require.Equal(t, "t", name)
	require.InDelta(t, 100, parts, 15, "about 100 partitions")
	require.Len(t, s.rows("SELECT range_type FROM system.table_estimates WHERE keyspace_name = 'ks'"), 1)
	require.Equal(t, []string{"Murmur3Partitioner"}, func() []string {
		v := s.rows("SELECT value FROM system_views.settings WHERE name = 'partitioner'")
		return []string{v[0][len("org.apache.cassandra.dht."):]}
	}())
	require.Len(t, s.rows("SELECT mebibytes FROM system_views.disk_usage WHERE keyspace_name = 'ks'"), 1)
}

// TestMapElementRestriction covers m[key] = value restrictions, which
// filter.
func TestMapElementRestriction(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.m (k int PRIMARY KEY, attrs map<text, text>)")
	s.ok("INSERT INTO ks.m (k, attrs) VALUES (1, {'color': 'red', 'size': 'L'})")
	s.ok("INSERT INTO ks.m (k, attrs) VALUES (2, {'color': 'blue', 'size': 'L'})")
	s.ok("INSERT INTO ks.m (k, attrs) VALUES (3, {'color': 'red'})")
	s.fails("SELECT k FROM ks.m WHERE attrs['color'] = 'red'", "ALLOW FILTERING")
	require.ElementsMatch(t, []string{"1", "3"}, s.rows("SELECT k FROM ks.m WHERE attrs['color'] = 'red' ALLOW FILTERING"))
	require.ElementsMatch(t, []string{"1", "2"}, s.rows("SELECT k FROM ks.m WHERE attrs['size'] = ? ALLOW FILTERING", "L"))
	s.fails("CREATE INDEX ON ks.m (attrs)", "not supported")
}
