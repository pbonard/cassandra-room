package db

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

type session struct {
	t  *testing.T
	d  *DB
	ks string
}

func openTest(t *testing.T, fs vfs.FS) *session {
	if fs == nil {
		fs = vfs.NewMem()
	}
	d, err := Open("db", Options{FS: fs})
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return &session{t: t, d: d}
}

// run executes a query, binding values given as CQL literal text with their
// types inferred from the prepared statement.
func (s *session) run(q string, args ...any) message.Message {
	s.t.Helper()
	p, err := s.d.Prepare(s.ks, q)
	if err != nil {
		return ErrorMessage(err)
	}
	vm, _, _ := p.Metadata()
	vals := make([]*primitive.Value, len(args))
	for i, a := range args {
		typ := p.state.Load().vars[i].typ
		switch v := a.(type) {
		case nil:
			vals[i] = &primitive.Value{Type: primitive.ValueTypeNull}
		case []byte:
			vals[i] = primitive.NewValue(v)
		default:
			kind := types.LitString
			switch v.(type) {
			case int, int64:
				kind = types.LitInteger
			case float64:
				kind = types.LitFloat
			case bool:
				kind = types.LitBoolean
			}
			b, err := types.FromLiteral(typ, kind, fmt.Sprint(v))
			if err != nil {
				b, err = types.FromString(typ, fmt.Sprint(v))
			}
			require.NoError(s.t, err, "%v as %s", v, typ)
			vals[i] = primitive.NewValue(b)
		}
	}
	_ = vm
	res := s.d.Execute(p, &RequestOptions{Values: vals, Version: primitive.ProtocolVersion4})
	if sk, ok := res.(*message.SetKeyspaceResult); ok {
		s.ks = sk.Keyspace
	}
	return res
}

func (s *session) ok(q string, args ...any) message.Message {
	s.t.Helper()
	res := s.run(q, args...)
	if e, ok := res.(message.Error); ok {
		s.t.Fatalf("%s: %s", q, e.GetErrorMessage())
	}
	return res
}

func (s *session) fails(q, substr string, args ...any) {
	s.t.Helper()
	res := s.run(q, args...)
	e, ok := res.(message.Error)
	require.True(s.t, ok, "%s: expected an error, got %v", q, res)
	require.Contains(s.t, e.GetErrorMessage(), substr, q)
}

// rows runs a query and renders its rows as CQL text, one string per row.
func (s *session) rows(q string, args ...any) []string {
	s.t.Helper()
	res := s.ok(q, args...).(*message.RowsResult)
	var out []string
	for _, r := range res.Data {
		var cols []string
		for i, v := range r {
			typ := s.colType(res.Metadata.Columns[i])
			if v == nil {
				cols = append(cols, "null")
			} else {
				cols = append(cols, formatValue(typ, v))
			}
		}
		out = append(out, strings.Join(cols, " "))
	}
	return out
}

func (s *session) colType(c *message.ColumnMetadata) *types.Type {
	t, err := types.Parse(strings.ToLower(c.Type.AsCql()))
	if err != nil {
		return types.BlobType
	}
	return t
}

func TestBasicCRUD(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.fails("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", "already exists")
	s.ok("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok("CREATE TABLE kv (k text PRIMARY KEY, v int, w text)")
	s.ok("INSERT INTO kv (k, v, w) VALUES ('a', 1, 'x')")
	s.ok("INSERT INTO kv (k, v) VALUES (?, ?)", "b", 2)
	require.Equal(t, []string{"a 1 x"}, s.rows("SELECT * FROM kv WHERE k = 'a'"))
	require.Equal(t, []string{"2 null"}, s.rows("SELECT v, w FROM kv WHERE k = ?", "b"))
	s.ok("UPDATE kv SET w = 'y' WHERE k = 'b'")
	require.Equal(t, []string{"b 2 y"}, s.rows("SELECT k, v, w FROM kv WHERE k = 'b'"))
	s.ok("DELETE w FROM kv WHERE k = 'b'")
	require.Equal(t, []string{"b 2 null"}, s.rows("SELECT k, v, w FROM kv WHERE k = 'b'"))
	s.ok("DELETE FROM kv WHERE k = 'b'")
	require.Empty(t, s.rows("SELECT * FROM kv WHERE k = 'b'"))
	// An UPDATE creates no row marker: deleting its only column removes it.
	s.ok("UPDATE kv SET v = 3 WHERE k = 'c'")
	require.Equal(t, []string{"c 3 null"}, s.rows("SELECT * FROM kv WHERE k = 'c'"))
	s.ok("DELETE v FROM kv WHERE k = 'c'")
	require.Empty(t, s.rows("SELECT * FROM kv WHERE k = 'c'"))
	// An INSERT does: the row stays with null columns.
	s.ok("INSERT INTO kv (k, v) VALUES ('d', 4)")
	s.ok("DELETE v FROM kv WHERE k = 'd'")
	require.Equal(t, []string{"d null null"}, s.rows("SELECT * FROM kv WHERE k = 'd'"))
	require.Len(t, s.rows("SELECT * FROM kv"), 2)
	require.Equal(t, []string{"2"}, s.rows("SELECT count(*) FROM kv"))
	s.fails("SELECT * FROM kv WHERE v = 1", "ALLOW FILTERING")
	require.Equal(t, []string{"a 1 x"}, s.rows("SELECT * FROM kv WHERE v = 1 ALLOW FILTERING"))
	s.fails("SELECT nope FROM kv", "Undefined column name nope")
	s.fails("INSERT INTO kv (k, v) VALUES ('e', 'str')", "Invalid STRING constant")
	s.fails("INSERT INTO kv (v) VALUES (1)", "Some partition key parts are missing: k")
	s.fails("SELECT * FROM nosuch", "does not exist")
}

func TestClustering(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok("CREATE TABLE ts (p int, c1 int, c2 text, v text, s int static, PRIMARY KEY (p, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)")
	for i := 0; i < 5; i++ {
		for _, c2 := range []string{"a", "b"} {
			s.ok("INSERT INTO ts (p, c1, c2, v) VALUES (1, ?, ?, ?)", i, c2, fmt.Sprintf("v%d%s", i, c2))
		}
	}
	s.ok("UPDATE ts SET s = 7 WHERE p = 1")
	require.Equal(t, []string{"4 a", "4 b", "3 a", "3 b"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 LIMIT 4"))
	require.Equal(t, []string{"2 a", "2 b", "1 a", "1 b"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND c1 >= 1 AND c1 < 3"))
	// Reversing the first clustering column reverses the whole clustering.
	require.Equal(t, []string{"1 b", "1 a", "2 b", "2 a"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND c1 >= 1 AND c1 < 3 ORDER BY c1 ASC"))
	require.Equal(t, []string{"1 b", "1 a", "2 b", "2 a"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND c1 >= 1 AND c1 < 3 ORDER BY c1 ASC, c2 DESC"))
	s.fails("SELECT c1, c2 FROM ts WHERE p = 1 ORDER BY c1 ASC, c2 ASC", "Unsupported order by relation")
	require.Equal(t, []string{"3 b"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND c1 = 3 AND c2 > 'a'"))
	require.Equal(t, []string{"4 b v4b 7"}, s.rows("SELECT c1, c2, v, s FROM ts WHERE p = 1 AND c1 = 4 AND c2 = 'b'"))
	require.Equal(t, []string{"0 b", "0 a", "2 b", "2 a"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND c1 IN (0, 2) ORDER BY c1 ASC"))
	require.Equal(t, []string{"3 a", "2 b"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND (c1, c2) < (3, 'b') AND (c1, c2) >= (2, 'b')"))
	// Range deletion, row deletion and partition deletion.
	s.ok("DELETE FROM ts WHERE p = 1 AND c1 > 2")
	require.Equal(t, []string{"2", "2", "1", "1", "0", "0"}, s.rows("SELECT c1 FROM ts WHERE p = 1"))
	s.ok("DELETE FROM ts WHERE p = 1 AND c1 = 1 AND c2 = 'a'")
	require.Equal(t, []string{"2 a", "2 b", "1 b", "0 a", "0 b"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1"))
	s.ok("INSERT INTO ts (p, c1, c2) VALUES (1, 4, 'z')") // newer than the range deletion
	require.Equal(t, []string{"4 z"}, s.rows("SELECT c1, c2 FROM ts WHERE p = 1 AND c1 = 4"))
	s.ok("DELETE FROM ts WHERE p = 1")
	require.Empty(t, s.rows("SELECT * FROM ts WHERE p = 1"))
	// Static-only partition.
	s.ok("UPDATE ts SET s = 9 WHERE p = 2")
	require.Equal(t, []string{"2 null null 9"}, s.rows("SELECT p, c1, c2, s FROM ts WHERE p = 2"))
	require.Equal(t, []string{"2 9"}, s.rows("SELECT DISTINCT p, s FROM ts"))
}

func TestPaging(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok("CREATE TABLE t (p int, c int, v int, PRIMARY KEY (p, c))")
	want := map[string]bool{}
	for p := 0; p < 20; p++ {
		for c := 0; c < 7; c++ {
			s.ok("INSERT INTO t (p, c, v) VALUES (?, ?, ?)", p, c, p*100+c)
			want[fmt.Sprintf("%d %d", p, c)] = true
		}
	}
	for _, q := range []string{"SELECT p, c FROM t", "SELECT p, c FROM t PER PARTITION LIMIT 3", "SELECT p, c FROM t LIMIT 50"} {
		p, err := s.d.Prepare("ks", q)
		require.NoError(t, err)
		var got []string
		var state []byte
		for {
			res := s.d.Execute(p, &RequestOptions{PageSize: 6, PagingState: state}).(*message.RowsResult)
			require.LessOrEqual(t, len(res.Data), 6)
			for _, r := range res.Data {
				got = append(got, fmt.Sprintf("%d %d", int32(be32(r[0])), int32(be32(r[1]))))
			}
			if res.Metadata.PagingState == nil {
				break
			}
			state = res.Metadata.PagingState
		}
		all := s.rows(q)
		require.Equal(t, all, got, q)
	}
	require.Len(t, s.rows("SELECT * FROM t"), 140)
	require.Len(t, s.rows("SELECT * FROM t PER PARTITION LIMIT 3"), 60)
	require.Len(t, s.rows("SELECT * FROM t WHERE token(p) > 0"), len(s.rows("SELECT * FROM t WHERE token(p) > 0 ALLOW FILTERING")))
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func TestTTLAndTimestamps(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	fs := vfs.NewMem()
	d, err := Open("db", Options{FS: fs, Now: func() time.Time { return now }})
	require.NoError(t, err)
	defer d.Close()
	s := &session{t: t, d: d}
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok("CREATE TABLE t (k int PRIMARY KEY, v text) WITH default_time_to_live = 100")
	s.ok("INSERT INTO t (k, v) VALUES (1, 'a') USING TTL 10")
	s.ok("INSERT INTO t (k, v) VALUES (2, 'b')")
	s.ok("INSERT INTO t (k, v) VALUES (3, 'c') USING TIMESTAMP 1000 AND TTL 0")
	s.ok("INSERT INTO t (k, v) VALUES (3, 'old') USING TIMESTAMP 999")
	require.Equal(t, []string{"10"}, s.rows("SELECT ttl(v) FROM t WHERE k = 1"))
	require.Equal(t, []string{"100"}, s.rows("SELECT ttl(v) FROM t WHERE k = 2"))
	require.Equal(t, []string{"c 1000 null"}, s.rows("SELECT v, writetime(v), ttl(v) FROM t WHERE k = 3"))
	now = now.Add(11 * time.Second)
	require.Empty(t, s.rows("SELECT * FROM t WHERE k = 1"))
	require.Len(t, s.rows("SELECT * FROM t"), 2)
}

func TestSchemaPersistsAndSystemTables(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("db", Options{FS: fs})
	require.NoError(t, err)
	s := &session{t: t, d: d}
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (a int, b text, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH comment = 'hi'")
	s.ok("INSERT INTO ks.t (a, b, c) VALUES (1, 'x', [1, 2])")
	require.NoError(t, d.Close())

	d, err = Open("db", Options{FS: fs})
	require.NoError(t, err)
	defer d.Close()
	s = &session{t: t, d: d}
	require.Equal(t, []string{"1 x 0x00000002000000040000000100000004000000020"[:0] + "1 x 0x0000000200000004000000010000000400000002"}, s.rows("SELECT * FROM ks.t"))
	require.Equal(t, []string{"local COMPLETED 5.0.4 org.apache.cassandra.dht.Murmur3Partitioner"},
		s.rows("SELECT key, bootstrapped, release_version, partitioner FROM system.local WHERE key = 'local'"))
	require.Contains(t, s.rows("SELECT keyspace_name FROM system_schema.keyspaces"), "ks")
	require.Equal(t, []string{"t hi"}, s.rows("SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = 'ks'"))
	require.Equal(t, []string{"a partition_key 0 int", "b clustering 0 text", "c regular -1 frozen<list<int>>"},
		s.rows("SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = 'ks' AND table_name = 't'"))
	s.ok("ALTER TABLE ks.t ADD d int")
	s.ok("INSERT INTO ks.t (a, b, d) VALUES (1, 'y', 5)")
	require.Equal(t, []string{"y 5"}, s.rows("SELECT b, d FROM ks.t WHERE a = 1 AND b = 'y'"))
	s.ok("ALTER TABLE ks.t DROP d")
	s.ok("ALTER TABLE ks.t ADD d text")
	require.Equal(t, []string{"y null"}, s.rows("SELECT b, d FROM ks.t WHERE a = 1 AND b = 'y'"), "a re-added column does not see the old data")
	s.ok("TRUNCATE ks.t")
	require.Empty(t, s.rows("SELECT * FROM ks.t"))
	s.ok("DROP TABLE ks.t")
	s.fails("SELECT * FROM ks.t", "does not exist")
	s.ok("DROP KEYSPACE ks")
}

func TestLWT(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok("CREATE TABLE t (k int PRIMARY KEY, v int)")
	require.Equal(t, []string{"true"}, s.rows("INSERT INTO t (k, v) VALUES (1, 1) IF NOT EXISTS"))
	require.Equal(t, []string{"false 1 1"}, s.rows("INSERT INTO t (k, v) VALUES (1, 2) IF NOT EXISTS"))
	require.Equal(t, []string{"false 1"}, s.rows("UPDATE t SET v = 3 WHERE k = 1 IF v = 2"))
	require.Equal(t, []string{"true"}, s.rows("UPDATE t SET v = 3 WHERE k = 1 IF v = 1"))
	require.Equal(t, []string{"3"}, s.rows("SELECT v FROM t WHERE k = 1"))
	require.Equal(t, []string{"false"}, s.rows("DELETE FROM t WHERE k = 2 IF EXISTS"))
	require.Equal(t, []string{"true"}, s.rows("DELETE FROM t WHERE k = 1 IF EXISTS"))
}

func TestMurmur3(t *testing.T) {
	// From Cassandra's MurmurHash.hash3_x64_128 (run in Java).
	for k, want := range map[string]int64{
		"":                      -1 << 63,
		"hello":                 -3758069500696749310,
		"a":                     -8839064797231613815,
		"abcdefghijklmnopqrstu": -3903098180127962820,
		"key1":                  1573573083296714675,
	} {
		require.Equal(t, want, Token([]byte(k)), k)
	}
	// Tails with high bytes, which Cassandra sign-extends.
	hi := []byte{0xff, 0x80, 1, 2, 3, 0x90, 7, 8, 9, 10, 11, 12, 13, 0xfe, 15, 16, 0xc0, 0x81, 3}
	want := []int64{-4442228696663692417, 8915363533249992128, -6979169580508520651, 3070089285984415867, -2207615382626362622, 4685134242467360119, 5781518398141443899, -9133192610196022745, -7291119913675794816, -2845254364353442723, 3044323868056032662, 5082219596318428943, -717098128381703507, -3414931521946909932, 6795884858055789221, -8872837510234358527, -2809116059857821520, -8576919035674581929, -3723512408977639682}
	for n := 1; n <= len(hi); n++ {
		require.Equal(t, want[n-1], murmur3(hi[:n]), n)
	}
}

func TestDescribe(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok(`CREATE TABLE "Mixed" (k int, "C" text, v int static, PRIMARY KEY ((k), "C")) WITH CLUSTERING ORDER BY ("C" DESC) AND comment = 'it''s'`)
	require.Equal(t, []string{"ks keyspace ks", "system keyspace system", "system_schema keyspace system_schema"}, s.rows("DESCRIBE KEYSPACES"))
	require.Equal(t, []string{`ks table "Mixed"`}, s.rows("DESCRIBE TABLES"))
	out := s.rows(`DESCRIBE TABLE "Mixed"`)
	require.Len(t, out, 1)
	require.Contains(t, out[0], `CREATE TABLE ks."Mixed" (`)
	require.Contains(t, out[0], `v int static,`)
	require.Contains(t, out[0], `PRIMARY KEY (k, "C")`)
	require.Contains(t, out[0], `CLUSTERING ORDER BY ("C" DESC)`)
	require.Contains(t, out[0], `comment = 'it''s'`)
	require.Len(t, s.rows("DESCRIBE KEYSPACE ks"), 2)
	require.Len(t, s.rows("DESCRIBE ONLY KEYSPACE ks"), 1)
	require.Len(t, s.rows("DESCRIBE SCHEMA"), 2)
	s.fails("DESCRIBE TABLE nope", "not found")
}

func BenchmarkExecute(b *testing.B) {
	d, err := Open("db", Options{FS: vfs.NewMem()})
	require.NoError(b, err)
	defer d.Close()
	run := func(q string) {
		p, err := d.Prepare("", q)
		require.NoError(b, err)
		if e, ok := d.Execute(p, &RequestOptions{}).(message.Error); ok {
			b.Fatal(e.GetErrorMessage())
		}
	}
	run("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	run("CREATE TABLE ks.kv (k blob PRIMARY KEY, c0 blob, c1 blob, c2 blob, c3 blob, c4 blob)")
	ins, err := d.Prepare("ks", "INSERT INTO kv (k, c0, c1, c2, c3, c4) VALUES (?, ?, ?, ?, ?, ?)")
	require.NoError(b, err)
	sel, err := d.Prepare("ks", "SELECT * FROM kv WHERE k = ?")
	require.NoError(b, err)
	val := primitive.NewValue(make([]byte, 34))
	key := func(i int) *primitive.Value { return primitive.NewValue([]byte(fmt.Sprintf("%010d", i))) }
	const n = 100000
	b.Run("insert", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := d.Execute(ins, &RequestOptions{Values: []*primitive.Value{key(i % n), val, val, val, val, val}})
			if _, ok := res.(message.Error); ok {
				b.Fatal(res)
			}
		}
	})
	for i := 0; i < n; i++ {
		d.Execute(ins, &RequestOptions{Values: []*primitive.Value{key(i), val, val, val, val, val}})
	}
	b.Run("select", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := d.Execute(sel, &RequestOptions{Values: []*primitive.Value{key(i % n)}}).(*message.RowsResult)
			if len(res.Data) != 1 {
				b.Fatal("missing row")
			}
		}
	})
}
