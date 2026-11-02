package cql

import (
	"errors"
	"testing"

	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/stretchr/testify/require"
)

func TestParseStatements(t *testing.T) {
	for _, q := range []string{
		`USE my_keyspace;`,
		`USE "MyKs"`,
		`CREATE KEYSPACE IF NOT EXISTS cycling WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND durable_writes = true;`,
		`CREATE TABLE IF NOT EXISTS cycling.cyclist_category (
			category text, points int, id uuid, lastname text STATIC,
			tags set<text>, scores list<int>, metadata map<text, frozen<tuple<int, text>>>,
			PRIMARY KEY ((category, points), id)
		) WITH CLUSTERING ORDER BY (id DESC) AND comment = 'test table' AND gc_grace_seconds = 10;`,
		`CREATE TABLE t (k int PRIMARY KEY, v text) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4'}`,
		`SELECT id, WRITETIME(lastname), TTL(lastname), CAST(points AS double), token(category) FROM cycling.cyclist_category WHERE category = ? AND points IN (10, 20, 30) GROUP BY category, points ORDER BY id ASC PER PARTITION LIMIT 5 LIMIT 100 ALLOW FILTERING;`,
		`SELECT DISTINCT category, points FROM cycling.cyclist_category;`,
		`SELECT JSON id, lastname FROM cycling.cyclist_category WHERE category = :cat AND points = :pts;`,
		`SELECT * FROM t WHERE token(k) > token(5) AND token(k) <= ?`,
		`SELECT * FROM t WHERE k = 1 AND (c1, c2) >= (1, 'a') AND (c1, c2) < (2, 'b')`,
		`SELECT count(*) FROM system.local`,
		`INSERT INTO cycling.cyclist_category (category, points, id, lastname, tags) VALUES ('gc', 200, 123e4567-e89b-12d3-a456-426614174000, 'Doe', {'a', 'b'}) IF NOT EXISTS USING TTL 86400 AND TIMESTAMP 1700000000;`,
		`INSERT INTO cycling.cyclist_category JSON '{"category": "gc", "points": 100}' DEFAULT UNSET;`,
		`INSERT INTO cycling.events (id, script, dur, b, f, n) VALUES (1, $$select 'hello'$$, 12h30m, 0xCAFE, -1.5e3, -NaN);`,
		`INSERT INTO t (k, m, l, tu) VALUES (now(), {1: 'a', 2: 'b'}, [1, 2], (1, 'x'))`,
		`UPDATE cycling.cyclist_category USING TTL 300 SET lastname = 'Smith', tags = tags + {'c'}, metadata['k'] = (1, 'v'), points += 5 WHERE category = 'gc' AND points = 200 IF lastname = 'Doe';`,
		`UPDATE t SET l = [1] + l, c = c - 1 WHERE k = 0`,
		`DELETE lastname, metadata['k'], addr.zip FROM cycling.cyclist_category USING TIMESTAMP 1000 WHERE category = 'gc' AND points = 200 IF EXISTS;`,
		`BEGIN UNLOGGED BATCH USING TIMESTAMP 12345
			INSERT INTO cycling.cyclist_category (category, points, id) VALUES ('a', 1, 123e4567-e89b-12d3-a456-426614174000);
			UPDATE cycling.cyclist_category SET lastname = 'Lee' WHERE category = 'a' AND points = 1;
			DELETE FROM cycling.cyclist_category WHERE category = 'b' AND points = 2;
		APPLY BATCH;`,
		`TRUNCATE TABLE cycling.cyclist_category;`,
		`TRUNCATE t`,
		`DROP TABLE IF EXISTS cycling.cyclist_category;`,
		`DROP KEYSPACE ks`,
		`ALTER TABLE t ADD v2 int`,
		`ALTER TABLE t ADD (a int, b text)`,
		`ALTER TABLE t DROP v2`,
		`ALTER TABLE t WITH gc_grace_seconds = 0`,
		`ALTER KEYSPACE ks WITH durable_writes = false`,
		`-- comment
		/* block */ SELECT "key" FROM system.local // trailing`,
	} {
		_, err := Parse(q)
		require.NoError(t, err, q)
	}
}

func TestParseAST(t *testing.T) {
	p, err := Parse(`SELECT COUNT(*), id AS foo, Name, "Quoted", "Escaped""Quote" FROM "MyKS".riders WHERE a = :x AND b > ? LIMIT :lim`)
	require.NoError(t, err)
	s := p.Statement.(*Select)
	require.Equal(t, "MyKS", s.Keyspace)
	require.Equal(t, "riders", s.Table)
	require.Equal(t, []string{"x", "", "lim"}, p.Markers)
	require.True(t, s.Selectors[0].Expr.(*FuncSel).Star)
	require.Equal(t, "foo", s.Selectors[1].Alias)
	require.Equal(t, "name", s.Selectors[2].Expr.(*ColumnSel).Name)
	require.Equal(t, "Quoted", s.Selectors[3].Expr.(*ColumnSel).Name)
	require.Equal(t, `Escaped"Quote`, s.Selectors[4].Expr.(*ColumnSel).Name)
	require.Equal(t, ">", s.Where[1].Op)
	require.Equal(t, &Marker{Index: 2}, s.Limit)

	p, err = Parse(`CREATE TABLE ks.t (a int, b text, c frozen<map<text, int>>, s int static, PRIMARY KEY ((a, b), c)) WITH CLUSTERING ORDER BY (c DESC) AND gc_grace_seconds = 0 AND compaction = {'class': 'X'}`)
	require.NoError(t, err)
	ct := p.Statement.(*CreateTable)
	require.Equal(t, []string{"a", "b"}, ct.PartitionKey)
	require.Equal(t, []string{"c"}, ct.Clustering)
	require.Equal(t, "frozen<map<text, int>>", ct.Columns[2].Type)
	require.True(t, ct.Columns[3].Static)
	require.Equal(t, []Ordering{{"c", true}}, ct.ClusteringOrder)
	require.Len(t, ct.Properties, 2)

	p, err = Parse(`UPDATE t USING TTL 5 SET a = a + 1, b = 'x', c = [1] + c, d -= {2} WHERE k = 1`)
	require.NoError(t, err)
	u := p.Statement.(*Update)
	ops := []string{}
	for _, o := range u.Operations {
		ops = append(ops, o.Op)
	}
	require.Equal(t, []string{"+", "=", "prepend", "-="}, ops)
	require.Equal(t, &Literal{Kind: types.LitInteger, Text: "5"}, u.Using.TTL)

	p, err = Parse(`INSERT INTO t (a, b, c, d) VALUES (-1, 1.5, 'it''s', 2d)`)
	require.NoError(t, err)
	ins := p.Statement.(*Insert)
	require.Equal(t, []Term{
		&Literal{types.LitInteger, "-1"}, &Literal{types.LitFloat, "1.5"},
		&Literal{types.LitString, "it's"}, &Literal{types.LitDuration, "2d"},
	}, ins.Values)

	p, err = Parse(`SELECT * FROM t WHERE k IN ? AND token(k) > 3`)
	require.NoError(t, err)
	s = p.Statement.(*Select)
	require.Equal(t, "IN", s.Where[0].Op)
	require.Equal(t, &Marker{0}, s.Where[0].Value)
	require.True(t, s.Where[1].Token)
}

func TestParseInvalid(t *testing.T) {
	for _, q := range []string{
		`SELECT FROM WHERE;`,
		`CREAT TABLE foo (id int PRIMARY KEY);`,
		`INSERT INTO foo (a, b) VALUES (1, );`,
		`SELECT * FROM ks.tbl WHERE a == 1;`,
		`SELECT * FROM t WHERE a = 'unterminated`,
		`SELECT * FROM t LIMIT 1 extra`,
	} {
		_, err := Parse(q)
		var se *SyntaxError
		require.True(t, errors.As(err, &se), "%q: %v", q, err)
	}
	_, err := Parse(`CREATE ROLE bob`)
	var ue *UnsupportedError
	require.True(t, errors.As(err, &ue))
}

func BenchmarkParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := Parse("SELECT peer, data_center, rack, tokens, rpc_address, schema_version FROM system.peers WHERE a = 1 AND b > ? LIMIT 10"); err != nil {
			b.Fatal(err)
		}
	}
}
