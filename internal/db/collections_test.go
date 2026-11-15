package db

import (
	"strings"
	"testing"

	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"

	"github.com/stretchr/testify/require"
)

func newKS(t *testing.T) *session {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	return s
}

func TestCollections(t *testing.T) {
	s := newKS(t)
	s.ok("CREATE TABLE c (k int PRIMARY KEY, s set<text>, l list<int>, m map<text, int>)")
	s.ok("INSERT INTO c (k, s, l, m) VALUES (1, {'b', 'a'}, [3, 1, 2], {'x': 1, 'y': 2})")
	require.Equal(t, []string{"{'a', 'b'} [3, 1, 2] {'x': 1, 'y': 2}"}, s.cqlRows("SELECT s, l, m FROM c WHERE k = 1"))
	s.ok("UPDATE c SET s = s + {'c'}, l = l + [4], m['z'] = 3 WHERE k = 1")
	s.ok("UPDATE c SET l = [0] + l WHERE k = 1")
	require.Equal(t, []string{"{'a', 'b', 'c'} [0, 3, 1, 2, 4] {'x': 1, 'y': 2, 'z': 3}"}, s.cqlRows("SELECT s, l, m FROM c WHERE k = 1"))
	s.ok("UPDATE c SET s = s - {'a'}, l = l - [1], m = m - {'x'} WHERE k = 1")
	require.Equal(t, []string{"{'b', 'c'} [0, 3, 2, 4] {'y': 2, 'z': 3}"}, s.cqlRows("SELECT s, l, m FROM c WHERE k = 1"))
	s.ok("UPDATE c SET l[1] = 30 WHERE k = 1")
	s.ok("DELETE l[0], m['y'] FROM c WHERE k = 1")
	require.Equal(t, []string{"[30, 2, 4] {'z': 3}"}, s.cqlRows("SELECT l, m FROM c WHERE k = 1"))
	s.fails("UPDATE c SET l[9] = 1 WHERE k = 1", "List index 9 out of bound, list has size 3")
	// Overwriting replaces the whole collection.
	s.ok("UPDATE c SET s = {'q'} WHERE k = 1")
	require.Equal(t, []string{"{'q'}"}, s.cqlRows("SELECT s FROM c WHERE k = 1"))
	s.ok("DELETE s FROM c WHERE k = 1")
	require.Equal(t, []string{"null"}, s.cqlRows("SELECT s FROM c WHERE k = 1"))
	// Filtering on collections.
	require.Equal(t, []string{"1"}, s.cqlRows("SELECT k FROM c WHERE m CONTAINS KEY 'z' ALLOW FILTERING"))
	require.Empty(t, s.cqlRows("SELECT k FROM c WHERE l CONTAINS 99 ALLOW FILTERING"))
	// Bound values.
	s.ok("UPDATE c SET s = s + ? WHERE k = 2", []byte{0, 0, 0, 2, 0, 0, 0, 1, 'b', 0, 0, 0, 1, 'a'})
	require.Equal(t, []string{"{'a', 'b'}"}, s.cqlRows("SELECT s FROM c WHERE k = 2"))
	s.fails("UPDATE c SET k = 1 WHERE k = 2", "PRIMARY KEY part k found in SET part")
	s.ok("CREATE TABLE f (k int PRIMARY KEY, s frozen<set<int>>)")
	s.fails("UPDATE f SET s = s + {1} WHERE k = 1", "Invalid operation (s = s + <value>) for frozen collection column s")
}

func TestUserTypes(t *testing.T) {
	s := newKS(t)
	s.ok("CREATE TYPE address (street text, zip int)")
	s.fails("CREATE TYPE address (x int)", "already exists")
	s.ok("CREATE TABLE u (k int PRIMARY KEY, home address, work frozen<address>, past list<frozen<address>>)")
	s.ok("INSERT INTO u (k, home, work, past) VALUES (1, {street: 'Main', zip: 10}, {street: 'Office', zip: 20}, [{street: 'Old', zip: 1}])")
	require.Equal(t, []string{"{street: 'Main', zip: 10} {street: 'Office', zip: 20} [{street: 'Old', zip: 1}]"},
		s.cqlRows("SELECT home, work, past FROM u WHERE k = 1"))
	s.ok("UPDATE u SET home.zip = 11 WHERE k = 1")
	s.ok("DELETE home.street FROM u WHERE k = 1")
	require.Equal(t, []string{"{street: null, zip: 11}"}, s.cqlRows("SELECT home FROM u WHERE k = 1"))
	s.fails("UPDATE u SET work.zip = 1 WHERE k = 1", "frozen UDT")
	s.ok("ALTER TYPE address ADD city text")
	s.ok("UPDATE u SET home.city = 'X' WHERE k = 1")
	require.Equal(t, []string{"{street: null, zip: 11, city: 'X'}"}, s.cqlRows("SELECT home FROM u WHERE k = 1"))
	s.fails("DROP TYPE address", "still used by table u")
	require.Equal(t, []string{"address ['street', 'zip', 'city'] ['text', 'int', 'text']"},
		s.cqlRows("SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = 'ks'"))
	out := s.rows("DESCRIBE KEYSPACE ks")
	require.Contains(t, out[1], "CREATE TYPE ks.address (")
}

func TestCounters(t *testing.T) {
	s := newKS(t)
	s.fails("CREATE TABLE bad (k int PRIMARY KEY, c counter, v int)", "Cannot mix counter and non counter columns")
	s.ok("CREATE TABLE cnt (k int, ck text, hits counter, misses counter, PRIMARY KEY (k, ck))")
	s.fails("INSERT INTO cnt (k, ck, hits) VALUES (1, 'a', 1)", "INSERT statements are not allowed on counter tables")
	s.fails("UPDATE cnt SET hits = 5 WHERE k = 1 AND ck = 'a'", "Cannot set the value of counter column hits")
	for i := 0; i < 10; i++ {
		s.ok("UPDATE cnt SET hits = hits + 2, misses = misses - 1 WHERE k = 1 AND ck = 'a'")
	}
	s.ok("UPDATE cnt SET hits += 5 WHERE k = 1 AND ck IN ('a', 'b')")
	require.Equal(t, []string{"a 25 -10", "b 5 null"}, s.cqlRows("SELECT ck, hits, misses FROM cnt WHERE k = 1"))
	s.ok("BEGIN COUNTER BATCH UPDATE cnt SET hits = hits + 1 WHERE k = 2 AND ck = 'x'; UPDATE cnt SET hits = hits + 1 WHERE k = 2 AND ck = 'x'; APPLY BATCH")
	require.Equal(t, []string{"2"}, s.cqlRows("SELECT hits FROM cnt WHERE k = 2 AND ck = 'x'"))
	s.fails("BEGIN BATCH UPDATE cnt SET hits = hits + 1 WHERE k = 2 AND ck = 'x'; APPLY BATCH", "Cannot include a counter statement in a logged batch")
	s.ok("DELETE FROM cnt WHERE k = 2 AND ck = 'x'")
	require.Empty(t, s.cqlRows("SELECT hits FROM cnt WHERE k = 2 AND ck = 'x'"))
}

// cqlRows runs a SELECT and renders its rows as CQL literals.
func (s *session) cqlRows(q string, args ...any) []string {
	s.t.Helper()
	res := s.ok(q, args...).(*message.RowsResult)
	p, err := s.d.Prepare(s.ks, q)
	require.NoError(s.t, err)
	sels := p.state.Load().stmt.(*selectStmt).sels
	var out []string
	for _, r := range res.Data {
		var cols []string
		for i, v := range r {
			switch sels[i].typ.Kind {
			case types.Text, types.Ascii:
				if v != nil {
					cols = append(cols, string(v))
					continue
				}
			}
			cols = append(cols, formatCQL(sels[i].typ, v))
		}
		out = append(out, strings.Join(cols, " "))
	}
	return out
}
