package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestINOrderBy: SELECT ... WHERE pk IN (...) ORDER BY reads every listed
// partition before applying LIMIT, sorts whether or not the clustering
// columns are selected, and by every clustering column.
func TestINOrderBy(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (p int, a int, b int, v text, PRIMARY KEY (p, a, b))")
	for _, r := range []struct {
		p, a, b int
		v       string
	}{{1, 5, 1, "1-5-1"}, {1, 5, 0, "1-5-0"}, {1, 9, 0, "1-9-0"}, {2, 1, 0, "2-1-0"}, {2, 5, 2, "2-5-2"}, {3, 0, 7, "3-0-7"}} {
		s.ok("INSERT INTO ks.t (p, a, b, v) VALUES (?, ?, ?, ?)", r.p, r.a, r.b, r.v)
	}
	// The smallest rows are in the last partitions read.
	require.Equal(t, []string{"3-0-7", "2-1-0"}, s.rows("SELECT v FROM ks.t WHERE p IN (1, 2, 3) ORDER BY a ASC LIMIT 2"))
	// Not selected, and ties on a broken by b.
	require.Equal(t, []string{"3-0-7", "2-1-0", "1-5-0", "1-5-1", "2-5-2", "1-9-0"},
		s.rows("SELECT v FROM ks.t WHERE p IN (1, 2, 3) ORDER BY a ASC"))
	require.Equal(t, []string{"1-9-0", "2-5-2", "1-5-1", "1-5-0", "2-1-0", "3-0-7"},
		s.rows("SELECT v FROM ks.t WHERE p IN (1, 2, 3) ORDER BY a DESC, b DESC"))
}

// TestDroppedColumnRows: a row whose only cells belonged to a dropped
// column (made by UPDATE: no row marker) is gone, not a row of nulls.
func TestDroppedColumnRows(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (k int, c int, a int, b int, PRIMARY KEY (k, c))")
	s.ok("UPDATE ks.t SET a = 1 WHERE k = 1 AND c = 1")
	s.ok("UPDATE ks.t SET b = 2 WHERE k = 1 AND c = 2")
	s.ok("INSERT INTO ks.t (k, c, a) VALUES (1, 3, 3)")
	s.ok("ALTER TABLE ks.t DROP a")
	require.Equal(t, []string{"2 2", "3 null"}, s.rows("SELECT c, b FROM ks.t WHERE k = 1"))
	require.Equal(t, []string{"2"}, s.rows("SELECT count(*) FROM ks.t WHERE k = 1"))
}

// TestPkIndicesNamedMarkers: the PREPARED result's partition key indices
// are the markers bound to the partition key columns, whatever they are
// named (drivers route by them).
func TestPkIndicesNamedMarkers(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (a int, b int, v int, other int, PRIMARY KEY ((a, b)))")
	for q, want := range map[string][]uint16{
		"SELECT v FROM ks.t WHERE a = :x AND b = :y":                                   {0, 1},
		"SELECT v FROM ks.t WHERE b = ? AND a = ?":                                     {1, 0},
		"SELECT v FROM ks.t WHERE other = :a AND a = :real AND b = :b ALLOW FILTERING": {1, 2},
		"UPDATE ks.t SET v = :v WHERE a = :id1 AND b = :id2":                           {1, 2},
		"INSERT INTO ks.t (v, b, a) VALUES (?, ?, ?)":                                  {2, 1},
	} {
		p, err := s.d.Prepare("", q)
		require.NoError(t, err, q)
		vm, _, _ := p.Metadata()
		require.Equal(t, want, vm.PkIndices, q)
	}
}

// TestGroupByPerPartitionLimit: PER PARTITION LIMIT with GROUP BY counts
// groups, not rows; and a null clustering value (a static-only row) is a
// group of its own, not the empty value's.
func TestGroupByPerPartitionLimit(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.t (p int, a int, b int, v int, PRIMARY KEY (p, a, b))")
	for p := 1; p <= 2; p++ {
		for a := 1; a <= 3; a++ {
			for b := 1; b <= 2; b++ {
				s.ok("INSERT INTO ks.t (p, a, b, v) VALUES (?, ?, ?, 1)", p, a, b)
			}
		}
	}
	require.Equal(t, []string{"1 1 2", "1 2 2", "2 1 2", "2 2 2"},
		s.rows("SELECT p, a, count(*) FROM ks.t GROUP BY p, a PER PARTITION LIMIT 2"))

	s.ok("CREATE TABLE ks.s (p int, c text, st int STATIC, v int, PRIMARY KEY (p, c))")
	s.ok("UPDATE ks.s SET st = 7 WHERE p = 1")
	s.ok("INSERT INTO ks.s (p, c, v) VALUES (2, '', 1)")
	s.ok("UPDATE ks.s SET st = 8 WHERE p = 2")
	require.Len(t, s.rows("SELECT p, c, count(*) FROM ks.s GROUP BY p, c"), 2)
}
