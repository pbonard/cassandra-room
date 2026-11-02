package db

import (
	"testing"

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
