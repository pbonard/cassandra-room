package db

import (
	"encoding/json"
	"testing"

	"github.com/pbonard/cassandra-room/internal/accord"

	"github.com/stretchr/testify/require"
)

func TestTransactions(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.acct (id int PRIMARY KEY, balance int, owner text) WITH transactional_mode = 'full'")
	s.ok("CREATE TABLE ks.log (id int, seq int, note text, PRIMARY KEY (id, seq)) WITH transactional_mode = 'full'")
	s.ok("CREATE TABLE ks.plain (k int PRIMARY KEY, v int)")
	s.ok("INSERT INTO ks.acct (id, balance, owner) VALUES (1, 100, 'a')")
	s.ok("INSERT INTO ks.acct (id, balance, owner) VALUES (2, 50, 'b')")

	transfer := `BEGIN TRANSACTION
		LET src = (SELECT balance FROM ks.acct WHERE id = ?);
		LET dst = (SELECT balance FROM ks.acct WHERE id = ?);
		SELECT src.balance, dst.balance;
		IF src.balance >= ? AND dst IS NOT NULL THEN
			UPDATE ks.acct SET balance -= ? WHERE id = ?;
			UPDATE ks.acct SET balance += ? WHERE id = ?;
			INSERT INTO ks.log (id, seq, note) VALUES (?, 1, 'moved');
		ELSE IF dst IS NULL THEN
			INSERT INTO ks.log (id, seq, note) VALUES (?, 2, 'no destination');
		ELSE
			INSERT INTO ks.log (id, seq, note) VALUES (?, 3, 'insufficient');
		END IF
	COMMIT TRANSACTION`
	// Applied: the SELECT shows the balances read (before the updates).
	require.Equal(t, []string{"100 50"}, s.rows(transfer, 1, 2, 30, 30, 1, 30, 2, 1, 1, 1))
	require.Equal(t, []string{"1 70", "2 80"}, s.rows("SELECT id, balance FROM ks.acct"))
	require.Equal(t, []string{"moved"}, s.rows("SELECT note FROM ks.log WHERE id = 1"))
	// ELSE IF: the destination does not exist.
	s.ok(transfer, 1, 9, 10, 10, 1, 10, 9, 2, 2, 2)
	require.Equal(t, []string{"no destination"}, s.rows("SELECT note FROM ks.log WHERE id = 2"))
	// ELSE: not enough money.
	s.ok(transfer, 2, 1, 500, 500, 2, 500, 1, 3, 3, 3)
	require.Equal(t, []string{"insufficient"}, s.rows("SELECT note FROM ks.log WHERE id = 3"))
	require.Equal(t, []string{"1 70", "2 80"}, s.rows("SELECT id, balance FROM ks.acct"))

	// References as values, a full SELECT, unconditional updates.
	s.ok(`BEGIN TRANSACTION
		LET a = (SELECT * FROM ks.acct WHERE id = 1);
		INSERT INTO ks.acct (id, balance, owner) VALUES (3, a.balance, a.owner);
		UPDATE ks.acct SET owner = 'c' WHERE id = 1;
	COMMIT TRANSACTION`)
	require.ElementsMatch(t, []string{"3 70 a", "1 70 c"}, s.rows("SELECT id, balance, owner FROM ks.acct WHERE id IN (3, 1)"))
	require.Equal(t, []string{"2 80 b"}, s.rows(`BEGIN TRANSACTION SELECT id, balance, owner FROM ks.acct WHERE id = 2; COMMIT TRANSACTION`))
	// LET with LIMIT 1 on a partition with clustering rows.
	require.Equal(t, []string{"moved"}, s.rows(`BEGIN TRANSACTION
		LET l = (SELECT note FROM ks.log WHERE id = 1 LIMIT 1);
		SELECT l.note;
	COMMIT TRANSACTION`))

	// Arithmetic in values and conditions, with precedence.
	s.ok("INSERT INTO ks.acct (id, balance, owner) VALUES (10, 7, 'x')")
	require.Equal(t, []string{"7"}, s.rows(`BEGIN TRANSACTION
		LET a = (SELECT balance FROM ks.acct WHERE id = 10);
		SELECT a.balance;
		IF a.balance * 2 + 1 = 15 AND a.balance - 1 > 5 AND 100 / a.balance = 14 THEN
			UPDATE ks.acct SET balance = a.balance * 3 - ? WHERE id = 10;
			INSERT INTO ks.acct (id, balance) VALUES (11, a.balance % 4 + 2 * 5);
		END IF
	COMMIT TRANSACTION`, 1))
	require.ElementsMatch(t, []string{"10 20", "11 13"}, s.rows("SELECT id, balance FROM ks.acct WHERE id IN (10, 11)"))
	// A null operand makes the result null; a missing row with += sets the
	// value.
	s.ok(`BEGIN TRANSACTION
		LET m = (SELECT balance FROM ks.acct WHERE id = 99);
		INSERT INTO ks.acct (id, balance) VALUES (12, m.balance + 1);
		UPDATE ks.acct SET balance += 5 WHERE id = 13;
	COMMIT TRANSACTION`)
	require.ElementsMatch(t, []string{"12 null", "13 5"}, s.rows("SELECT id, balance FROM ks.acct WHERE id IN (12, 13)"))
	s.fails(`BEGIN TRANSACTION LET a = (SELECT balance FROM ks.acct WHERE id = 10);
		UPDATE ks.acct SET balance = a.balance / 0 WHERE id = 10; COMMIT TRANSACTION`, "divide by zero")
	s.fails(`BEGIN TRANSACTION LET a = (SELECT owner FROM ks.acct WHERE id = 10);
		UPDATE ks.acct SET balance = a.owner + 1 WHERE id = 10; COMMIT TRANSACTION`, "numeric")
	// The prepend form is still a prepend.
	s.ok("CREATE TABLE ks.lists (k int PRIMARY KEY, l list<int>) WITH transactional_mode = 'full'")
	s.ok("INSERT INTO ks.lists (k, l) VALUES (1, [3])")
	s.ok("BEGIN TRANSACTION UPDATE ks.lists SET l = [1, 2] + l WHERE k = 1; COMMIT TRANSACTION")
	require.Equal(t, []string{"0x00000003000000040000000100000004000000020000000400000003"}, s.rows("SELECT l FROM ks.lists WHERE k = 1"), "[1, 2, 3]")

	// TransactionStatement's validation.
	s.fails("BEGIN TRANSACTION SELECT * FROM ks.plain WHERE k = 1; COMMIT TRANSACTION", "Accord transactions are disabled on table")
	s.fails("BEGIN TRANSACTION LET l = (SELECT * FROM ks.log WHERE id = 1); SELECT l.note; COMMIT TRANSACTION", "SELECT must specify either all primary key elements")
	s.fails("BEGIN TRANSACTION SELECT * FROM ks.acct; COMMIT TRANSACTION", "Range queries are not allowed")
	s.fails("BEGIN TRANSACTION UPDATE ks.acct SET balance = 1 WHERE id = 1 IF balance = 2; COMMIT TRANSACTION", "may not specify their own conditions")
	s.fails("BEGIN TRANSACTION UPDATE ks.acct USING TIMESTAMP 5 SET balance = 1 WHERE id = 1; COMMIT TRANSACTION", "custom timestamps")
	s.fails("BEGIN TRANSACTION INSERT INTO ks.acct (id, balance) VALUES (1, 1) USING TTL 5; COMMIT TRANSACTION", "custom ttls")
	s.fails("BEGIN TRANSACTION LET a = (SELECT balance FROM ks.acct WHERE id = 1); UPDATE ks.acct SET owner = a.balance WHERE id = 1; COMMIT TRANSACTION", "Invalid reference")
	s.fails("BEGIN TRANSACTION LET a = (SELECT balance FROM ks.acct WHERE id = 1); SELECT a.owner; COMMIT TRANSACTION", "does not select a column owner")
	s.fails("BEGIN TRANSACTION COMMIT TRANSACTION", "Transaction contains no reads or writes")
	s.fails("BEGIN TRANSACTION SELECT * FROM ks.acct WHERE id = 1 ORDER BY id; COMMIT TRANSACTION", "")
}

// TestTransactionSchemaPinned: a transaction carries the schema it was
// made with; a replica whose table changed since reads nothing, and the
// transaction ends with a schema change, writing nothing.
func TestTransactionSchemaPinned(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("CREATE TABLE ks.acct (id int PRIMARY KEY, balance int) WITH transactional_mode = 'full'")
	tbl, err := s.d.schema.Load().table("ks", "acct")
	require.NoError(t, err)
	u := &accordUpdate{Mode: "write", Stmts: []accordStmt{{Query: "INSERT INTO ks.acct (id, balance) VALUES (1, 5)", Keyspace: "ks"}}}
	key := accordKey(tbl.Storage, []byte{0, 0, 0, 1})
	s.d.pinKeys(u, []accord.Key{key})
	require.NotZero(t, u.SchemaEpoch)
	require.Len(t, u.Tables, 1)
	b, err := json.Marshal(u)
	require.NoError(t, err)
	txn := &accord.Txn{Kind: accord.Write, Keys: []accord.Key{key}, Update: b}
	sc, changed, err := s.d.pinnedSchema(txn)
	require.NoError(t, err)
	require.False(t, changed)
	require.Same(t, s.d.schema.Load(), sc)

	// After a change, the transaction still runs against the snapshot it
	// was made with: every replica and coordinator computes the same
	// writes, whichever side of the ALTER it is on.
	s.ok("ALTER TABLE ks.acct ADD note text")
	sc, changed, err = s.d.pinnedSchema(txn)
	require.NoError(t, err)
	require.False(t, changed)
	require.NotSame(t, s.d.schema.Load(), sc)
	_, err = sc.table("ks", "acct")
	require.NoError(t, err)
	require.Nil(t, sc.byStorage[tbl.Storage].Column("note"))
	w, res, err := accordAgent{s.d}.Execute(txn, accord.Timestamp{Epoch: 1, HLC: 1}, map[string][]byte{})
	require.NoError(t, err)
	require.NotEmpty(t, w)
	require.NotEqual(t, accordSchemaChanged, res)

	// Once no snapshot with the table as it was is kept (here: dropped
	// from the history, as after a restart), the transaction reads nothing
	// and ends with the schema change, writing nothing.
	s.d.schemas.mu.Lock()
	s.d.schemas.list = nil
	s.d.schemas.mu.Unlock()
	_, changed, err = s.d.pinnedSchema(txn)
	require.NoError(t, err)
	require.True(t, changed)
	r, err := accordData{s.d}.Read(key, accord.Timestamp{}, txn)
	require.NoError(t, err)
	require.Equal(t, accordSchemaChanged, r)
	w, res, err = accordAgent{s.d}.Execute(txn, accord.Timestamp{Epoch: 1, HLC: 1}, map[string][]byte{"x": r})
	require.NoError(t, err)
	require.Empty(t, w)
	require.Equal(t, accordSchemaChanged, res)
	// Statements made after the change run.
	s.ok("INSERT INTO ks.acct (id, balance, note) VALUES (1, 5, 'n')")
	require.Equal(t, []string{"5 n"}, s.rows("SELECT balance, note FROM ks.acct WHERE id = 1"))
}
