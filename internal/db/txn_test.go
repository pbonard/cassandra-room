package db

import (
	"testing"

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
