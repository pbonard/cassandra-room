# Accord

`internal/accord` implements the Accord protocol (CEP-15), following the
design of Apache Cassandra's accord library (`cassandra-accord`:
`primitives`, `local`, `messages`, `coordinate`) and its integration in
Cassandra (`service/accord`). EPaxos was used only for background. The
Java code is a reference: the Go code reuses its concepts and message
names, not its class structure.

## Protocol

- **Timestamps** (`primitives.go`): epoch, hybrid logical clock, node.
  Each node's clock is strictly increasing and carries a per-node tag in
  its low 10 bits. Clock values from different nodes therefore never
  collide, so an execution timestamp's hlc can be the write timestamp of
  the transaction's updates (`Timestamp.uniqueHlc`).
- **PreAccept** (`replica.go`, `Commands.preaccept`): a replica proposes
  executing at the TxnID if nothing conflicting on its keys has a later
  timestamp and the TxnID's epoch is current. Otherwise it proposes a new,
  later timestamp. It returns the conflicting transactions with lower ids
  (its dependencies).
- **Fast path** (`coordinate.go`, `CoordinateTransaction`): if a fast
  quorum of every shard voted for the TxnID, the transaction executes there
  with the merged dependencies of the fast votes. Fast quorums follow
  `Shard.simpleFastQuorumSize`: all 3 of 3 replicas, 4 of 5.
- **Privileged coordinator** (`PrivilegedCoordinatorWithoutDeps`): when the
  coordinator replicates one of the transaction's keys, it votes first,
  locally, and its vote counts, so a shard's fast quorum is
  `privilegedWithoutDepsFastQuorumSize`: 2 of 3 replicas. It decides the
  fast path locally, and only if no recovery has promised a ballot there.
  A recovery that reaches the undecided coordinator therefore knows the
  fast path was not taken. A recovery that doesn't reach it assumes it
  voted.
- **Medium path**: if a slow quorum of every shard (but not a fast quorum)
  voted for the TxnID, one Accept round of that proposal commits it, with
  the votes' dependencies. The replicas don't compute new ones.
- **Slow path** (`Propose`): the coordinator proposes the latest timestamp
  to every replica in an Accept round. The Accept round also recomputes
  dependencies against that timestamp.
- **Commit, then stable** (`Stabilise`, CASSANDRA-18365): the decision is
  committed to a quorum of every shard before any replica is told it is
  stable. Only stable commands execute, so any recovery finds the
  decision first.
- **Execute** (`ExecuteTxn`, `StableThenRead`): a replica answers a read
  once every dependency that executes earlier has applied. The read
  happens under the store lock, so the command's own writes cannot slip in
  between. The coordinator computes the writes and result
  (`Agent.Execute`) and applies them everywhere (`Persist`). It answers
  once a quorum of every shard has applied them, then keeps retrying any
  replica that missed them, and marks the transaction durable once all
  have applied it.
- **Recovery** (`Recover`, `BeginRecovery`): any replica's progress log
  recovers a transaction that has stalled. The recovery coordinator takes a
  ballot and gathers a quorum of every shard, then acts on what it finds:

  | What the quorum reports | What recovery does |
  | --- | --- |
  | Invalidated | Commits the invalidation. |
  | Applied | Applies again everywhere. |
  | Stable or Committed | Re-stabilises and executes. |
  | Accepted (highest ballot) | Re-proposes that timestamp, with dependencies from the latest phase merged with what replicas witnessed (`LatestDeps`). |
  | Nothing accepted | Checks whether the fast path was possible (see below). |

  The fast path cannot have been taken, and recovery invalidates the
  transaction, in either of these cases:

  - too many replicas in the quorum did not vote for the fast path; or
  - a transaction that executes later has decided, or proposed with a later
    TxnID, dependencies that omit this one (`supersedingRejects`).

  Earlier transactions that may execute later but are only Accepted are
  awaited first (`simpleWait`). Otherwise recovery proposes the TxnID.
- **Invalidation** (`Invalidate`): AcceptInvalidate at a quorum, then the
  invalidation is committed everywhere.
- **Progress log** (`SimpleProgressLog`) finds stalled transactions:
  - an undecided command left untouched;
  - a stable command waiting on a dependency.

  It first fetches the outcome from other replicas (`FetchData`,
  `Propagate`), and recovers the transaction when nobody has finished it.
- **Ephemeral reads** (`CoordinateEphemeralRead`): a single-key read-only
  transaction is not committed or journaled.
  1. A quorum of every shard reports the writes on the key that are not yet
     applied at every replica.
  2. One replica waits until those have applied there, then reads.

  Every write acknowledged before the read started is applied at a quorum,
  and the dependency quorum intersects it. Reads of several keys use the
  whole protocol, so that they form one snapshot.
- **Epochs** (`Topologies`): each phase needs a quorum of every shard of
  every epoch from the TxnID's (or the oldest epoch not yet synchronised)
  to the execution timestamp's.
  - **Newer epochs:** an Accept therefore also reaches the replicas of a
    newer epoch and their dependencies (CASSANDRA-18365). A replica that
    knows a newer epoch doesn't vote for the fast path of an older one.
  - **Synchronisation:** every node polls the others for the oldest epoch
    they still have transactions of in progress. Older epochs leave the
    quorums only once no node has any.
  - **Stale epochs:** a replica refuses a new transaction from an epoch
    before that (`StaleTopology`). The coordinator then recovers it (so it
    never executes) and retries in the newer epoch.
- **Joining nodes**:
  - **While bootstrapping:** a pending replica receives every Apply, and
    writes go to the replicas of every epoch up to the current one, so its
    streamed data plus those writes are complete.
  - **Old dependencies:** a dependency from before the node joined that it
    doesn't know doesn't hold up applying, because its effect came with the
    bootstrap data, and writes are timestamped cells (the latest wins in any
    order). Reads at that node wait until such dependencies are fetched; it
    answers "not ready" and the coordinator reads from another replica.
- **Journal** (`Journal`, `AccordJournal`): every state change is made
  durable before the replica answers, and replayed at startup.
  - **Truncation:** a transaction applied at every replica is truncated
    after `RetainApplied` to its id, status and timestamp. An erased
    command is only adopted once it is durably decided everywhere
    (CASSANDRA-18365).
  - **Forgetting:** truncated and invalidated records are forgotten after
    `TombstoneRetention`, and their journal records deleted. Each key keeps
    the largest timestamp it witnessed, so a late message about a forgotten
    transaction gets no fast-path vote. Its recovery invalidates it.

## In the database

`internal/db/accord.go` connects the protocol to the database:

- **Which tables:** tables `WITH transactional_mode = 'full'` route every
  statement through Accord:
  - conditional statements and conditional batches, instead of Paxos;
  - plain writes and batches, as one transaction each (custom timestamps,
    and batches mixing transactional and other tables, are refused);
  - single-partition reads at any consistency, as read transactions,
    paged: each page is one transaction carrying the select's paging state;
  - reads of several partitions, which first run a read-only transaction
    on each;
  - range reads (`TxnRangeRead`, read ephemerally):
    1. The range is split by the replica sets of the newest epoch.
    2. For each sub-range in token order, a quorum of its replicas (in every
       epoch not yet synchronised) reports the writes in it not yet applied
       everywhere.
    3. One replica reads the sub-range once those writes have applied there.

    Replicas keep the keys they index in token order, so a range's pending
    writes are found by binary search, not by scanning every key.

    Each partition read reflects every write acknowledged before the read
    started. The reads go sub-range by sub-range until the page is full, and
    the paging state records the sub-range to resume from, its own paging
    state, and what is left of the `LIMIT`.

    Aggregates and `GROUP BY` over ranges:
    - **`GROUP BY`:** groups always hold whole partitions, so each sub-range
      returns complete groups, and the `LIMIT` is applied across them.
    - **Plain aggregates:** each sub-range returns its partial state (count,
      sum, min and max, first values), which the coordinator merges.
      `avg` is merged as a sum and a count.
- **Transactions** (`txn.go`, `TransactionStatement`):
  `BEGIN TRANSACTION … COMMIT TRANSACTION` supports:
  - `LET` assignments of one row (the full primary key, or the partition
    key and `LIMIT 1`);
  - a `SELECT`, or a `SELECT` of `LET` references, returned to the client;
  - `IF … THEN`, `ELSE IF … THEN`, `ELSE … END IF` blocks, with comparisons
    and `IS [NOT] NULL` on references;
  - references as update values;
  - arithmetic (`+ - * / %`, with precedence) on references, literals and
    bind markers, in values and conditions. Operand types come from the
    references, and a null operand gives null;
  - `c += v` and `c -= v` on numeric columns (an implicit read of the
    updated row; a missing value counts as zero);
  - Cassandra's validation messages.

  In a cluster, the statement is one Accord transaction over every
  partition it reads or may write. Replicas read the `LET` rows and the
  `SELECT`; the coordinator evaluates the blocks and updates. On a single
  node it runs under a lock.
- **Keys:** a key is a partition: its token, plus the table's storage id
  and the partition key. An epoch's topology maps it to the replicas of its
  keyspace in that epoch's ring.
- **Transaction contents:** a transaction carries the statements' text and
  bound values, so any node can execute it again. Replicas read the rows
  the conditions need from their own data. The coordinator checks the
  conditions and computes the writes, timestamped with the execution hlc.
  List element paths are deterministic, so every coordinator computes the
  same writes.
- **Storage:** the journal is an engine table (one row per command), synced
  on every write. Messages use `VerbAccord` on their own stage.

## Tests

- `internal/accord/sim_test.go` runs an in-memory cluster of 5 nodes at
  RF 3 under these conditions:
  - no faults;
  - 5% message loss;
  - crashes and restarts from the journal;
  - partitions.

  It runs a list-append workload, as accord's burn test does, and checks
  four things:
  - every replica of a key holds the same list, in execution order;
  - every acknowledged transaction read exactly the values appended before
    its execution timestamp;
  - acknowledged appends are applied once, and invalidated ones never;
  - real-time order is respected (strict serializability).
- `internal/accord/recovery_test.go` covers specific cases:
  - a fast-path commit that reached one replica, recovered without it;
  - a transaction barely witnessed, then invalidated;
  - Accept reaching a newer epoch's replicas.
- `TestClusterAccord` (`internal/db`) runs concurrent compare-and-set from
  every node, with a replica down, and then after it restarts.

## Not done

- **Range transactions:** range reads are ephemeral, so they form a
  per-partition snapshot, not one across the range. Range transactions
  that commit (with range dependencies and sync points) aren't done; as in
  Cassandra, transactions reject range reads.
- **Epoch synchronisation:** it is per node, not per range, and a node
  still on an old epoch holds it back for everyone.
- **Write coverage after joins:** a write that a joining node missed while
  down (after the Apply retries give up) is only repaired when a read
  fetches it, or by repair.
- **Other optimisations:** per-shard command stores, journal segments
  (each command is one synced row), and `PrivilegedCoordinatorWithDeps`.
- **Transaction results:** the result of a transaction is not paged (a
  transaction executes once, as in Cassandra).
