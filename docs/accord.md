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
- **Epochs** (`Topologies`): each phase needs a quorum of every shard of
  every epoch from the TxnID's to the execution timestamp's. An Accept
  therefore also reaches the replicas of a newer epoch and their
  dependencies (CASSANDRA-18365). Nodes keep the last 64 topologies.
- **Journal** (`Journal`, `AccordJournal`): every state change is made
  durable before the replica answers, and replayed at startup. A
  transaction applied at every replica is truncated after `RetainApplied`
  to its id, status and timestamp. An erased command is only adopted once
  it is durably decided everywhere (CASSANDRA-18365).

## In the database

`internal/db/accord.go` connects the protocol to the database:

- **Which tables:** tables `WITH transactional_mode = 'full'` run
  conditional statements (single statements and conditional batches) as
  Accord transactions instead of Paxos. SERIAL reads of such tables first
  run a read-only transaction, so they are ordered after every transaction
  decided before them.
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

- **Transactions:** CQL transactions (`BEGIN TRANSACTION … COMMIT
  TRANSACTION`, `LET`) and non-conditional reads and writes through Accord
  (transactional_mode 'full' in Cassandra also routes those). Range
  transactions and sync points are not done either.
- **Topology changes:** bootstrap of Accord state (and data) onto new
  replicas. Nodes joining or leaving while Accord transactions run rely
  on multi-epoch quorums only.
- **Optimisations:** the medium path, privileged coordinators, ephemeral
  reads, per-shard command stores, and journal segments (each command is
  one synced row).
- **Truncated state:** commands truncated to a tombstone are kept forever
  (no `RedundantBefore` bound).
