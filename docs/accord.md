# Accord

`internal/accord` implements the Accord protocol (CEP-15), following the
design of Apache Cassandra's accord library (`cassandra-accord`:
`primitives`, `local`, `messages`, `coordinate`) and its integration in
Cassandra (`service/accord`). EPaxos was used only for background. The
Java code is a reference: the Go code reuses its concepts and message
names, not its class structure.

## Protocol

- **Timestamps** (`primitives.go`): epoch, hybrid logical clock, node,
  ordered in that order. Each node's clock ticks in microseconds: a new
  value is the wall clock's, unless the clock is already ahead (then one
  above its last value), and above any timestamp it was asked to follow;
  it observes every timestamp it receives. Two nodes can produce the same
  HLC: the node breaks the tie between timestamps. Cells, though, carry
  the HLC alone, so a transaction's writes take a separate write HLC,
  strictly above every conflicting write applied before it and decided
  once for all replicas (`UniqueHLC`, see "HLC order" below). An earlier
  clock gave every value a 10-bit per-node tag so that clocks never
  collided; rounding up to the tag spaced a node's transactions about
  1 ms apart, which under load ran the clock ahead of the wall clock and
  let a transaction's cells beat ordinary writes made just after it.
- **PreAccept** (`replica.go`, `Commands.preaccept`): If the transaction's TxnID is in the current epoch and newer than every conflicting transaction the replica has seen on those keys, the replica votes to execute it at TxnID. Otherwise, it proposes a later timestamp. It returns the conflicting transactions with lower ids
  (its dependencies).
- **Fast path** (`coordinate.go`, `CoordinateTransaction`): if a fast
  quorum of every shard voted for the TxnID, the transaction executes there
  with the merged dependencies of the fast votes. Fast quorums follow
  `Shard.simpleFastQuorumSize`: all 3 of 3 replicas, 4 of 5.
- **Privileged coordinator** (`PrivilegedCoordinatorWithoutDeps`): when the
  transaction has a single key and the coordinator replicates it, it votes
  first,
  locally, and its vote counts, so a shard's fast quorum is
  `privilegedWithoutDepsFastQuorumSize`: 2 of 3 replicas. It decides the
  fast path locally, and only if no recovery has promised a ballot there.
  A recovery that reaches the undecided coordinator therefore knows the
  fast path was not taken. A recovery that doesn't reach it assumes it
  voted. As in cassandra-accord `fc4a769`, multi-key transactions don't use
  it.
- **Medium path**: if a slow quorum of every shard (but not a fast quorum)
  voted for the TxnID, one Accept round of that proposal commits it, with
  the votes' dependencies. The replicas don't compute new ones. It is off
  for privileged transactions (cassandra-accord `fc4a769`: the medium path
  with coordinator-optimised quorums is not correctly implemented).
- **Reads** go to the coordinator itself, then to the nearest replicas
  (same rack, then same data center), as upstream picks read candidates.
  If none of a shard's read replicas could read (a dependency still
  recovering, a replica unreachable), the coordinator asks again every
  50 ms until its deadline: the decision is final, so giving up early only
  loses the outcome.
- **Slow path** (`Propose`): the coordinator proposes the latest timestamp
  to every replica in an Accept round. The Accept round also recomputes
  dependencies against that timestamp.
- **Commit and stable** (`Stabilise`, CASSANDRA-18365): the decision is
  committed to a quorum of every shard, and the read replicas are sent it
  as stable, with the read (`StableThenRead`), in the same round: the
  decision is already final (a fast quorum, or an Accept quorum, recorded
  it), and a read replica only reads. The transaction writes nothing until
  its write HLC is decided at a quorum (below), after the commit quorum;
  the other replicas are told it is stable then. Any dependencies a
  recovery adds to a stable decision execute after the transaction, so
  they cannot change its reads.
- **Execute** (`ExecuteTxn`, `StableThenRead`): a replica answers a read
  once every dependency that executes earlier has applied. The read
  happens under the store lock, so the command's own writes cannot slip in
  between. The coordinator computes the writes and result
  (`Agent.Execute`) and applies them everywhere (`Persist`). It answers
  once a quorum of every shard has applied them, then keeps retrying any
  replica that missed them, and marks the transaction durable once all
  have applied it. If the coordinator stops first, the progress log of a
  replica holding the transaction applied but not durable finishes the
  job (the home shard's replicas first): it sends the outcome to every
  replica, which a replica that never heard of the transaction needs, then
  marks it durable everywhere.
- **Recovery** (`Recover`, `BeginRecovery`): any replica's progress log
  recovers a transaction that has stalled: after `RecoverAfter`, or a
  tenth of it once its coordinator is known to be down (`Config.Alive`).
  A dependency the replica does not know is recovered on the keys it is a
  dependency on, which it touches: a shard it does not touch has no fast
  votes for it, which must not count as "no fast path". A recovery that
  learns the definition from a reply while running on other keys starts
  again on the transaction's own. The recovery coordinator takes a ballot
  and gathers a quorum of every shard, then acts on what it finds:

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
- **Waiting for another coordinator** (`awaitOutcome`): a coordinator
  preempted by a recovery, or whose attempt went stale, asks the replicas
  for the outcome. Once one has it (PreApplied or Applied), it applies it
  at a slow quorum of every shard before answering the client, as the
  coordinator's own persist does. A QUORUM read outside Accord right after
  the answer then sees its writes; this matters on tables whose
  conditional statements, or writes, run on Accord while their reads do
  not.
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
  - **Synchronisation:** older epochs leave the quorums only once no
    transaction of theirs can still be in progress: a leader runs sync
    points over the ranges that changed, then tells every node the epoch is
    synchronised (see "Epoch synchronisation" below).
  - **Stale epochs:** a replica refuses a new transaction from an epoch
    before that (`StaleTopology`). The coordinator then recovers it (so it
    never executes) and retries in the newer epoch.
  - **Unknown epochs:** a replica sent a message of an epoch it does not
    know yet (the transaction's id, or its execution timestamp) waits for
    it, and asks the sender for the metadata (`FetchEpoch`); a coordinator
    waits likewise before it proposes, executes or applies at an epoch a
    replica proposed. A replica never executes a transaction before it
    knows both epochs. Ownership is decided by the epochs a transaction
    spans: with an older epoch in their place, a node behind the one that
    made it a pending replica of a key applied the transaction without
    that key, and the write was lost there once it learnt the epoch.
  - **HLC order:** transactions execute in timestamp order (epoch
    first, then HLC, then node), while their writes are cells timestamped
    with an HLC alone. Two mechanisms keep the written cells in execution
    order without changing the SSTable cell format:
    1. An execution timestamp exceeds every conflicting one its replicas
       witnessed in HLC too (`mergeMax`, as upstream's
       `Timestamp.mergeMax`).
    2. When a replica answers a read at execution time, it reports a
       minimum HLC strictly above every earlier write applied on those
       keys (and above any streamed bootstrap bound); the coordinator
       executes and persists with `UniqueHLC = max(ExecuteAt.HLC, MinHLC)`
       (`TimestampWithUniqueHlc` / `ExecuteTxn` in upstream). This covers
       both an older-epoch transaction taking the slow path at a higher
       HLC after a newer-epoch fast-path transaction committed, and two
       slow-path transactions across shards proposing the same microsecond
       HLC on different nodes.
    3. That HLC is decided before any replica applies the transaction
       (`DecideUniqueHlc`): it depends on what the executor's read
       replica applied, so two executors (its coordinator and a
       recovery) reading different replicas can compute different ones,
       and each replica kept the first it was sent; a later transaction
       written just above one replica's then lost to another's. The
       executor has a quorum of every shard accept its HLC at its ballot
       (a replica that promised a higher ballot refuses: the executor
       was preempted; one that already applied the transaction answers
       the HLC it applied, which is final). A recovery adopts the HLC
       accepted at the highest ballot among its replies: a quorum that
       accepted one intersects its own. The reads go out with the commit
       round instead of after it, so that this costs no extra round trip.
  - **Late ownership** (`late_owned`): a transaction applied here that
    gains a key this node owned at the epoch it executed at is counted.
    Gaining keys of a later epoch is normal (a range the node starts
    receiving: the streamed data holds the write).
- **Exclusive sync points** (`syncpoint.go`, `CoordinateSyncPoint`): a
  transaction over ranges, with no reads or writes.
  - **Proposal:** a quorum of every shard (in every epoch not yet
    synchronised) witnesses it. Each replica records a barrier for its
    ranges and returns every transaction there with a lower id.
  - **Barrier:** a replica that witnessed it refuses any new transaction
    with a lower id (PreAccept and the original Accept answer "stale"; a
    recovery reports a superseding reject). Such a transaction's coordinator
    recovers it, which invalidates it, and retries with a new id.
  - **Applying:** a sync point is applied once every one of its
    dependencies has applied, so afterwards every transaction with a lower
    id on its ranges has applied or can never commit.
  - **Abandoned ones:** a sync point is never recovered. One its coordinator
    abandons only leaves barriers, and is invalidated locally after a while.
- **Epoch synchronisation**: for each new epoch, in order, the leader (the
  live normal node with the lowest address among those whose gossiped
  metadata epoch is at least this node's: a node cut off from the metadata
  log never synchronised the epochs it did not know, and no other node
  did either) finds the ranges whose replicas
  or pending replicas changed from the previous epoch (a range starting to
  move to a node changes only its pending replicas: earlier-epoch
  transactions, which do not reach that node, must be done before data is
  streamed to it).
  1. It runs one sync point over all of them, with quorums in both
     epochs. The barrier refuses any earlier-epoch transaction the sync
     point doesn't follow. A sync point needing quorums returns as soon as
     they applied it, not once every replica answered (one that does not
     answer cost a request timeout per changed range, which a node joining
     waits for).
  2. Once the sync points apply, no earlier-epoch transaction is in progress
     there, and every node is told the epoch is synchronised. Each node
     journals its synchronised epoch, and the leader re-sends it every
     durability interval, for nodes that restarted without it or joined
     since. A node waiting for an epoch to be synchronised (one about to
     bootstrap) also asks the others for theirs (`GetSyncedEpoch`) every
     200 ms, instead of waiting for the leader's notice, which a node
     joining was not there for.
  3. Phases then stop including the earlier epochs. An epoch that changed no
     replicas (a schema change) needs no sync point.
- **Durability** (`CoordinateShardDurable`, `RedundantBefore`): each node
  periodically runs a sync point over the ranges it is the first replica
  of, applied at every replica (pending replicas included: a bootstrap's
  sync point, whose pending replicas are the nodes about to receive the
  data, does not wait for them). It skips
  ranges with no transaction above their current bound. Each sync point's
  id then becomes the range's redundancy bound on every replica and
  pending replica.
  - **Forgetting:** below the bound, applied and invalidated records are
    forgotten (with their journal records).
  - **Late messages:** a late message about a forgotten transaction is
    refused (PreAccept, Accept), and its recovery reports it durable.
  - **Unknown dependencies:** below the bound they count as applied.
  - **Missed bounds:** a replica that missed a bound and waits on a
    transaction the others forgot learns the bound from their status reply
    (`Truncated`, with their bounds) and adopts it: the sync point applied
    there too. The adopted bound is journaled before anything below it is
    forgotten.
  - **Majority fallback** (`SetMajorityDurable`): when a replica is down
    the universal sync point times out. The round then settles for one
    applied at a majority: barriers rise everywhere, and applied records
    below it drop their local dependencies. A replica coming back catches
    up through the next universal round, whose sync point waits for (and
    fetches) what it missed.
  - **Idle keys:** a key with no transactions left whose latest timestamp is
    below its bound is forgotten; the barrier refuses anything it would
    have ordered.
- **Joining nodes** (`Bootstrap`): before streaming, a joining node takes
  one sync point over all the ranges it will own in every table, applied
  at a quorum of every shard, and kept once it is: it then waits half a
  request timeout at most for the other replicas, and not at all for those
  the failure detector believes down (`Config.Alive`). Those that did not
  apply it (down, cut off, behind the epoch) are only left out of the
  sources (below); it used to wait for every replica first, a whole 8 s
  when one did not answer. If it fails the join fails (and can be retried), unless a
  range has no live quorum (`ErrUnavailable`, a replica being replaced at
  RF 2): no transaction commits there meanwhile, and the node goes without
  the point.
  - **One sync point, after the epoch is synchronised:** it first waits
    (briefly, best effort) until the epoch it learnt of the movement in is
    synchronised: until then, a transaction of an earlier epoch, whose
    replicas do not include the new node, may commit with an id above the
    sync point and never reach it. One sync point over every range, not
    one per range: a transaction on a range with an id between that range's
    point and a later one's would be covered by neither.
  - **Per range:** a node keeps one bootstrap point per range it received
    (the store's `bootstrapped` bounds), not one for the node. It only skips
    unknown dependencies where its data reflects them: on a range it
    replicated all along, it waits for each one. A single node-wide point,
    raised by a later bootstrap, let a transaction on such a range execute,
    and read, before a dependency's writes had applied there.
  - **Sources:** the point is only valid where the data comes from: the
    data is streamed only from replicas that applied the sync point.
  - **Leaving and removed nodes' ranges:** the node driving the movement
    takes the sync point (`BootstrapPoints`) before streaming, applied at
    every node the data is streamed from, and tells the nodes taking over
    once they have the data (`SendBootstrapped`, best effort: a node not
    told fetches the earlier transactions instead). The sync point is not
    optional: without it, a transaction of the epoch before could apply at
    the source after the export, and at a node taking over only on the keys
    it replicated then. Its failure fails the movement, as for a join.
    Learning a point wakes the commands waiting on dependencies below it.
  - **Persisted:** the bootstrap points are journaled and restored on
    restart.
  - **Streamed data:** it then holds every transaction before the sync
    point; later ones reach the node as a pending replica, since writes go
    to the replicas of every epoch up to the current one.
  - **Earlier dependencies:** the node doesn't wait to apply earlier ones it
    doesn't know, because writes are timestamped cells and the latest wins
    in any order. Its reads wait until they are fetched; it answers "not
    ready" and the coordinator reads from another replica.
  - **Reads:** a replica reads, for a transaction, only the keys it is a
    read replica of at the execution epoch, and waits only for their
    dependencies: keys it is only pending on may still be streaming to it.
    (It answered them, and the coordinator then skipped their own shard;
    and an unknown dependency on them held up the read of the others until
    the request timed out.) The coordinator keeps a reply's reads only for
    the keys its replica reads for.
- **Shards** group a transaction's keys by their replicas and their
  pending replicas together: a node pending on one key (taking over its
  range) is not an owner of keys that merely share its replicas, whose
  transactions never reach it.
- **Journal** (`Journal`, `AccordJournal`): every state change is made
  durable before the replica answers, and replayed at startup.
  - **Group commit** (`BatchJournal`): handlers encode their records under
    the store lock and queue them; one writer makes each batch durable with
    one sync, and a handler waits for its records after releasing the lock.
    Replicas also read transaction data outside the lock (a command's own
    writes wait for its reads in progress).
  - **Errors:** a failed batch is retried until it is written, and nothing
    queued after it counts as durable before it: the replica stops
    answering rather than answer with state the journal lacks (its memory
    already holds that state, so even a retried request would otherwise be
    answered from it). Failures are counted (`journal_errors`). A journal
    without batches goes through the same writer, one record at a time.
  - **Truncation:** a transaction applied at every replica is truncated
    after `RetainApplied` to its id, status and timestamp. An erased
    command is only adopted once it is durably decided everywhere
    (CASSANDRA-18365).
  - **Forgetting:** below the durability bound, as above. Without
    durability rounds, truncated and invalidated records are forgotten after
    `TombstoneRetention`, and each key keeps the largest timestamp it
    witnessed, so a late message about a forgotten transaction gets no
    fast-path vote.
- **Progress** (`ProgressLog`): a stalled transaction is recovered first by
  the replicas of its home key (the first); the others wait three times
  longer. Background retries of an outcome to replicas that missed it are
  bounded (1024 transactions); beyond that, replicas catch up by fetching
  and through durability rounds.
- **Observability** (`Stats`, `Commands`), exposed as two virtual tables:
  - `system_views.accord_stats`: fast, medium and slow paths, recoveries,
    invalidations, ephemeral and range reads, sync points, durability
    and majority rounds, epoch syncs, journal errors, shed retries, stale
    refusals, timeouts, command and key counts, barriers,
    redundancy bounds, and epochs.
  - `system_views.accord_txns`: the transactions in progress on the node,
    with their status, execution timestamp and what they wait on
    (`DebugBlockedTxns`).

## In the database

`internal/db/accord.go` connects the protocol to the database:

- **Every table** runs its conditional statements (and conditional
  batches), its LOGGED batches spanning several partitions, and SERIAL
  reads on Accord: Paxos was removed, and there is no batchlog. A logged
  batch is then atomic (all or nothing, and never seen half applied); its
  cells keep the client's timestamp (`USING TIMESTAMP`, or the protocol
  batch's), and it needs a quorum of each partition's replicas, whatever
  the consistency level. UNLOGGED and COUNTER batches, and logged ones on
  one partition (atomic anyway), are applied as plain writes. The
  `transactional_mode` table option chooses what else does:
  - `off` (the default): nothing else. The transaction reads the
    conditions at QUORUM from the executing replica (`AccordInteropRead`),
    because other writes bypass Accord. SERIAL reads run an Accord
    read-only transaction, then read at QUORUM with blocking read repair.
  - `mixed_reads`: plain writes run on Accord too, and the table takes
    `BEGIN TRANSACTION` (with interop reads); non-SERIAL reads don't.
  - `full`: every statement runs on Accord.

  When fewer than a quorum of a key's replicas are alive, the statement
  fails with Unavailable before coordinating.
- **`full` tables** route every statement through Accord:
  - conditional statements and conditional batches;
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
- **Schema pinning:** a transaction carries the metadata epoch it was made
  at and a fingerprint of each table it uses (columns, types and options).
  Replicas and coordinators prepare its statements against a metadata
  snapshot whose tables have those fingerprints: the current one, or one of
  the last 64 kept. So every one of them, on either side of an `ALTER`,
  computes the same reads and writes. A node behind the transaction's epoch
  catches up first. Only when no kept snapshot matches (a change long ago,
  or before the node restarted) does the replica read nothing; the
  transaction then writes nothing and fails with "the schema of a table
  changed during the transaction; retry it".
- **Storage:** the journal is an engine table (one row per command); each
  group-committed batch is one synced write. Messages use `VerbAccord` on their own stage.

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

  Liveness: of the transactions whose coordinator stayed up, at least half
  are acknowledged (one whose coordinator crashed ends unknown to its
  client).

  The simulation runs in a `testing/synctest` bubble: a fake clock that
  advances only when every goroutine of the cluster is blocked. Delays,
  timeouts and the fault schedule therefore play out the same on a loaded
  machine, or under the race detector, as on an idle one, and waiting costs
  no real time: the crash test takes about 4 s instead of 30–45 s. The
  network's delays and losses and the faults are drawn from a fixed seed.
  The goroutine interleaving is still the Go scheduler's, so two runs are
  not identical, but the checks hold on every run (400 of 400 crash runs).
  A crash is a real one: `Node.Stop` cancels and waits for everything the
  node started, so a crashed node sends nothing more.
- `internal/accord/recovery_test.go` covers specific cases:
  - a fast-path commit that reached one replica, recovered without it;
  - a transaction barely witnessed, then invalidated;
  - Accept reaching a newer epoch's replicas;
  - a transaction applied at a quorum whose coordinator stopped before
    reaching the last replica, which never heard of it: the progress log
    re-sends the outcome to every replica, then marks it durable;
  - a dependency recovered by a node that does not know it, on keys it does
    not touch: those shards have no votes for it, which must not count as
    "no fast path" (that invalidated a transaction decided on the fast
    path).
- `internal/accord/syncpoint_test.go`:
  - a sync point under load (every dependency applied, lower ids refused);
  - durability rounds every 300 ms under message loss (all checks hold,
    records forgotten, late messages refused);
  - bootstrap at a sync point, kept across a restart;
  - per-range bootstrap points: a later bootstrap leaves earlier ranges'
    points alone, and points sent by another node (`SendBootstrapped`) are
    kept across a restart;
  - an unknown earlier dependency skipped on a bootstrapped range and
    waited for on another;
  - shards split by pending replicas (`review_test.go`);
  - `moving_test.go`: a replica behind the epoch waits for it before
    applying, reads skip keys a replica is only pending on, pending
    replicas changing a range's epoch sync, bootstrap points waking
    waiting commands, late ownership counted, `UniqueHLC` across epochs
    and same-HLC ties, a recovery adopting the decided write HLC, a
    preempted executor's HLC refused, `propose` keeping input
    dependencies, universal durability sync points waiting for pending
    replicas, and a bootstrap's sync point not waiting for them; a quorum
    sync point returning without waiting for a silent replica, a bootstrap
    point lingering only for live replicas, a node waiting for an epoch to
    be synchronised asking the others (`GetSyncedEpoch`), and the decided
    write HLC floored at the execution timestamp and waiting for the
    execution epoch;
  - majority durability with a replica down, and its catching up.
- `internal/accord/review_test.go`: recovery across epochs, a `Truncated`
  outcome, and ephemeral reads retried at a newer epoch.
- `TestTransactionSchemaPinned` (`internal/db`): a table changed under a
  transaction.
- `TestClusterAccordDecommission` (`internal/db`): transfers while a node
  leaves; balances are conserved, the replicas converge, the nodes taking
  over learn their bootstrap points, and no late ownership is counted.
- `TestPrivilegedRestrictions`: the two cassandra-accord `fc4a769`
  restrictions.
- `internal/db` cluster tests:
  - `TestClusterAccord`: concurrent compare-and-set from every node, with a
    replica down, and then after it restarts;
  - `TestClusterLWT` and `TestClusterInterop`: lightweight transactions on
    ordinary and `mixed_reads` tables;
  - `TestClusterAccordDurability`: durability rounds, with the virtual
    tables reporting them.

## Not done

- **Range reads:** they are ephemeral, so they form a per-partition
  snapshot, not one across the range. Sync points are the only range
  transactions; as in Cassandra, `BEGIN TRANSACTION` rejects range reads.
- **Unported upstream work:** per-shard command stores, a segmented binary
  journal (records are JSON rows, group-committed), partial dependencies, `PrivilegedCoordinatorWithDeps`,
  replica-side execution, and Paxos migration (unnecessary here: Paxos was
  removed).
- **Transaction results:** the result of a transaction is not paged (a
  transaction executes once, as in Cassandra).
