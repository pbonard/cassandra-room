# Clusters

Nodes form a cluster as Cassandra nodes do, with the same consistency
levels and the same failure behaviour seen from drivers. Everything is in
`internal/cluster` (transport, membership, ring) and `internal/db/proxy.go`,
`topology.go` (coordination), using only the Go standard library (no memberlist or Raft dependency).

```
cassandra-room -addr 10.0.0.1:9042 -listen-address 10.0.0.1:7000 -seeds 10.0.0.1:7000 -admin-address 10.0.0.1:7199
```

## Membership and ring

Cassandra's legacy design gossips everything (tokens, status, schema
versions) and lets each node derive the ring from what it has heard. Here,
as in CEP-21 (Transactional Cluster Metadata, Cassandra 5.1+), the two
jobs are split: **gossip only says who is alive**, and **cluster metadata
is a linearized log of epochs**.

- **Messaging** (`MessagingService`, `internal/cluster/messaging.go`):
  request/response over TCP, with Cassandra 4.0's protections:
  - up to three connections per peer by message class: urgent
    (membership, metadata service), small (reads, writes), large
    (streaming, range reads, repair, and any message over 64 KiB), so a
    large message never delays a heartbeat or a write;
  - a handshake agreeing on the protocol version (refusing peers with no
    common version), then frames carrying a CRC32C of header and payload
    (a mismatch closes the connection) and the sender's deadline;
  - queue limits as `internode_application_*_queue_*`: 4 MiB per
    connection, then 128 MiB per endpoint and 512 MiB in all. Outbound, a
    request past them fails as overloaded; inbound, a connection past them
    is not read until requests finish, which pushes back through TCP;
  - requests past their deadline when they arrive or when a worker frees
    up are dropped and counted; handlers get the deadline as a context and
    long ones (range reads) stop when it passes; each kind of work (reads,
    writes, gossip, metadata, streaming, statements) has a bounded number
    of workers;
  - optionally mutually authenticated TLS (`-internode-cert/-key/-ca`,
    checking the CA chain, and names with `-internode-verify-hostname`)
    and LZ4 compression of payloads over 512 bytes
    (`-internode-compression`, on by default).
- **Membership** (`membership.go`, after hashicorp/memberlist: SWIM with
  the Lifeguard extensions):
  - every probe interval (1 s) a node pings one member, in shuffled
    round-robin order; with no ack it asks 3 others to ping it (indirect
    probe);
  - a member that fails both probes becomes *suspect*; the suspicion
    timeout starts at `SuspicionMaxTimeoutMult` × min and shrinks towards
    min (`SuspicionMult` × log10(N) × interval) as other members confirm it
    independently; then the member is *dead*;
  - a node told it is suspect refutes it by raising its incarnation;
    liveness is ordered by (generation, incarnation), so a restarted node
    (new generation) is alive again at once;
  - the local health score (awareness) stretches probe timeouts when this
    node itself is slow, so it does not accuse healthy peers;
  - alive/suspect/dead messages are piggybacked on gossip every 200 ms to 3
    random members, each retransmitted RetransmitMult × ceil(log10(N+1))
    times (transmit-limited queue);
  - a full state exchange with a random member every 30 s (push-pull)
    repairs anything gossip lost, and is how a new node joins through a
    seed.

  The endpoint state is only address, host id, generation and the node's
  **metadata epoch**; nothing about tokens or schema.
- **Cluster metadata** (`cms.go`, `internal/db/metadata.go`, after
  CEP-21): the schema, the roles and the node directory (address, host id,
  native address, DC, rack, tokens, status) form one snapshot with an
  epoch. Every change (DDL, role changes, a node registering, joining or
  leaving) is a transformation: the proposer plans it on a copy of the
  current snapshot and the **Cluster Metadata Service** chooses the value
  of the next epoch by single-decree Paxos among its members (the seeds).
  Concurrent changes are serialized: a proposer whose slot was taken
  catches up and plans again. Each change carries an entry id, so a change
  chosen through another proposer is not applied twice. Committed
  snapshots are pushed to the live nodes; a node that missed one sees a
  higher epoch in gossip and fetches the snapshot from that peer. A node
  installs snapshots in epoch order only, creating storage for new tables
  and dropping storage of removed ones.
- **Ring** (`ring.go`): built from the metadata's node directory, never
  from gossip, so all nodes at the same epoch agree on the replicas. Each
  node has `-num-tokens` random vnode tokens. SimpleStrategy and
  NetworkTopologyStrategy (rack-aware) place replicas.

## Coordination (StorageProxy)

Statements run through a storage interface: the local engine on a single
node, the coordinator in a cluster.

- **Writes** go to every replica of each partition; the coordinator waits
  for the consistency level's blockFor acks per partition. Too few live
  replicas: `Unavailable`; no answer in time: `WriteTimeout`. Down or
  failing replicas get **hints**, stored locally and replayed when they come
  back (and every 5 s). Nodes joining or taking over a range also receive
  its writes (pending ranges).
- **Reads** of a row ask one replica (this node first) for the data and
  the others blockFor needs for a digest; matching digests answer at once,
  a mismatch reads full rows from everyone who answered and merges them
  (row fragments merge by timestamp, so the merge is the reconciliation).
  A replica slower than the 99th percentile of recent replica reads, or
  failing, gets another replica asked in its place (speculative retry). Replicas that
  returned less get the merged row (blocking read repair). Range scans read
  one ring range at a time, in pages, cutting each page at the first
  replica page that was truncated.
- **Schema** and role changes are metadata commits (above), from any node.
- **Counters** are CounterMutations: the coordinator builds deltas, each
  partition's first live replica (its leader) turns them into its counter
  shard and replicates the result; a statement or batch may span
  partitions.
- **EACH_QUORUM** under NetworkTopologyStrategy needs a quorum in every
  data center, for reads and writes; writes also wait for pending
  replicas on top of any consistency level (blockForWrite).
- **Lightweight transactions** run single-decree Paxos per partition among
  its replicas (Cassandra's Paxos v1, as ScyllaDB's `service/paxos`):
  prepare (finishing an accepted but uncommitted round, repairing replicas
  that missed the last commit), a QUORUM read and the condition check,
  propose (the update timestamped with the ballot), commit at the
  statement's consistency level, and prune once every replica learnt it.
  Each replica's promise, accepted proposal and last commit are cells of a
  `system.paxos`-like table, synced before answering. SERIAL reads finish
  rounds in progress first. Any node can coordinate.
- **Secondary index** queries walk the ring's token ranges in order, as
  Cassandra's index range reads do: the coordinator asks one live replica
  of each range to read its local index for that range, until the page is
  full, and the paging state records the next range (and the replica's
  own paging state inside it). LIMIT holds across pages. ANN queries ask
  every node for its best rows and merge them by score (LIMIT at most
  1000).
- Drivers see the cluster through `system.peers`/`peers_v2` and route
  token-aware.

## Topology (admin endpoint)

- **Streaming** (`internal/db/stream.go`) moves ranges as sstable files,
  as Cassandra's entire-sstable streaming does: the source exports the
  range (sstables wholly inside it hard-linked, the sections of the others
  written out), the receiver pulls the files in 4 MiB chunks over the
  large-message connection, imports them (hard-linked into the table, not
  rewritten) and writes the index entries of the imported rows. A node
  that must push a range asks the receiver to pull it. Bootstrap,
  decommission and removenode record every range they finish
  (`stream-progress.json`), so an interrupted operation run again resumes
  where it stopped. Not done: sendfile (chunks go through messages).
- **Bootstrap**: a new node catches up with the metadata, then commits its
  registration as JOINING (TCM's PrepareJoin: from that epoch writes also
  go to it), streams the ranges it will own from current replicas,
  rebuilds its local indexes, and commits NORMAL (FinishJoin). The first
  node of a cluster registers as NORMAL directly.
- **Decommission** (`POST /decommission`): refused for CMS members, and if
  fewer nodes than a replication factor would remain; otherwise the node
  commits LEAVING (PrepareLeave), streams its ranges to their new replicas,
  replays its hints, and commits LEFT (FinishLeave).
- **Repair** (`POST /repair?keyspace=`): for each range this node
  replicates, each replica builds a Merkle tree (the engine's, over purged
  rows); differing leaves are streamed both ways.
- **Cleanup** (`POST /cleanup?keyspace=`): rewrites the tables (and their
  indexes) without the rows of ranges this node no longer replicates, as
  nodetool cleanup does after nodes join; refused while nodes are joining
  or leaving.
- **Removenode** (`POST /removenode?node=`, an address or host id): a dead
  node is committed LEAVING; for each of its ranges, the nodes that become
  replicas get the data from a surviving replica (the coordinator asks the
  source to push it); then it is LEFT.
- **Replace** (`-replace-address` on a new node's first boot): the new
  node takes the dead node's tokens as JOINING with `Replaces` set (writes
  of those ranges reach it as a pending replica, the dead node counts as
  down even if the address is reused), streams them from the other
  replicas, then one commit makes it NORMAL and the dead node LEFT.
- **Metadata service membership** (`POST /cms/reconfigure?members=`) is
  part of the metadata: the members of epoch N decide epoch N+1, so a
  reconfiguration is committed by the old members and the following epochs
  by the new ones. The seeds are the initial members.
- **Metadata log**: each commit records its epoch, kind (REGISTER,
  PREPARE_JOIN/FINISH_JOIN, PREPARE_LEAVE/FINISH_LEAVE, PREPARE_REMOVE,
  PREPARE_REPLACE/FINISH_REPLACE, RECONFIGURE_CMS, SCHEMA_CHANGE, ROLES)
  and what it did; the last 256 are in `system_views.cluster_metadata_log`,
  the node directory in `system_views.cluster_metadata_directory`.
- `GET /status` is nodetool status.

## Not done

replaying the metadata log (each epoch's value is a full snapshot); internode encryption and compression, EACH_QUORUM per
data center (it is approximated as QUORUM), digest reads, speculative
retry by latency.

## Tests

`internal/cluster` tests messaging, SWIM joins, state spread, failure
detection and restart with a new generation, refutation, the suspicion
timeout, concurrent CMS commits from three proposers (exactly one epoch
per change, the same snapshot everywhere), and replica placement. `internal/db/cluster_test.go` runs in-process
clusters: schema propagation, replication at RF 3, QUORUM with a node down
and ALL unavailable, metadata catch-up and hint replay after restart, read repair of a divergent
replica, counters and LWT from every coordinator, scattered index queries,
bootstrap streaming, repair of divergent replicas and decommission. The
gocql test runs three nodes on 127.0.0.1-3 and checks token-aware routing
to all of them. Three real processes were also checked with cqlsh: QUORUM
with one node killed, Unavailable at ALL, hint delivery on restart, and
decommission.
