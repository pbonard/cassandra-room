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

- **Messaging** (`MessagingService`): framed request/response over one TCP
  connection per peer.
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
- **Reads** ask blockFor replicas (this node first), retry another replica
  on failure, and merge the raw row fragments they return; row fragments
  merge by timestamp, so the merge is the reconciliation. Replicas that
  returned less get the merged row (blocking read repair). Range scans read
  one ring range at a time, in pages, cutting each page at the first
  replica page that was truncated.
- **Schema** and role changes are metadata commits (above), from any node.
- **Counters** run on the partition's first live replica, where counter
  shards are read locally.
- **Lightweight transactions** run single-decree Paxos per partition among
  its replicas (Cassandra's Paxos v1, as ScyllaDB's `service/paxos`):
  prepare (finishing an accepted but uncommitted round, repairing replicas
  that missed the last commit), a QUORUM read and the condition check,
  propose (the update timestamped with the ballot), commit at the
  statement's consistency level, and prune once every replica learnt it.
  Each replica's promise, accepted proposal and last commit are cells of a
  `system.paxos`-like table, synced before answering. SERIAL reads finish
  rounds in progress first. Any node can coordinate.
- **Secondary index** queries are gathered from every live node, each
  answering for the partitions it is the first live replica of.
- Drivers see the cluster through `system.peers`/`peers_v2` and route
  token-aware.

## Topology (admin endpoint)

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
