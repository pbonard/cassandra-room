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
- **Counters and LWT** run on the partition's first live replica: counter
  shards are read locally there; conditions are read at QUORUM under the
  partition's lock. This is not Paxos: conditional updates are
  linearizable while that replica stays the leader.
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
- `GET /status` is nodetool status.

## Not done

Paxos for LWT; the CMS acceptors' promises live in memory (a CMS member
that restarts in the middle of a commit could accept an older ballot;
TCM keeps them in a system table), CMS membership changes (it is fixed to
the seeds), the metadata log itself (only the latest snapshot is kept);
cleanup of ranges a node no longer owns, `removenode` and
`replace_address`, internode encryption and compression, EACH_QUORUM per
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
