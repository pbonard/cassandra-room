# Clusters

Nodes form a cluster as Cassandra nodes do, with the same consistency
levels and the same failure behaviour seen from drivers. Everything is in
`internal/cluster` (transport, membership, ring) and `internal/db/proxy.go`,
`topology.go` (coordination), using only the Go standard library.

```
cassandra-room -addr 10.0.0.1:9042 -listen-address 10.0.0.1:7000 -seeds 10.0.0.1:7000 -admin-address 10.0.0.1:7199
```

## Membership and ring

- **Messaging** (`MessagingService`): framed request/response over one TCP
  connection per peer.
- **Gossip** (`Gossiper`): every second a node sends its view of all
  nodes' states (host id, tokens, status, data center, rack, native
  address, schema version and epoch, generation and heartbeat version) to a
  random live node, sometimes a seed or a down node; newer states win. A
  node whose heartbeat stops for 5 s is down (a timeout, not phi accrual).
- **Ring** (`TokenMetadata`): each node has `-num-tokens` random vnode
  tokens. SimpleStrategy and NetworkTopologyStrategy (rack-aware) place
  replicas.

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
- **Schema** changes and role changes run on the schema leader (the live
  node with the smallest host id), which pushes the schema to every node;
  nodes gossip a schema epoch and pull newer schemas.
- **Counters and LWT** run on the partition's first live replica: counter
  shards are read locally there; conditions are read at QUORUM under the
  partition's lock. This is not Paxos: conditional updates are
  linearizable while that replica stays the leader.
- **Secondary index** queries are gathered from every live node, each
  answering for the partitions it is the first live replica of.
- Drivers see the cluster through `system.peers`/`peers_v2` and route
  token-aware.

## Topology (admin endpoint)

- **Bootstrap**: a new node (with seeds, not itself a seed) joins as
  JOINING, streams the ranges it will own from current replicas, rebuilds
  its local indexes, then becomes NORMAL.
- **Decommission** (`POST /decommission`): refused if fewer nodes than a
  replication factor would remain; otherwise the node goes LEAVING, streams
  its ranges to their new replicas, replays its hints, and leaves.
- **Repair** (`POST /repair?keyspace=`): for each range this node
  replicates, each replica builds a Merkle tree (the engine's, over purged
  rows); differing leaves are streamed both ways.
- `GET /status` is nodetool status.

## Not done

Paxos, cleanup of ranges a node no longer owns, `removenode` and
`replace_address`, internode encryption and compression, EACH_QUORUM per
data center (it is approximated as QUORUM), digest reads, speculative
retry by latency.

## Tests

`internal/cluster` tests messaging, gossip convergence and down detection,
and replica placement. `internal/db/cluster_test.go` runs in-process
clusters: schema propagation, replication at RF 3, QUORUM with a node down
and ALL unavailable, hint replay after restart, read repair of a divergent
replica, counters and LWT from every coordinator, scattered index queries,
bootstrap streaming, repair of divergent replicas and decommission. The
gocql test runs three nodes on 127.0.0.1-3 and checks token-aware routing
to all of them. Three real processes were also checked with cqlsh: QUORUM
with one node killed, Unavailable at ALL, hint delivery on restart, and
decommission.
