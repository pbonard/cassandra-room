# Clusters

Nodes form a cluster as Cassandra nodes do, with the same consistency
levels and the same failure behaviour seen from drivers. Everything is in
`internal/cluster` (transport, membership, ring) and `internal/db/proxy.go`,
`topology.go` (coordination), using only the Go standard library (no memberlist or Raft dependency).

```
cassandra-room -addr 10.0.0.1:9042 -listen-address 10.0.0.1:7000 -seeds 10.0.0.1:7000 -admin-address 10.0.0.1:7199
```

## Addresses

As Cassandra's, each node has an address it listens on and an address it is
known by, for other nodes and for clients:

| | Listens on | Known by |
|---|---|---|
| Internode | `-listen-address` (`listen_address`) | `-broadcast-address` (`broadcast_address`), default the listen address |
| Clients | `-addr` (`rpc_address`) | `-broadcast-rpc-address` (`broadcast_rpc_address`), default the `-addr` host |

The broadcast address is the node's identity: other nodes connect to it, the
cluster metadata and seeds name it, and `system.peers` lists it. It is for
nodes that cannot be reached at the address they listen on: behind NAT, in
another network or Kubernetes cluster (`-listen-address 0.0.0.0:7000` needs
one). The client broadcast address is what drivers find in `system.local`
and `system.peers` and connect to (`-addr :9042` in a cluster needs one).
Host names are resolved at startup.

With `-prefer-local` (`prefer_local`), nodes of the same data center connect
to each other at their listen addresses, recorded in the cluster metadata,
and use broadcast addresses only across data centers; `system.peers` shows
the listen address as `preferred_ip`. With internode TLS and
`-internode-verify-hostname`, a certificate is checked against the address
connected to, so it names both.

## Client encryption

`-client-cert` and `-client-key` encrypt the native port
(`client_encryption_options`); with `-client-ca`, clients must also present
a certificate signed by that CA (`require_client_auth`), and with
`-client-tls-optional` the port also accepts unencrypted connections
(`optional: true`, told apart by the first byte of a TLS handshake). The
handshake must complete within 10 seconds.

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
  native address, DC, rack, tokens, status) form the metadata, with an
  epoch. Every change (DDL, role changes, a node registering, joining or
  leaving) is a transformation: the proposer plans it on a copy of the
  current metadata and the **Cluster Metadata Service** chooses the entry
  of the next epoch by single-decree Paxos among its members (the seeds at
  first). The acceptors of epoch N+1 are the members recorded at epoch N,
  read together with it. Concurrent changes are serialized: a proposer
  whose slot was taken catches up and plans again. Each change carries an
  entry id, so a change chosen through another proposer is not applied
  twice.
- **Metadata log** (`internal/db/metalog.go`, TCM's LocalLog): an epoch's
  entry holds only the change: the scalars, the keyspaces, tables, roles
  and nodes it added, changed or removed, and a hash of the resulting
  metadata. Committed entries are pushed to the live nodes, which apply
  them in epoch order; a node that misses some (a gap, or a higher epoch
  seen in gossip) fetches the entries after its epoch from a peer and
  replays them, or a snapshot when the peer's log (the last 256 entries)
  no longer reaches back. A hash mismatch also falls back to a snapshot.
  Each node logs an entry durably (`metadata-log.json`) before installing
  it and replays logged entries at startup, so a crash in between loses
  nothing. Installing creates storage for new tables and drops storage of
  removed ones.
- **Ring** (`ring.go`): built from the metadata's node directory, never
  from gossip, so all nodes at the same epoch agree on the replicas. Each
  node has `-num-tokens` random vnode tokens. SimpleStrategy and
  NetworkTopologyStrategy (rack-aware) place replicas.

## Coordination (StorageProxy)

Statements run through a storage interface: the local engine on a single
node, the coordinator in a cluster.

- **Writes** go to every replica of each partition, in every data center;
  the coordinator waits for the consistency level's blockFor acks per
  partition (for `LOCAL_ONE`, `LOCAL_QUORUM` and `LOCAL_SERIAL`, acks from
  this data center only: the others still get the write).
- **Forwarding** (`FORWARD_TO`, `sendMessagesToNonlocalDC`): replicas in
  this data center are sent the write directly. In each other data center,
  replicas receiving the same writes are sent one message, to a random one
  of them, which delivers it to the others over its own connections; each
  answers the coordinator directly (`RESPOND_TO`). So a write crosses to
  each data center once instead of once per replica. A replica the
  forwarding node cannot reach is reported to the coordinator as failed
  (and hinted); if the forwarding node itself cannot be sent the message,
  the others are sent it directly. Forwarding needs messaging version 5,
  so a node still on version 4 is always sent its writes directly. A replica
  that does not answer within the write timeout is hinted, as one that
  fails. Too few live
  replicas: `Unavailable`; no answer in time: `WriteTimeout`. Down or
  failing replicas get **hints**, stored locally and replayed when they come
  back (and every 5 s), as Cassandra's hinted handoff: no hints for a node
  down longer than `max_hint_window` (3 h); a hint expires after the
  smallest `gc_grace_seconds` of its tables (none for tables with 0);
  replay is throttled to `hinted_handoff_throttle` (1 MiB/s) shared by the
  nodes being replayed to; hints for nodes that left are dropped. Nodes joining or taking over a range also receive
  its writes (pending ranges).
- **Reads** of a row ask one replica (this node first) for the data and
  the others blockFor needs for a digest; matching digests answer at once,
  a mismatch reads full rows from everyone who answered and merges them
  (row fragments merge by timestamp, so the merge is the reconciliation).
  A replica slower than the table's `speculative_retry` (a percentile of
  that table's recent replica reads, `99p` by default; a fixed `Nms`;
  `ALWAYS`; `NONE`; or `MIN`/`MAX` of two), or failing, gets another
  replica asked in its place. Range reads read full rows from the
  replicas (no digests, as in Cassandra), retry a failed replica with one
  of the same data center for EACH_QUORUM, and repair the replicas that
  returned less. Replicas that
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
- **Lightweight transactions** are Accord transactions on every table
  (Paxos was removed; see `docs/accord.md`). Tables whose other writes
  bypass Accord (`transactional_mode` `off`, the default) have their
  conditions read at QUORUM by the executing replica (interop reads). SERIAL
  reads first run an Accord read-only transaction, then read at QUORUM.
- Drivers see the cluster through `system.peers`/`peers_v2` and route
  token-aware.

## Management API

`-admin-address` serves the management API (`internal/manage`), what an
operator needs of a node (Cassandra's Management API and nodetool), as JSON
over HTTP, optionally with TLS (`-admin-cert`, `-admin-key`) and required
client certificates (`-admin-ca`):

| | |
|---|---|
| `GET /api/v1/health`, `/api/v1/ready` | liveness; readiness (in the ring and serving clients) |
| `GET /api/v1/node`, `/api/v1/nodes` | this node; every node (address, host id, data center, rack, status, up, tokens, metadata service membership) |
| `GET /api/v1/ring?keyspace=` | ranges and their replicas (`describering`) |
| `GET /api/v1/keyspaces` | the user keyspaces |
| `POST /api/v1/flush` | flush every memtable |
| `POST /api/v1/decommission` | leave the ring, in the background (`/node` shows LEAVING, then LEFT) |
| `POST /api/v1/cleanup?keyspace=`, `/removenode?node=`, `/cms?members=` | as nodetool |
| `POST /api/v1/repair` | `{"keyspace","table","start","end"}`: a token range (start, end], wrapping allowed, or the whole keyspace |
| `POST /api/v1/snapshots`, `DELETE /api/v1/snapshots/{name}` | snapshot user keyspaces (returns each table's files), clear |
| `POST /api/v1/import` | `{"keyspace","table","files"}`: load sstables into a table on this node |
| `POST /api/v1/load` | `{"keyspace","table","files","consistency"}`: write sstables' rows through the coordinator to their replicas in this cluster, whatever its topology (sstableloader); `ALL` by default |
| `POST /api/v1/commitlog/archive` | close the commit log's segment, so every acknowledged write is archived; `{"files"}`: the archived segments (needs `-commitlog-archive-dir`) |
| `POST /api/v1/commitlog/replay` | `{"files","tables","until"}`: replay archived segments on this node, leaving out what was written after `until` (microseconds); `tables` maps the storage ids a snapshot reported to `keyspace.table` |
| `GET /metrics` | Prometheus metrics: client requests' latency (by kind and consistency level) and errors (timeouts, unavailables...), each table's size, sstables and memtables, flushes and compactions, hints, nodes up and down by data center, dropped and forwarded messages, thread pools, Accord's paths, recoveries and queues, native connections, the Go runtime and process |

A restore is a truncate (new, empty storage on every node) followed by each
node importing its own snapshot files. Into a cluster of another topology
(other nodes, tokens or replication), each table's files from every node of
the backup are loaded instead (`/api/v1/load`): the coordinator sends each
partition to its replicas here, keeping timestamps and deletions.

Point in time: with `-commitlog-archive-dir`, the engine hard-links every
commit log segment it moves on from into that directory (segments are then
not reused). A restore to time T imports the last backup before T, then
replays each node's segments since, leaving out cells and deletions whose
timestamps are after T (Cassandra's `restore_point_in_time`). Writes are
matched to tables by storage id, so writes from before a `TRUNCATE` never
come back. `-flags-file` reads further flags
from a file, one per line (the command line overrides them), for an init
container to set per-pod flags.

## Topology (admin endpoint)

- **Streaming** (`internal/db/stream.go`) moves ranges as sstable files,
  as Cassandra's entire-sstable streaming does: the source exports the
  range (sstables wholly inside it hard-linked, the sections of the others
  written out), the receiver pulls the files in 4 MiB chunks over the
  large-message connection, imports them (hard-linked into the table, not
  rewritten). A node
  that must push a range asks the receiver to pull it. Bootstrap,
  decommission and removenode record every range they finish
  (`stream-progress.json`), so an interrupted operation run again resumes
  where it stopped. Not done: sendfile (chunks go through messages).
- **Bootstrap**: a new node catches up with the metadata, then commits its
  registration as JOINING (TCM's PrepareJoin: from that epoch writes also
  go to it), streams the ranges it will own from current replicas, and
  commits NORMAL (FinishJoin). The first
  node of a cluster registers as NORMAL directly.
- **Decommission** (`POST /api/v1/decommission`): refused for CMS members, and if
  fewer nodes than a replication factor would remain; otherwise the node
  commits LEAVING (PrepareLeave), streams its ranges to their new replicas,
  replays its hints, and commits LEFT (FinishLeave).
- **Repair** (`POST /api/v1/repair`): for each range this node
  replicates, each replica builds a Merkle tree (the engine's, over purged
  rows); differing leaves are streamed both ways.
- **Cleanup** (`POST /api/v1/cleanup?keyspace=`): rewrites the tables without the rows of ranges this node no longer replicates, as
  nodetool cleanup does after nodes join; refused while nodes are joining
  or leaving.
- **Removenode** (`POST /api/v1/removenode?node=`, an address or host id): a dead
  node is committed LEAVING; for each of its ranges, the nodes that become
  replicas get the data from a surviving replica (the coordinator asks the
  source to push it); then it is LEFT.
- **Replace** (`-replace-address` on a new node's first boot): the new
  node takes the dead node's tokens as JOINING with `Replaces` set (writes
  of those ranges reach it as a pending replica, the dead node counts as
  down even if the address is reused), streams them from the other
  replicas, then one commit makes it NORMAL and the dead node LEFT.
- **Metadata service membership** (`POST /api/v1/cms?members=`) is
  part of the metadata: the members of epoch N decide epoch N+1. As TCM's
  Prepare/AdvanceCMSReconfiguration, a reconfiguration moves one member
  per epoch, additions first; a node is brought up to date with the log
  (`VerbMetadataSync`) before it is added. A step checks the members it
  starts from, so concurrent reconfigurations fail instead of mixing; an
  interrupted one is continued by running it again. The seeds are the
  initial members.
- **Metadata history**: each commit records its epoch, kind (REGISTER,
  PREPARE_JOIN/FINISH_JOIN, PREPARE_LEAVE/FINISH_LEAVE, PREPARE_REMOVE,
  PREPARE_REPLACE/FINISH_REPLACE, RECONFIGURE_CMS, SCHEMA_CHANGE, ROLES)
  and what it did; the last 256 are in `system_views.cluster_metadata_log`,
  the node directory in `system_views.cluster_metadata_directory`.
- `GET /api/v1/nodes` is nodetool status.

## Not done

- Metadata: applying an entry still re-encodes the whole metadata to check
  its hash; the target of an interrupted reconfiguration is not recorded
  in the metadata (TCM records it so any node can finish it).
- Repair is full repair only (no incremental repair); streaming sends
  files through messages (no sendfile).

## Tests

`internal/cluster` tests messaging, SWIM joins, state spread, failure
detection and restart with a new generation, refutation, the suspicion
timeout, concurrent CMS commits from three proposers (exactly one epoch
per change, the same metadata everywhere), commits while the members
change one at a time, catching up by replaying a peer's log or from a
snapshot, and replica placement. `internal/db/metalog_test.go` checks that
entries hold only the changed elements and rebuild the same metadata,
divergence detection, and replay of logged entries at startup. `internal/db/cluster_test.go` runs in-process
clusters: schema propagation, replication at RF 3, QUORUM with a node down
and ALL unavailable, metadata catch-up and hint replay after restart, read repair of a divergent
replica, counters and LWT from every coordinator,
bootstrap streaming, repair of divergent replicas and decommission. The
gocql test runs three nodes on 127.0.0.1-3 and checks token-aware routing
to all of them. Three real processes were also checked with cqlsh: QUORUM
with one node killed, Unavailable at ALL, hint delivery on restart, and
decommission.
