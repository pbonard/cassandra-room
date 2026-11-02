# Execution layer

The execution layer sits between the CQL native protocol server
(`internal/server`) and the storage engine (`internal/storage/lsm`). It is
a single-node port of Cassandra's `cql3` statements, `schema` and the
read/write paths of `db`, with the Java classes as the reference for
behaviour and error messages.

```
internal/cql     lexer + recursive-descent parser -> raw statements (Parser.g, CQLStatement.Raw)
internal/types   CQL types: literals, validation, byte-comparable keys (db/marshal, utils/bytecomparable)
internal/db      schema, prepare, execute: DDL, INSERT/UPDATE/DELETE/BATCH, SELECT, LWT, DESCRIBE,
                 virtual system tables (cql3/statements, schema, SystemKeyspace, SchemaKeyspace)
internal/server  native protocol v3-v5: framing, compression, admission control, prepared cache
```

The server calls `db.Prepare` and `db.Execute` directly. The earlier
`QueryHandler`/`Request` callback, the server's placeholder results, the
generated ANTLR parser (41k lines) and its separate AST-building pass are
gone: the hand-written parser produces the raw statements the execution
layer prepares, and is about 20x faster (3 µs for a typical SELECT, 62 µs
with ANTLR).

## Statements

A query is parsed, then *prepared* against the current schema
(`Prepared`): column names are resolved, terms are converted to their
receivers' types (`Term.Raw.prepare`), bind markers get their specifications
(name and type, returned in the PREPARED result), and the WHERE clause is
analysed (`StatementRestrictions`). Execution binds the request's values.
A prepared statement records the schema version; after a schema change it
is re-prepared transparently on its next execution, so its id stays valid.
Unprepared QUERY messages go through the same path and the same cache.

| CQL | Java reference | Notes |
|---|---|---|
| USE | `UseStatement` | |
| CREATE/ALTER/DROP KEYSPACE | `CreateKeyspaceStatement`, ... | SimpleStrategy and NetworkTopologyStrategy accepted; replication is not applied (one node) |
| CREATE TABLE | `CreateTableStatement.builder` | same validation and messages; regular columns ordered by name |
| ALTER TABLE ADD/DROP/RENAME/WITH | `AlterTableStatement` | a dropped column's cells are ignored; re-adding gives a new column |
| DROP TABLE, TRUNCATE | `DropTableStatement`, `TruncateStatement` | TRUNCATE switches the table to new, empty storage |
| INSERT, UPDATE, DELETE | `ModificationStatement` | USING TTL/TIMESTAMP, default_time_to_live, IN on keys, static columns, row/range/partition deletions |
| BATCH (CQL and protocol) | `BatchStatement` | applied atomically in one engine batch with one timestamp |
| IF NOT EXISTS / IF EXISTS / IF conditions | `CQL3CasRequest`, `StorageProxy.cas` | single partition; linearizable among conditional statements on one node (striped lock) |
| SELECT | `SelectStatement` | see below |
| DESCRIBE | `DescribeStatement` | KEYSPACES, TABLES, KEYSPACE, TABLE, SCHEMA, CLUSTER (cqlsh 6 uses it) |

SELECT supports partition key EQ/IN (all components), `token()` ranges,
full scans, clustering prefixes of EQ/IN followed by a slice (single- or
multi-column, including multi-column slices over columns of mixed order),
ORDER BY (reversing the clustering), LIMIT, PER PARTITION LIMIT, DISTINCT,
ALLOW FILTERING on any column (including CONTAINS on frozen collections),
`writetime`, `ttl`, `token`, `cast`, aliases, and the aggregates `count`,
`min`, `max`, `sum`, `avg`. A partition with static data and no rows
returns one row of its static columns, as in Cassandra. Paging uses an
opaque state holding the last row's storage key and the remaining LIMIT and
PER PARTITION LIMIT counts.

Errors are the protocol's: `Invalid`, `SyntaxError`, `ConfigError`,
`AlreadyExists`, `Unauthorized` (system keyspaces), with Cassandra's
messages (for example the ALLOW FILTERING message).

## Data layout

Each table has its own storage table in the engine. Rows are stored in
token order, as with Murmur3Partitioner:

```
partition prefix P = token (8 bytes, sign bit flipped) | escaped partition key
table without clustering columns:  row       = P
table with clustering columns:     header    = P | 0x10      static row and partition deletion
                                   tombstone = P | 0x18 | ...  range deletion (bounds in cells)
                                   row       = P | clustering components | 0x38
```

- The token is Cassandra's `MurmurHash.hash3_x64_128` (with its
  sign-extension quirk), checked against the Java code's output.
- The partition key is its serialized form (CompositeType for several
  columns), escaped as a byte-comparable blob, so equal tokens sort by key
  bytes as `DecoratedKey` does.
- Clustering keys use the byte-comparable encoding of
  `ByteComparable.md`: separators 0x40 (value), 0x3E (null), 0x3F/0x41
  (empty), terminator 0x38, bound terminators 0x20/0x60; DESC columns are
  bit-inverted. Slices are therefore plain key ranges. bigint, timestamp
  and time use the fixed-length signed form; varint and decimal use simple
  order-preserving forms of our own (decimals that compare equal, such as
  1.0 and 1.00, are the same clustering, as in Cassandra; reads return the
  canonical form).
- A row value (the `row` package) holds cells by column id. Column 0 is the
  primary key liveness (the row marker INSERT writes), so a row inserted
  with null columns exists and a row created by UPDATE disappears when its
  cells are deleted.
- Row deletions are row-level deletions, purged by the engine's normal
  rules. Partition and range deletions shadow rows under other keys, so they
  are stored as live cells (timestamp, local deletion time as value) that
  the engine never purges: purging them by gc_grace_seconds could resurrect
  older rows in sstables not part of the compaction. They cost space until
  the partition is rewritten; compaction-aware purging of them is future
  work.

## Schema and system tables

The schema is kept as an immutable snapshot swapped on each DDL statement,
and persisted in `schema.json` (written to a temporary file, synced,
renamed). Storage tables are created before the schema is saved and dropped
after, and at startup storage missing from the schema is dropped, so a
crash at any point leaves a consistent state. The schema version (in
`system.local`) is a digest of the definitions.

`system.local`, `system.peers`, `system.peers_v2` and `system_schema.*`
(keyspaces, tables, columns, and empty dropped_columns, triggers, views,
types, functions, aggregates, indexes) are virtual tables generated on each
read and served through the same SELECT path, so drivers' metadata queries
work unchanged. The node reports release version 5.0.4 and owns the whole
ring.

## Configuration

`cmd/cassandra-room`: `-addr`, `-data-dir`, `-commitlog-sync` (`periodic`,
the default, fsyncs every `-commitlog-sync-period`, 10 s; `batch` fsyncs
before acknowledging each write), `-memtable-space`, `-cluster-name`.

Table compaction options map to the engine: UnifiedCompactionStrategy
options one to one (scaling_parameters, base_shard_count,
target_sstable_size, min_sstable_size, sstable_growth, ...);
TimeWindowCompactionStrategy to the time-window mode (window unit and size,
unsafe_aggressive_sstable_expiration); SizeTiered and Leveled run as UCS with
T4 and L10. gc_grace_seconds sets the purge horizon. Options changed by
ALTER TABLE take effect when the server restarts.

## Also supported

- **Multi-cell columns** (row format v2): non-frozen `list`, `set`, `map`
  and user types store one cell per element, keyed by a path (the
  byte-comparable element or key, a time-ordered id for lists, the field
  index for UDTs). Overwriting a collection writes a complex deletion at
  ts-1 first, as Cassandra's setters do. All collection operations work:
  `+`/`-`, prepend, `m[k] = v`, `l[i] = v`, `u.f = v`, element deletes; list
  index operations read before writing.
- **User-defined types**: CREATE/ALTER/DROP TYPE, frozen and non-frozen,
  `{field: value}` literals, `system_schema.types`, DESCRIBE.
- **Counters**: counter tables with Cassandra's validation; a counter cell
  holds a context of per-node shards (id, clock, count) merged by clock,
  updated by read-modify-write of the node's own shard. Deleting a counter
  is final, as Cassandra documents (this also keeps merging associative).
- **JSON**: SELECT JSON, INSERT JSON (DEFAULT NULL/UNSET), toJson(),
  fromJson().
- **GROUP BY** on a primary key prefix, **arithmetic** in selections,
  **conditional batches** (one partition).
- **Secondary indexes** (legacy and `StorageAttachedIndex`): an index is a
  storage table keyed by (value, partition, clustering), written in the
  row's batch; reads verify entries against the base row and delete stale
  ones. SAI indexes also serve ranges.
- **Authentication and authorization**: PasswordAuthenticator (SASL PLAIN,
  bcrypt), roles, GRANT/REVOKE on keyspaces/tables/roles, LIST
  ROLES/USERS/PERMISSIONS.
- **Clusters**: see `docs/cluster.md`.

## Purging deletions

Partition deletions and range tombstones are rows of their own (the
partition header and tombstone rows). Compaction runs a per-partition
filter (`purge.go`, an `lsm.RowFilter`): it drops the rows, static cells
and range tombstones a deletion shadows, and drops the deletion itself
once it is older than `gc_grace_seconds` and no older data of the
partition (or of the range) remains outside the compaction, in other
sstables or the memtables.

## Vectors

`vector<float, n>` (any element type; float or double for search) is
sent to drivers as Cassandra's custom `VectorType(... , n)`. The
`similarity_cosine`, `similarity_euclidean` and `similarity_dot_product`
functions score two vectors; `ORDER BY v ANN OF [..] LIMIT k` on a column
with an SAI index (`similarity_function` option: cosine by default,
euclidean, dot_product) returns the k most similar rows among those the
rest of the query selects. Scoring is exact (every candidate row is
scored), not an approximate graph search.

## Not supported yet

- user-defined functions and aggregates, materialized views,
  triggers, LIKE, data masking



## Tests

- `internal/types`: byte-comparable order and round trips for every key
  type against independent comparators, reversed and inside sequences;
  literal parsing.
- `internal/cql`: parsing of every supported statement, AST checks, syntax
  errors.
- `internal/db`: CRUD and row liveness, clustering (static columns, range,
  row and partition deletions, ORDER BY, IN, multi-column slices), paging
  through every query shape against the unpaged result, TTL and timestamps,
  schema persistence across restarts and the system tables, LWT, DESCRIBE,
  Murmur3 against Cassandra's Java implementation.
- `internal/server`: protocol tests on a real (in-memory) database, and
  `TestGocql`, which runs the gocql driver over TCP with protocols v3 and
  v4: every common type, paging, batches, LWT, TTL, error codes and schema
  metadata. cqlsh 6.2 (Python driver, protocol v5) was also checked by hand:
  queries, DESCRIBE and `system.local` display correctly.

`BenchmarkExecute` (in-process, no network, one core): a 6-column insert
takes about 8.6 µs and a point SELECT about 17 µs, most of it in the
memtable (the benchmark rewrites the same 100k keys, so reads merge several
fragments).
