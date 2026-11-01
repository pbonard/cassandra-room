# Storage engine design (`internal/storage/lsm`)

This document describes cassandra-room's own LSM storage engine. It replaces
the plan in MEMO.md to use Pebble as the storage engine.

The engine follows **Cassandra's storage architecture**: one commit log per
node, and a memtable, sstable set and compaction strategy per table. It
follows **Cassandra's semantics**: cells are reconciled by write timestamp,
not by arrival order. It stores data in **Pebble's sstable format**, reusing
Pebble's reader and writer. Compaction is Cassandra 5's Unified Compaction
Strategy (UCS) per table.

Status: M3 is done: Cassandra's architecture and timestamp semantics, a
merging iterator, UCS compaction with one shard per table, and a randomized
model test. Benchmarks against Pebble for each milestone are in section 10.
Sections marked *planned* describe later milestones.

## 1. Goals

- **Cassandra's storage architecture**, so per-table operations (flush,
  snapshot, drop, truncate, repair, streaming) touch only that table and
  tables do not interfere with each other's compaction.
- **Cassandra's semantics**: last-write-wins by cell timestamp, timestamped
  tombstones, TTLs, `gc_grace_seconds`.
- **UCS compaction per table**, as in Cassandra 5, plus a time-window mode
  for insert-only TTL tables (*planned*).
- **Partitions never split across sstables in a level or shard.** Shard
  boundaries are token boundaries, so this follows from the layout.
- **Performance at least equal to the Pebble fork** on the workloads in
  section 10. Anything reused from Pebble that measurably slows the engine
  is replaced.

Non-goals: byte compatibility with Cassandra's sstable or commit log files.
Migrating data from real Cassandra goes through CQL, or through a read-only
Cassandra sstable importer if that becomes necessary.

## 2. Architecture

| Part | Per node | Per table | Cassandra equivalent |
|---|---|---|---|
| Commit log (WAL) | ✓ segmented; a segment is deleted once every table that wrote to it has flushed past it | | `CommitLog` |
| Memtable memory budget | ✓ when mutable memtables exceed a fraction of the budget, the largest is flushed; writes stall when the budget is exhausted | | `memtable_heap_space`, `MemtableCleaner` |
| WAL space limit | ✓ when live segments exceed it, the tables pinning the oldest segment are flushed | | `commitlog_total_space`, `flushDataFrom` |
| Manifest | ✓ one log of edits for all tables | | per-table directories and transaction logs |
| Memtable, flush | | ✓ | `ColumnFamilyStore` memtables |
| sstables | | ✓ in `tables/<table ID>/` | `<keyspace>/<table>-<id>/` |
| Compaction strategy and settings | | ✓ | per-table compaction options |

Keys inside a table are `token | partition key | clustering key`, with no
table ID: the table is the directory.

Per-table operations:

- **Create table:** a manifest edit and a new directory.
- **Drop table:** a manifest edit, then the directory is deleted; WAL entries
  for the table are ignored on replay.
- **Flush table:** rotates only that table's memtable.

## 3. What is reused from Pebble, and what is not

Pebble is pinned at upstream commit a995e89. Only its public packages are
imported.

| Area | Choice | Why |
|---|---|---|
| sstable format, writer, reader, block iterators | **Reuse** `pebble/sstable` (columnar format, `TableFormatPebblev6`) | The most optimised part of Pebble. |
| Bloom filters | **Reuse** `pebble/sstable/tablefilters/bloom` | Same reason. |
| Block cache | **Reuse** Pebble's cache, one per engine (`CacheSize`, 64 MiB by default), keyed by file number | Sharded, CLOCK-Pro, tuned for these blocks. |
| Files | **Reuse** `pebble/vfs`, `pebble/objstorage/objstorageprovider` | `vfs.NewMem` and `vfs.NewCrashableMem` make tests fast and crash tests possible. |
| WAL writing and record framing | **Reuse** `pebble/record`: `LogWriter` for the WAL, `Writer` for the manifest | Checksummed, block-aligned records with torn-write detection; `LogWriter` buffers records, writes them from a background goroutine, and groups concurrent syncs. |
| Memtable | **Own**: skiplist, single writer per table, lock-free readers; keys and values in arena chunks | Pebble's `arenaskl` is internal. If ours stays slower (section 10), port it. |
| Commit log management, manifest, read path, compaction, merge function | **Own** | Must follow Cassandra's architecture and semantics. |

What Pebble's format holds for Cassandra:

- one key per row; the partition header (partition deletion, static row) is
  a key that sorts first in the partition;
- cells are encoded in the value against the table's schema, as Cassandra's
  `SerializationHeader` does (*planned*, data-model layer);
- the Bloom filter is built on the partition prefix through a `Comparer`
  whose `Split` returns its length (*planned*);
- `Statistics.db`-style metadata (min and max timestamps, max expiry time,
  tombstone histogram) is written by block property collectors (*planned*).

## 4. Cassandra semantics (M2)

Cassandra resolves conflicts by **write timestamp**; Pebble resolves them by
sequence number. These differ whenever writes arrive out of order, which
repair, hinted handoff and client-supplied timestamps cause routinely. So:

- **Every row write is a merge operand** (Pebble's `MERGE` kind), merged by
  the engine's `Merger`. The default is `internal/storage/row`, which
  implements Cassandra's rules, checked against the cassandra-5.0 source:
  - a row holds an optional row deletion (`markedForDeleteAt`,
    `localDeletionTime`) and cells sorted by column; each cell has a
    timestamp, a local deletion time and a TTL;
  - two cells of a column reconcile as `Cells.resolveRegular`: the higher
    timestamp wins; at equal timestamps a tombstone or expiring cell beats a
    live one, a tombstone beats an expiring one, the later local deletion
    time wins, then the lower TTL, then the larger value;
  - a row deletion shadows cells with timestamps at or below it, and the
    superseding deletion wins (`DeletionTime.supersedes`);
  - merging is commutative, associative and idempotent (tested on random
    rows), so fragments combine in any order.
  Collections (cells with paths), counters and range tombstones are not
  modelled yet.
- **Memtables keep every fragment**; a flush merges each key's fragments into
  one entry, and records the file's minimum and maximum timestamps (as
  Cassandra's `Statistics.db`) in the manifest.
- **Reads merge all fragments of a row** across memtables and sstables.
  sstables are read in order of maximum timestamp, highest first, and the
  read stops once a row deletion shadows the next sstable's newest data, as
  Cassandra's single-partition reads do. TTL expiry is decided by the reader
  (`row.Row.Live(now)`), not stored.
- **Compaction merges fragments with the same function** (M3).
- **Pebble's `DELETE`, `SINGLEDEL` and `RANGEDEL` are never used for user
  data**: they hide older entries by sequence number, which would let an
  older tombstone delete newer data.


## 5. Write path

1. `Engine.Apply(batch)` (or `Engine.Put` with a pooled batch) assigns the
   batch, whose entries may belong to several tables, a block of sequence
   numbers and hands it to the WAL as one record. The WAL is
   `record.LogWriter`: the record is copied into a buffer and written by a
   background goroutine.
2. Each entry is inserted into its table's mutable memtable, under the
   engine's commit lock. A batch becomes visible to readers atomically
   (readers ignore sequence numbers above the last fully applied batch).
3. Durability follows Cassandra's commit log modes:
   - **periodic** (default, `NoSync`): `Apply` returns once the batch is in
     memory; a background syncer fsyncs the WAL every `WALSyncPeriod`
     (10 s, Cassandra's `commitlog_sync_period`);
   - **group** (`Sync`): `Apply` waits for the fsync, outside the commit
     lock, so concurrent synced writes share one fsync. The batch may be
     visible before it is durable, as in Cassandra.
4. Before the write, the engine checks, without taking the engine mutex
   unless a limit is crossed:
   - **memory:** if mutable memtables use more than `MemTableCleanupThreshold`
     of `MemTableBudget`, the largest one is rotated and queued for flush; if
     all memtables, including ones waiting to flush, exceed the budget, the
     write waits;
   - **WAL:** the current segment is replaced when it exceeds
     `WALSegmentSize`; if live segments exceed `WALTotalSpace`, the tables
     whose memtables hold entries from the oldest segment are flushed.
5. Flushes run in the background, oldest first. A flush writes one sstable
   into the table's directory, records it and the table's flushed sequence
   number in the manifest, and deletes WAL segments no table still needs.

## 6. Manifest and recovery

The manifest is an append-only log of edits written with `pebble/record`. A
file named `CURRENT` names the live manifest and is replaced atomically. An
edit records:

- tables created and dropped;
- per table: sstables added (number, size, smallest and largest internal
  keys, sequence number range, minimum and maximum write timestamps) and
  deleted, and the **flushed sequence
  number**: every WAL entry of the table at or below it is in sstables;
- the next file number, the last sequence number, and the lowest WAL segment
  still needed.

On open, the engine replays the manifest, then replays WAL segments at or
above the lowest needed one. An entry is applied only if its table exists and
its sequence number is above the table's flushed sequence number. The
recovered memtables are flushed, a new manifest is written, and old segments
are deleted.

## 7. Read path

- **Point lookup (M2):** every memtable and every sstable whose key range and
  Bloom filter may contain the key contribute fragments, which are merged
  (section 4). sstables are visited by maximum timestamp and skipped once a
  row deletion shadows them. Blocks come from the block cache.
- **Iterators (M3):** `Table.NewIter(lower, upper)` merges a table's
  memtables and the sstables whose key ranges overlap the bounds, combining
  every source's fragment of a row with the `Merger`, so a scan returns the
  same rows as point lookups. An iterator holds a reference on the table's
  read state, so sstables replaced by a compaction stay open (and on disk)
  until the last iterator using them closes.
- **Snapshots (*planned*):** a sequence number plus a reference to a table's
  file set.

## 8. Compaction: UCS per table (M3; sharding M4)

The rules follow Cassandra 5's Unified Compaction Strategy, checked against
the cassandra-5.0 branch (commit 3e6d5ca):
`src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.md`
and `unified/Controller.java`.

- **Scaling parameter `W`**, per table. `W < 0` is leveled (fanout
  `F = 2 - W`, threshold `T = 2`), `W > 0` is tiered (`F = T = 2 + W`),
  `W = 0` gives `F = T = 2`.
- **Defaults** (`CassandraRelevantProperties` and `cassandra_latest.yaml`):
  `scaling_parameters` T4 (tiered, `W = 2`), `target_sstable_size` 1 GiB,
  `base_shard_count` 4, `min_sstable_size` 100 MiB, `sstable_growth` 0.333,
  `max_sstables_to_compact` 64. Cassandra 5.0's shipped `cassandra.yaml`
  still defaults tables to STCS; UCS is the default only in
  `cassandra_latest.yaml`. This engine defaults every table to UCS T4,
  which behaves like STCS.
- **Density and levels:** an sstable's density is its size divided by the
  fraction of the token range it covers; level `n` starts at density
  `m·F^n`, where `m` is the flush size.
- **Trigger:** within a level, sstables are grouped into overlapping sets; a
  set with `T` or more sstables is compacted.
- **Sharding** (*planned*, M4): output is cut at token boundaries, so a
  partition is never split, and compactions in different shards run in
  parallel. In M3 every table has one shard, so density is file size and a
  table runs at most one compaction at a time (`MaxConcurrentCompactions`
  bounds compactions across tables).
- **Installing a compaction (M3):** the output is written and synced, then
  one manifest edit adds it and deletes the inputs. Readers switch to the new
  read state; each input file is closed and deleted when its last reader
  releases it. After a crash, recovery deletes sstables the manifest does
  not reference, so a compaction is either fully installed or invisible.
- **Fully expired sstables** (*planned*) are dropped without rewriting, checked every 10
  minutes (`CompactionController.getFullyExpiredSSTables`, used by every
  Cassandra strategy).
- **Time-window mode** (*planned*): sstables grouped by maximum write
  timestamp, for insert-only TTL tables, so whole files expire together.

## 9. Purging (*planned*, M5)

- **Purge rule (Cassandra's):** a tombstone older than `gc_grace_seconds` is
  dropped if no sstable outside the compaction overlaps its key with older
  data (checked with Bloom filters and per-file timestamp ranges) and no
  memtable holds the key.
- **Due sstables:** the Pebble fork's per-file summary and `IsFileDue` carry
  over, so files whose tombstones become due (by time or by repair
  watermark) are compacted even when UCS would not pick them.

## 10. Testing and benchmarks

- Unit tests per component, with `vfs.NewMem`.
- Crash tests with `vfs.NewCrashableMem`: crash at random points, dropping
  unsynced data, and check that every synced write survives and nothing else
  appears.
- A randomized model test (M3, `TestModel`): random row fragments with
  out-of-order timestamps, cell tombstones and row deletions against an
  in-memory model, with random flushes, compactions (every scaling
  parameter class), reopens and crashes; every scan and point lookup must
  equal the model byte for byte, and no synced write may be lost.
- Benchmarks against Pebble on the same workload, re-run after each
  milestone.

**Method.** `internal/storage/lsm/bench_test.go` runs the same workload on
the engine and on Pebble at a995e89: keys `key%012d`, values one encoded row
with one 100-byte cell, an in-memory file system, Apple M-series with 8
threads. Pebble gets the same block cache size (64 MiB) and 10-bit Bloom
filters, and for reads both stores are compacted to one file per key (the
engine has no compaction yet; Pebble is compacted manually). Each figure is
the median of 8 runs, because single runs vary by up to 20%.

| Benchmark | M1 | **M2** | Pebble | Notes |
|---|---|---|---|---|
| `Put`, one writer, periodic sync | 610–700 ns | **452 ns** | 552 ns | |
| `Put`, 8 writers, periodic sync | – | **549 ns** | 590 ns | |
| `Put`, 8 writers, synced | – | 1003 ns | **887 ns** | Cassandra's default is periodic; see below |
| `Get` from sstables | 4.4–4.7 µs | **804 ns** | 1388 ns | |

What changed in M2, from CPU and allocation profiles:

- **Writes:** the WAL moved to `record.LogWriter` (M1 issued one file write
  per record, 61% of allocated bytes); batches are pooled (`Engine.Put`);
  memtable nodes and towers come from slabs; the commit path checks limits
  with atomics and takes the engine mutex only when a limit is crossed (it
  was contended by every writer).
- **Reads:** Pebble's block cache (M1 re-read, checksummed and decompressed
  the index and data blocks on every lookup).
- **Allocated bytes per write** stay higher than Pebble's (about 1.2 KB
  against 180 B). Most of it is the in-memory file system growing the WAL
  file's buffer, which Pebble avoids by recycling WAL files; on a real disk
  file writes do not allocate. The rest is the memtable arena, which holds
  the data. Recycling WAL segments, as Cassandra and Pebble do, is a later
  improvement.
- **Synced writes are 13% slower than Pebble's.** Pebble inserts into the
  memtable outside its commit lock and publishes batches in order; the
  engine inserts under the lock. That design was implemented here
  (compare-and-swap skiplist insertion plus an ordered publish queue) and
  measured slower on every write benchmark, so it was reverted; the
  lock-free insertion is kept and tested for later use. Cassandra's default
  commit log mode is periodic, where the engine is faster than Pebble.

### Results from M3 on (4-core Xeon, Linux)

The figures above came from an Apple M-series laptop. From M3 on, the
benchmarks run on a 4-vCPU Intel Xeon (2.1 GHz) Linux VM, so they are not
comparable with the table above. Each figure is the median of 6 runs
(`benchstat`); this machine is noisier, with spreads of 5–20%.
`WriteSustained` runs with `-benchtime=1000000x`. The engine's read and
write benchmarks run with `DisableCompactions`, like Pebble's.

| Benchmark | M3 | Pebble | Notes |
|---|---|---|---|
| `Put`, one writer, periodic sync | 1149 ns | 1272 ns | |
| `Put`, 4 writers, periodic sync | 1232 ns | 1288 ns | |
| `Put`, 4 writers, synced | 3135 ns | **2954 ns** | |
| `Get` from sstables | **1708 ns** | 2525 ns | |
| `Scan100` (seek, then 100 rows) | **8.9 µs** | 10.6 µs | |
| `WriteSustained` (with compactions) | 2289 ns | **1659 ns** | |
| `WriteSustained` write amplification | **2.50** | 2.93 | UCS T4 against Pebble's leveled |

## 11. Milestones

| Milestone | Contents |
|---|---|
| **M1** (done) | WAL, memtable, flush to sstables, manifest, recovery, point lookups, for one key space. |
| **M1.5** (done) | Cassandra's architecture: one WAL with segments and a memory budget, per-table memtables, sstables and directories, create and drop table. |
| **M2** (done) | Cassandra's timestamp semantics (row codec, merge on read and flush, timestamp-ordered reads); periodic and group commit log sync; the speed fixes in section 10. |
| **M3** (done) | Merging iterator, UCS for one shard per table, model test, reference-counted read states. |
| M4 | UCS sharding, parallel compactions, time-window mode. |
| M5 | Purge rule, collector summaries, due-file picking. |
| M6 | Snapshots, streaming and repair support, benchmarks against the Pebble fork. |
