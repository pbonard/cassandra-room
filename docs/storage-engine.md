# Storage engine design (`internal/storage/lsm`)

This document describes cassandra-room's own LSM storage engine. It replaces
the plan in MEMO.md to use Pebble as the storage engine.

The engine follows **Cassandra's storage architecture**: one commit log per
node, and a memtable, sstable set and compaction strategy per table. It
follows **Cassandra's semantics**: cells are reconciled by write timestamp,
not by arrival order. It stores data in **Pebble's sstable format**, reusing
Pebble's reader and writer. Compaction is Cassandra 5's Unified Compaction
Strategy (UCS) per table.

Status: M6 is done: Cassandra's architecture and timestamp semantics, a
merging iterator, UCS compaction with sharding and parallel compactions, a
time-window mode, Cassandra's purge rules, read and on-disk snapshots,
streaming (export and validated import) and Merkle trees for repair, all
checked by a randomized model test with crashes. Benchmarks against Pebble
for each milestone are in section 10.

## 1. Goals

- **Cassandra's storage architecture**, so per-table operations (flush,
  snapshot, drop, truncate, repair, streaming) touch only that table and
  tables do not interfere with each other's compaction.
- **Cassandra's semantics**: last-write-wins by cell timestamp, timestamped
  tombstones, TTLs, `gc_grace_seconds`.
- **UCS compaction per table**, as in Cassandra 5, plus a time-window mode
  for insert-only TTL tables (M4).
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
table ID: the table is the directory. The token is 8 bytes, big-endian, so
byte order is ring order (the schema layer maps Murmur3's signed tokens by
flipping the sign bit). Sharding (section 8) reads it; nothing else in the
engine interprets keys.

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
- `Statistics.db`-style metadata (min and max timestamps, max local
  deletion time, tombstone histogram) is recorded per sstable in the
  manifest (M5, section 9) rather than by block property collectors.

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
- **Read snapshots (M6, `Table.NewSnapshot`):** the table's read state (a
  reference on its memtables and sstables) plus the visible sequence
  number, which hides later writes in the memtables. Reads through the
  snapshot (`Get`, `NewIter`) see the table as it was, whatever flushes,
  compactions and purges happen afterwards: replaced sstables stay open and
  on disk until the snapshot and its iterators close. Snapshots must be
  closed before the engine.

## 7a. Snapshots, streaming and repair (M6)

These are the storage engine's side of Cassandra's operations; the
cluster layer (choosing ranges, peers and transfers) is out of scope.

- **On-disk snapshots (`Table.SnapshotToDisk`, `nodetool snapshot`):** the
  table is flushed and its sstables hard-linked into
  `snapshots/<name>/<table>/`, then `manifest.json` (the data files and
  creation time, as Cassandra's `SnapshotManifest`) is written last, so an
  incomplete snapshot is recognisable; this follows `TakeSnapshotTask`
  (flush unless skipped, link, manifest). Not ported: `schema.cql` (no
  schema layer yet), snapshot TTLs and ephemeral snapshots. Links share space with the live
  table until compaction replaces its files. Recovery ignores the snapshots
  directory. `Engine.ClearSnapshot` deletes one; `Table.SnapshotFiles` lists
  one for restoring with `Import`.
- **Streaming out (`Table.ExportRange`):** the table is flushed first, as
  `StreamSession.addTransferRanges` flushes ("always flush on stream
  request"), so what is sent is durable at the sender. Then its sstables
  are sent as they are, as Cassandra does: one wholly inside the range is
  hard-linked into the caller's directory (copied if that is another file
  system), as entire-sstable streaming sends whole files; from one that
  crosses a bound, the section inside the range is written to a new file,
  as Cassandra streams sstable sections. Nothing is merged: the files hold
  row fragments, which the receiver reconciles by timestamp. Writes after
  the flush are not sent.
- **Streaming in (`Table.Import`, also `nodetool import`):** every file is
  read in full and validated first (the equivalent of
  `SSTableImporter`'s `extendedVerify`, which Cassandra leaves off by
  default; the engine always does it, since the scan also yields the
  summary, and it keeps corrupt files out): keys strictly increasing (one entry per
  key, as the engine's reads require), only `SET` and `MERGE` entries, rows
  the Merger decodes; the same pass computes the file's summary. Files on
  the engine's file system with sequence numbers below 2⁴⁸ are then
  hard-linked in unchanged, as Cassandra's zero-copy streaming of entire
  sstables (CASSANDRA-14556) and `SSTableImporter`'s move without
  `copyData` do (the engine links, leaving the source in place); the others
  are merged and rewritten, split at
  shard boundaries. All files are installed in one manifest edit or none,
  and a rejected file installs nothing (`TestImportRejectsBadFiles`).
  Imported rows reconcile by timestamp like any fragment, so importing the
  same data twice changes nothing.
- **Repair (`Table.MerkleTree`, `MerkleTree.Diff`):** a hash tree of the
  rows of a token range, split into `2^depth` equal leaves: the perfect tree
  of Cassandra's `MerkleTree`. As in `Validator` and `MerkleTree`, each row
  (Cassandra: each partition) gets a 256-bit digest (SHA-256 of its
  length-prefixed key and value; Cassandra concatenates two murmur3_128), a
  leaf is the XOR of its rows' digests and an inner node the XOR of its
  children. Rows are hashed after purging everything past gc grace, as
  `CassandraValidationIterator`'s controller does ("we always purge"), so a
  replica that has already purged a deletion matches one that has not.
  Cassandra also splits leaves unevenly by sampled keys; the engine uses
  the perfect tree only.
  `Diff` returns the differing token ranges, adjacent ones merged;
  `TokenRange.Bounds` turns one into key bounds for `ExportRange`.
  `TestRepair` runs a full repair between two diverged engines: after
  streaming the differing ranges both ways their trees and rows are equal.
- *Not done:* incremental repair (repaired/unrepaired sstable sets and
  `repairedAt`), and with it `only_purge_repaired_tombstones`.

## 8. Compaction: UCS per table (M3, M4)

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
  fraction of the token space it covers, from its first key's token to its
  last's (`ShardManager.density`); a span under 2⁻⁴⁸ (one partition, or a
  test's narrow keys) counts as the whole space, as
  `MINIMUM_TOKEN_COVERAGE` does. Level `n` starts at density `m·F^n`, where
  `m` is the flush size. The engine is one node owning the whole ring, so
  there are no local ranges to discount.
- **Trigger and buckets (M4):** within a level, sstables form overlap sets
  (`Overlaps.constructOverlapSets`: for every key, some set holds every
  sstable covering it). A set with `T` or more sstables triggers a
  compaction; it is extended transitively with neighbouring sets that share
  an sstable (`assignOverlapsIntoBuckets`, `TRANSITIVE`), forming a bucket.
  The bucket with the highest overlap wins, across levels too, ties going to
  the lower level. A late bucket is cut to its oldest sstables (by maximum
  timestamp) so that no overlap set holds more than the limit
  (`pullLastWithOverlapLimit`).
- **Sharding (M4):** at the start of a compaction, the output's density is
  estimated (total input size over the inputs' combined span) and turned
  into a shard count with `Controller.getNumShards`: `base_shard_count`
  shards, multiplied by powers of two as density grows past
  `target_sstable_size`, damped by `sstable_growth`, and reduced to a power
  of two below `min_sstable_size`. The token space is cut into that many
  equal shards, and the output gets one sstable per shard it touches.
  Boundaries are whole tokens, so a partition is never split, and since
  every count is a power-of-two multiple of the base, a boundary at one
  density is a boundary at every higher one. Flushes are split the same
  way, by the memtable's size.
- **Parallelism (M4):** each output shard is its own task (UCS's
  `parallelize_output_shards`, on by default; `SerialOutputShards` turns it
  off), and a table runs several compactions at once: each pick excludes
  sstables other compactions use, and buckets share no sstable.
  `MaxConcurrentCompactions` (Cassandra's `concurrent_compactors`) bounds
  both the compactions and the shard tasks running at once. A compaction
  installs all its shards in one manifest edit, or none.
- **Installing a compaction (M3):** the output is written and synced, then
  one manifest edit adds it and deletes the inputs. Readers switch to the new
  read state; each input file is closed and deleted when its last reader
  releases it. After a crash, recovery deletes sstables the manifest does
  not reference, so a compaction is either fully installed or invisible.
- **Fully expired sstables (M5)** are dropped without rewriting, checked
  every `ExpiredCheckInterval` (10 minutes, `expired_sstable_check_frequency_seconds`;
  `CompactionController.getFullyExpiredSSTables`, used by every Cassandra
  strategy). See section 9.
- **Time-window mode (`TableOptions.TimeWindow`):** a port of
  `TimeWindowCompactionStrategy.getNextBackgroundSSTables` and
  `newestBucket`: sstables are grouped by the window of their maximum write
  timestamp. The current window is the newest one ever seen
  (`highestWindowSeen`, which never moves back, not the wall clock); it is
  compacted with size-tiered rules (`SizeTieredCompactionStrategy.getBuckets`
  and `mostInterestingBucket`, `bucket_low` 0.5, `bucket_high` 1.5,
  `min_sstable_size` 50 MiB) once it holds `min_threshold` (4) sstables. Any
  older window with two or more sstables is compacted, its `max_threshold`
  (32) smallest at a time (`trimToThreshold`). Windows are visited newest
  first; the first eligible wins. Output is not sharded, so a window ends
  as one file that expires as a whole. STCS ranks buckets by read hotness;
  the engine has no read meters, so every hotness is 0 (as for Cassandra's
  unread sstables) and the tie-break decides: `Collections.max` over it
  picks the bucket with the largest average size (the code's comment says
  the smallest; the code is followed).
- **Choosing among buckets:** within a level, UCS picks uniformly at random
  among the buckets with the highest overlap (reservoir sampling), as
  `Level.getCompactionPick` does. One deliberate difference: Cassandra
  compares levels by their maximum overlap even when it is below the
  level's threshold, which with per-level scaling parameters can discard a
  lower level's valid pick; the engine compares only levels with a pick.

## 9. Purging (M5)

Compared with cassandra-5.0's `AbstractCell.purge`, `DeletionPurger`,
`CompactionController.getPurgeEvaluator` and `getFullyExpiredSSTables`,
`AbstractCompactionStrategy.worthDroppingTombstones` and
`StatsMetadata.getEstimatedDroppableTombstoneRatio`. (An earlier version
of this document said these were checked when only `AbstractCell.purge`
had been read; the comparison below was done afterwards, and the
differences it found were fixed or are listed.) Cell reconciliation
(section 4) matches `CellLivenessInfo.resolve` and
`resolveSameTimestampTie` exactly.

- **Purge rule.** A compaction drops a cell tombstone, an expired cell or a
  row deletion when its local deletion time (the expiry time, for expiring
  cells) is before `gcBefore = now − gc_grace_seconds` and its timestamp is
  below the row's *maximum purgeable timestamp*: the smallest timestamp of
  the row's data outside the compaction, in every memtable (including
  entries not yet visible to readers) and in every sstable of the table's
  current state that may hold the key (key range, then a point lookup that
  starts with the Bloom filter). Cassandra's evaluator is coarser: after a
  Bloom filter check it takes the whole sstable's minimum timestamp, and a
  memtable's minimum if it holds the partition, because it must also
  cover partition and range deletions. Rows here are independent keys, so
  the row's own minimum is the exact bound, and the engine purges at least
  as often. Such a deletion cannot shadow anything
  still stored, so dropping it never changes what reads return, now or
  later (`row.TestPurgeInvisible` checks this on random rows; changing the
  strict `<` to `<=` already fails it). A row left with nothing is dropped.
- **Cheaper than it sounds.** Compaction decodes rows only if some input's
  summary says it may hold a deletion past `gcBefore`; otherwise rows are
  copied undecoded, as in M3.1. When it does, one decoding per row both
  purges it and yields its statistics (`row.PurgeAndSummarize`), into a
  pooled scratch row. The lookups run only for rows with a candidate, skip
  sources whose data is all newer than every candidate, stop at the first
  source at or below the oldest candidate, read memtable fragments without
  merging them, and reuse one point iterator per sstable for the whole
  compaction shard (rows come in key order).
- **Not ported: turning expired cells into tombstones.** Cassandra rewrites
  an expired cell that cannot be purged yet as a tombstone (smaller). A
  tombstone beats an expiring cell of the same timestamp, so the rewrite
  could hide a later-expiring cell stored elsewhere; the engine keeps the
  expired cell until it can be purged.
- **The gc grace contract.** As in Cassandra, a write that arrives after a
  deletion was purged, with an older timestamp than the deletion, is not
  shadowed by it: `gc_grace_seconds` bounds how late writes (hints, repair)
  may arrive. The model test generates writes accordingly.
- **Summaries.** Every sstable records, in the manifest, its creation time,
  row and cell counts, number of droppable parts (tombstones, expiring cells, row
  deletions), the smallest and largest local deletion times (the largest
  is "never" if any cell is live), and a 32-bin streaming histogram of drop
  times (Cassandra's `TombstoneHistogram` has 100 bins and rounds times to
  60 s). Flushes and purging
  compactions compute them exactly; compactions that copy rows undecoded
  combine the inputs' (upper bounds on counts, safe bounds on times). Files
  from before M5 have none and are never dropped or picked for tombstones.
- **Fully expired sstables.** As `getFullyExpiredSSTables`: an sstable whose
  largest local deletion time is before `gcBefore`, and whose newest data
  is older than the smallest timestamp of the overlapping sstables that
  still hold data (those that are themselves fully expired do not count)
  and of the memtables, is deleted without being read. Cassandra takes each
  memtable's minimum timestamp; the engine scans the memtables over the
  sstable's key range, up to 4096 entries (beyond that the sstable is
  kept).
- **Single-sstable tombstone compactions.** A port of
  `worthDroppingTombstones`: an sstable older than
  `tombstone_compaction_interval` (one day) whose droppable tombstones over
  its cell count exceed `tombstone_threshold` (0.2) qualifies if nothing
  overlaps it, or it is fully expired, or, unless
  `unchecked_tombstone_compaction`, the share of its keys outside the
  overlapping sstables' token ranges times that ratio still exceeds the
  threshold (Cassandra estimates the share from index samples, the engine
  from token spans, assuming keys spread evenly; BTI sstables, like these,
  always count as informative). The time-window mode picks the smallest
  qualifying sstable, as TWCS does; UCS tables pick the largest, as STCS
  does. Cassandra 5.0's UCS has no such compactions: for UCS tables they
  are this design's due-file picking, an extension. A file written in the
  current second never qualifies, so with an interval of 0 a file whose
  tombstones cannot be dropped yet is not rewritten in a loop.
- **Row existence.** The row codec has no primary-key liveness (Cassandra's
  row marker): a row with no cells and no deletion is empty and may be
  dropped. The schema layer must represent `INSERT` of a key alone with a
  cell (for example an empty marker column).
- **Major compaction (`Table.Compact`):** Cassandra's `nodetool compact`:
  flush, wait for the table's running compactions (pausing new ones), then
  merge and purge all of its sstables into one set of shards.
- *Not done:* repair-aware purging (`only_purge_repaired_tombstones`) needs
  repaired-state tracking (section 11).

## 10. Testing and benchmarks

- Unit tests per component, with `vfs.NewMem`.
- Crash tests with `vfs.NewCrashableMem`: crash at random points, dropping
  unsynced data, and check that every synced write survives and nothing else
  appears.
- The model test (M6) also holds read snapshots across random operations
  and checks them against the model as of their creation, and streams
  random ranges out and back in (which must change nothing).
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

Each run of the suite measures the engine and Pebble side by side, so the
ratio to Pebble from the same run (engine ÷ Pebble, below 1 is faster) is
the more reliable comparison between milestones; absolute figures drift with
the VM's load (Pebble's own `Put` varied by 22% between the M3 and M3.1
runs).

| Benchmark | M3 | M3 ÷ Pebble | M3.1 | M3.1 ÷ Pebble | Pebble (M3.1 run) |
|---|---|---|---|---|---|
| `Put`, one writer, periodic sync | 1149 ns | 0.90 | 1446 ns | 0.93 | 1558 ns |
| `Put`, 4 writers, periodic sync | 1232 ns | 0.96 | 1240 ns | 0.88 | 1413 ns |
| `Put`, 4 writers, synced | 3135 ns | 1.06 | 3249 ns | 1.18 | 2750 ns |
| `Get` from sstables | 1708 ns | 0.68 | 1686 ns | **0.65** | 2593 ns |
| `Scan100` (seek, then 100 rows) | 8.9 µs | 0.84 | 8.4 µs | **0.76** | 10.9 µs |
| `WriteSustained` (with compactions) | 2289 ns | 1.38 | 1877 ns | **1.06** | 1775 ns |
| `WriteSustained` write amplification | 2.50 | 0.85 | 2.81 | 0.96 | 2.93 |
| Allocations per `Get` / `Scan100` | 4 / 11 | | 3 / 3 | | 2 / 2 |

(The benchmark's own key formatting accounts for 2 allocations per
operation in every column.)

What changed in M3.1, from CPU, allocation and blocking profiles of
`WriteSustained` (writers were never stalled; the gap was CPU spent in
compaction and in growing files):

- **Compaction reads bypass the block cache.** Inputs are read with
  Pebble's compaction iterator into a private buffer pool, with sequential
  readahead, as Pebble's own compactions do, so compactions no longer
  insert every block they read into the cache (and evict blocks that reads
  use).
- **The merging iterator no longer copies.** Sources holding the current
  key are advanced at the next positioning call, so a row held by one source
  is passed on without copying its key and value; only rows with fragments
  in several sources are merged into a new buffer.
- **Compaction output timestamps come from the inputs.** The output's
  minimum and maximum timestamps are the union of the inputs' instead of
  being decoded from every row. Merging only keeps or drops existing cells
  and deletions, so the union bounds the output; a wider range only makes
  reads stop early less often, and purging (M5) more conservative.
- Together these cut `WriteSustained` by 18–23% (1.38× Pebble to 1.06×).
  Write amplification rose from 2.50 to 2.81, still below Pebble's 2.93:
  with compactions keeping up, more of them run before the benchmark ends.
- **WAL segments are recycled**, as Pebble and older Cassandra versions do:
  up to two obsolete segments are renamed and overwritten instead of
  creating new files. On a real disk this avoids allocating blocks and
  updating the file size on every fsync. Records carry their segment
  number, so the old segment's records past the new end are recognised on
  replay and end it (`TestWALRecycleReplay`, `TestWALRecycling`). Recovery
  deletes recycled segments, which lie below the manifest's minimum WAL
  segment. The in-memory benchmarks barely show it: the `Put` benchmarks
  never flush, so no segment becomes obsolete.
- **Reads allocate less.** `Get` merges an sstable's entry while the
  iterator still pins the block, instead of copying it first (one copy per
  hit instead of two). `Iterator` keeps its sources, heap entries and a few
  memtable and sstable sources inline, so a scan over a few files costs one
  allocation; empty memtables are skipped.
- **Synced writes are still slower than Pebble's** (see the M2 notes above); this
  milestone did not change the commit path.
- *Later:* the commit pipeline (memtable nodes linked outside `commitMu`,
  batches published in order) was tried again after M6 and measured
  slower again in an interleaved A/B on this machine: single-writer `Put`
  +19%, unsynced parallel puts +14%, synced within noise. It was reverted.
  New WAL segments are now preallocated to `WALSegmentSize`, as Cassandra
  creates commit log segments at their full size; that made no measurable
  difference on this VM's disk, and the in-memory file system ignores it.
  On a real disk (`BenchmarkPutParallelDisk`), synced 4-writer puts are
  on par with Pebble's (0.94x and 1.05x in two runs): fsync dominates, and
  most of the in-memory benchmark's gap comes from growing the WAL file in
  memory.

### M4 results

M4 adds `WriteSustainedTokens`: the sustained-write workload with each key
prefixed by a hash of it, as the schema layer prefixes keys with the
partition's token. The original benchmark's keys share their first 7 bytes,
so they span less than 2⁻⁴⁸ of the token space and never shard. `lsm` uses
the default sharding, which does not shard at this data size
(`min_sstable_size` is 100 MiB); `lsm-sharded` scales the targets to the
benchmark (4 base shards, 4 MiB target, no minimum), so flushes and
compactions split into shards that compact in parallel. Pebble's wait for
its compactions also ends once it has started none for 500 ms, with the
timer paused meanwhile: with these keys Pebble reports a compaction debt it
never schedules, and the earlier wait never returned.

| Benchmark | M4 | M4 ÷ Pebble | Pebble (M4 run) |
|---|---|---|---|
| `Put`, one writer, periodic sync | 1064 ns | 0.83 | 1275 ns |
| `Put`, 4 writers, periodic sync | 1150 ns | 0.86 | 1333 ns |
| `Put`, 4 writers, synced | 3168 ns | 1.10 | 2882 ns |
| `Get` from sstables | 1650 ns | 0.60 | 2736 ns |
| `Scan100` | 8.5 µs | 0.78 | 10.9 µs |
| `WriteSustained` | 1715 ns | 1.06 | 1621 ns |
| `WriteSustained` write amplification | 3.05 | 0.99 | 3.07 |
| `WriteSustainedTokens`, `lsm` | 1775 ns | 0.93 | 1899 ns |
| `WriteSustainedTokens`, `lsm-sharded` | **1466 ns** | **0.77** | 1899 ns |
| `WriteSustainedTokens` write amplification, `lsm` / `lsm-sharded` | 2.88 / 2.71 | 0.85 / 0.80 | 3.39 |

Write amplification of `WriteSustained` rose from 2.81 to 3.05: buckets now
take in every sstable that transitively overlaps the triggering set, as
Cassandra's do, so compactions are larger; it stays level with Pebble's.

### M5 results

`DeleteHeavy` overwrites and deletes random keys of a 200k-key token-spread
key space (one operation in three is a delete; every operation carries the
next timestamp, so the last one wins in both stores), waits for
compactions, then moves the clock an hour ahead and runs a major
compaction. `lsm` uses `gc_grace_seconds` 0; `lsm-nopurge` keeps the
default 10 days, so the difference is what purging reclaims. Pebble deletes
with its own tombstones. The other benchmarks were unchanged within noise.

| Benchmark | M5 | M5 ÷ Pebble | Pebble (M5 run) |
|---|---|---|---|
| `Put`, one writer | 1194 ns | 0.94 | 1269 ns |
| `Put`, 4 writers, periodic sync | 1097 ns | 0.84 | 1303 ns |
| `Put`, 4 writers, synced | 3309 ns | 1.20 | 2747 ns |
| `Get` from sstables | 1571 ns | 0.64 | 2461 ns |
| `Scan100` | 8.5 µs | 0.78 | 11.0 µs |
| `WriteSustained` | 1699 ns | 1.04 | 1628 ns |
| `WriteSustainedTokens`, `lsm` / `lsm-sharded` | 1814 / 1547 ns | 1.00 / 0.85 | 1810 ns |
| `DeleteHeavy`, `lsm` (purging) | 2506 ns | 1.26 | 1992 ns |
| `DeleteHeavy`, `lsm-nopurge` | 2130 ns | 1.07 | 1992 ns |
| `DeleteHeavy` sstable MiB before major compaction, `lsm` / `lsm-nopurge` | 7.05 / 11.41 | 1.45 / 2.35 | 4.85 |
| `DeleteHeavy` sstable MiB after major compaction, `lsm` / `lsm-nopurge` | 5.06 / 7.40 | 1.20 / 1.76 | 4.20 |

Purging costs 18% on this workload (decoding rows of compactions that may
hold purgeable deletions, and the point lookups for maximum purgeable
timestamps) and saves 38% of the space before the major compaction. What
remains above Pebble is Cassandra's model rather than overhead: a
tombstone stays while any sstable outside the compaction may hold older
data for its key, which tiered compaction (T4) keeps longer than Pebble's
leveled bottom level, and every tombstone is a row with a timestamp and a
deletion time.

### M6 results (final)

The plan was to benchmark against the Pebble fork at M6; the fork is not
available to this repository, so every comparison is with upstream Pebble
at the commit in `go.mod`. `StreamRange` (new) moves a 100k-row table to a
second store: the engine exports and imports (validated, then linked),
Pebble writes an external sstable from an iterator and ingests it, without
reading it back.

| Benchmark | M6 | M6 ÷ Pebble | Pebble (M6 run) |
|---|---|---|---|
| `Put`, one writer | 1292 ns | 0.91 | 1413 ns |
| `Put`, 4 writers, periodic sync | 1201 ns | 0.92 | 1308 ns |
| `Put`, 4 writers, synced | 3311 ns | 1.11 | 2982 ns |
| `Get` from sstables | 1682 ns | **0.61** | 2755 ns |
| `Scan100` | 8.4 µs | **0.81** | 10.4 µs |
| `WriteSustained` | 1740 ns | 1.01 | 1721 ns |
| `WriteSustainedTokens`, `lsm` / `lsm-sharded` | 2006 / 1752 ns | 1.05 / **0.91** | 1919 ns |
| write amplification, `WriteSustainedTokens` `lsm-sharded` | 2.74 | **0.81** | 3.39 |
| `DeleteHeavy`, purging / not | 2394 / 2071 ns | 1.17 / 1.01 | 2044 ns |
| `DeleteHeavy` MiB after major compaction, purging / not | 5.06 / 7.40 | 1.20 / 1.76 | 4.20 |
| `StreamRange` per row | 847 ns | 1.93 | 438 ns |

Ratio to Pebble (same run) across milestones:

| Benchmark | M3 | M3.1 | M4 | M5 | M6 |
|---|---|---|---|---|---|
| `Put`, one writer | 0.90 | 0.93 | 0.83 | 0.94 | 0.91 |
| `Put`, 4 writers | 0.96 | 0.88 | 0.86 | 0.84 | 0.92 |
| `Put`, 4 writers, synced | 1.06 | 1.18 | 1.10 | 1.20 | 1.11 |
| `Get` | 0.68 | 0.65 | 0.60 | 0.64 | 0.61 |
| `Scan100` | 0.84 | 0.76 | 0.78 | 0.78 | 0.81 |
| `WriteSustained` | 1.38 | 1.06 | 1.06 | 1.04 | 1.01 |
| `WriteSustainedTokens`, sharded | | | 0.77 | 0.85 | 0.91 |

Where the engine is still slower, and why:

- **Synced writes (1.1–1.2×):** the commit path inserts into the memtable
  under the commit lock; see the M2 notes. Cassandra's default commit log
  mode is periodic, where the engine is faster.
- **Deletes with purging (1.17× time, 1.2× space after a major
  compaction):** purging costs row decoding and maximum-purgeable lookups,
  and Cassandra's rule keeps a tombstone while older data for its key may
  remain elsewhere; see the M5 notes.
- **Streaming (1.9×):** Import reads every file in full to validate it and
  compute its summary before linking it in; Pebble's ingest reads only the
  metadata. The validation is what keeps a corrupt or foreign file out of
  the table, so it stays.

## 11. Milestones

| Milestone | Contents |
|---|---|
| **M1** (done) | WAL, memtable, flush to sstables, manifest, recovery, point lookups, for one key space. |
| **M1.5** (done) | Cassandra's architecture: one WAL with segments and a memory budget, per-table memtables, sstables and directories, create and drop table. |
| **M2** (done) | Cassandra's timestamp semantics (row codec, merge on read and flush, timestamp-ordered reads); periodic and group commit log sync; the speed fixes in section 10. |
| **M3** (done) | Merging iterator, UCS for one shard per table, model test, reference-counted read states. |
| **M3.1** (done) | Speed: compaction reads outside the block cache, copy-free merging iterator, WAL segment recycling, fewer read allocations. |
| **M4** (done) | UCS sharding of flushes and compactions, overlap buckets, parallel output shards and concurrent compactions per table, time-window mode. |
| **M5** (done) | Purge rule in compactions, per-sstable summaries in the manifest, fully expired sstable drops, single-sstable tombstone compactions. |
| **M6** (done) | Read snapshots, on-disk snapshots, range export and validated import (linked when possible), Merkle trees for repair; final benchmarks against Pebble. |
