// Package lsm is cassandra-room's LSM storage engine. See
// docs/storage-engine.md for the design.
package lsm

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("lsm: not found")
	// ErrClosed is returned by operations on a closed engine.
	ErrClosed = errors.New("lsm: closed")
	// ErrUnknownTable is returned for a table that does not exist or was
	// dropped.
	ErrUnknownTable = errors.New("lsm: unknown table")
	// ErrTableExists is returned by CreateTable for an existing table.
	ErrTableExists = errors.New("lsm: table exists")
)

// TableID identifies a table. The schema layer maps Cassandra's table UUIDs
// to TableIDs.
type TableID uint64

func (id TableID) dirName() string { return fmt.Sprintf("%016x", uint64(id)) }

// Options configures an Engine. The zero value is usable; see ensureDefaults.
type Options struct {
	// FS is the file system the engine lives on. Defaults to vfs.Default.
	FS vfs.FS
	// MemTableBudget is the memory all memtables may use together, including
	// those waiting to be flushed. Writes stall while it is exceeded.
	// Defaults to 256 MiB. (Cassandra: memtable_heap_space.)
	MemTableBudget int64
	// MemTableCleanupThreshold is the fraction of MemTableBudget that mutable
	// memtables may use before the largest is flushed. Defaults to 0.33.
	// (Cassandra: memtable_cleanup_threshold, 1/(memtable_flush_writers+1).)
	MemTableCleanupThreshold float64
	// WALSegmentSize is the size at which the commit log moves to a new
	// segment. Defaults to 32 MiB. (Cassandra: commitlog_segment_size.)
	WALSegmentSize int64
	// WALTotalSpace is the size of live commit log segments above which the
	// tables pinning the oldest segment are flushed. Defaults to 1 GiB.
	// (Cassandra: commitlog_total_space.)
	WALTotalSpace int64
	// WALSyncPeriod is how often the commit log is fsynced in the background,
	// making earlier NoSync writes durable. Defaults to 10 s. Negative
	// disables it. (Cassandra: commitlog_sync periodic, with
	// commitlog_sync_period 10000ms, its default.)
	WALSyncPeriod time.Duration
	// BloomBitsPerKey configures the Bloom filter written into every sstable.
	// Defaults to 10 (about 1% false positives, Cassandra's default).
	BloomBitsPerKey uint32
	// BlockSize is the target uncompressed size of sstable data blocks. Zero
	// uses Pebble's default.
	BlockSize int
	// CacheSize is the size of the block cache shared by all tables.
	// Defaults to 64 MiB.
	CacheSize int64
	// Merger reconciles versions of a row. Defaults to Cassandra's rules
	// (row.Merger).
	Merger Merger
	// TableOptions returns a table's compaction options; the schema layer
	// supplies them. Nil gives every table the defaults (UCS T4).
	TableOptions func(TableID) TableOptions
	// MaxConcurrentCompactions limits compactions running at once, across
	// tables. Defaults to min(max(NumCPU/2, 2), 8). (Cassandra:
	// concurrent_compactors.)
	MaxConcurrentCompactions int
	// DisableCompactions turns automatic compactions off (for tests and
	// benchmarks).
	DisableCompactions bool
	// Now is the clock used to decide what is expired and purgeable.
	// Defaults to time.Now.
	Now func() time.Time
}

// Merger reconciles row fragments. The engine stores every write as a
// fragment and combines fragments of the same key with Merge, in any order:
// Merge must be commutative and associative, as Cassandra's reconciliation
// by timestamp is.
type Merger interface {
	// Merge combines two fragments of the same row and appends the result to
	// dst.
	Merge(dst, a, b []byte) ([]byte, error)
	// Timestamps returns the smallest and largest write timestamps in a
	// fragment, including deletions. ok is false if it holds none.
	Timestamps(v []byte) (minTS, maxTS int64, ok bool, err error)
	// DeletedAt returns the timestamp at or below which the fragment's row
	// deletion shadows all data, or math.MinInt64 if it has none.
	DeletedAt(v []byte) (int64, error)
}

func (o *Options) ensureDefaults() {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.MemTableBudget <= 0 {
		o.MemTableBudget = 256 << 20
	}
	if o.MemTableCleanupThreshold <= 0 || o.MemTableCleanupThreshold > 1 {
		o.MemTableCleanupThreshold = 0.33
	}
	if o.WALSegmentSize <= 0 {
		o.WALSegmentSize = 32 << 20
	}
	if o.WALTotalSpace <= 0 {
		o.WALTotalSpace = 1 << 30
	}
	if o.WALSyncPeriod == 0 {
		o.WALSyncPeriod = 10 * time.Second
	}
	if o.BloomBitsPerKey == 0 {
		o.BloomBitsPerKey = 10
	}
	if o.CacheSize <= 0 {
		o.CacheSize = 64 << 20
	}
	if o.Merger == nil {
		o.Merger = row.Merger{}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxConcurrentCompactions <= 0 {
		o.MaxConcurrentCompactions = min(max(runtime.NumCPU()/2, 2), 8)
	}
}

// tableFormat is the sstable format the engine writes: Pebble's columnar
// format with checksummed footers.
const tableFormat = sstable.TableFormatPebblev6

// WriteOptions configures a single write.
type WriteOptions struct {
	// Sync makes the write durable (the WAL is fsynced) before Apply returns,
	// like Cassandra's commitlog_sync batch or group mode. Without it, the
	// write is durable after the next periodic sync (WALSyncPeriod), like
	// Cassandra's default periodic mode.
	Sync bool
}

var (
	// Sync is a WriteOptions that fsyncs the WAL.
	Sync = &WriteOptions{Sync: true}
	// NoSync is a WriteOptions that hands the write to the OS without fsync.
	NoSync = &WriteOptions{Sync: false}
)
