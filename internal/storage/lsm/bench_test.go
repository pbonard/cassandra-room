package lsm

// Benchmarks comparing the engine with Pebble (pinned in go.mod) on the same
// workload: the same keys and the same encoded row values (one 100-byte cell).
// Run with: go test -run x -bench . ./internal/storage/lsm/

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

func benchKey(i int) []byte { return []byte(fmt.Sprintf("key%012d", i)) }

func benchValue() []byte {
	return row.Append(nil, &row.Row{
		Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
		Cells:    []row.Cell{{Column: 1, Timestamp: 1, LocalDeletionTime: row.NoDeletionTime, Value: make([]byte, 100)}},
	})
}

type benchStore interface {
	put(key, value []byte, sync bool) error
	get(key []byte) error
	flush() error
	// compact leaves each key in one file. The benchmarks write every key
	// once and flush once, so the engine already holds one file; Pebble
	// moves its flushed file down with a manual compaction.
	compact() error
	close() error
}

type lsmStore struct {
	e   *Engine
	tbl *Table
}

func (s lsmStore) put(key, value []byte, sync bool) error {
	wo := NoSync
	if sync {
		wo = Sync
	}
	return s.e.Put(1, key, value, wo)
}
func (s lsmStore) get(key []byte) error { _, err := s.tbl.Get(key); return err }
func (s lsmStore) flush() error         { return s.e.Flush() }
func (s lsmStore) compact() error       { return nil }
func (s lsmStore) close() error         { return s.e.Close() }

type pebbleStore struct{ db *pebble.DB }

func (s pebbleStore) put(key, value []byte, sync bool) error {
	wo := pebble.NoSync
	if sync {
		wo = pebble.Sync
	}
	return s.db.Set(key, value, wo)
}
func (s pebbleStore) get(key []byte) error {
	_, c, err := s.db.Get(key)
	if err == nil {
		c.Close()
	}
	return err
}
func (s pebbleStore) flush() error { return s.db.Flush() }
func (s pebbleStore) compact() error {
	return s.db.Compact(context.Background(), []byte("a"), []byte("z"), true)
}
func (s pebbleStore) close() error { return s.db.Close() }

func openBench(b *testing.B, engine string) benchStore {
	switch engine {
	case "lsm":
		e, err := Open("db", &Options{FS: vfs.NewMem(), DisableCompactions: true})
		if err != nil {
			b.Fatal(err)
		}
		tbl, err := e.CreateTable(1)
		if err != nil {
			b.Fatal(err)
		}
		return lsmStore{e, tbl}
	default:
		// Automatic compactions are off for the read and write benchmarks;
		// BenchmarkWriteSustained measures them. Pebble gets the same block
		// cache size and Bloom filters as the engine's defaults.
		c := pebble.NewCache(64 << 20)
		defer c.Unref()
		opts := &pebble.Options{FS: vfs.NewMem(), Cache: c, DisableAutomaticCompactions: true}
		for i := range opts.Levels {
			opts.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) }
		}
		db, err := pebble.Open("db", opts)
		if err != nil {
			b.Fatal(err)
		}
		return pebbleStore{db}
	}
}

var engines = []string{"lsm", "pebble"}

func BenchmarkPut(b *testing.B) {
	val := benchValue()
	for _, eng := range engines {
		b.Run(eng, func(b *testing.B) {
			s := openBench(b, eng)
			defer s.close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.put(benchKey(i), val, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPutParallel(b *testing.B) {
	val := benchValue()
	for _, eng := range engines {
		for _, sync := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/sync=%t", eng, sync), func(b *testing.B) {
				s := openBench(b, eng)
				defer s.close()
				var n atomic.Int64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := s.put(benchKey(int(n.Add(1))), val, sync); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

func BenchmarkGetFromSSTables(b *testing.B) {
	const n = 100000
	val := benchValue()
	for _, eng := range engines {
		b.Run(eng, func(b *testing.B) {
			s := openBench(b, eng)
			defer s.close()
			for i := 0; i < n; i++ {
				if err := s.put(benchKey(i), val, false); err != nil {
					b.Fatal(err)
				}
			}
			if err := s.flush(); err != nil {
				b.Fatal(err)
			}
			// Both engines now read from a single file per key.
			if err := s.compact(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.get(benchKey((i * 7919) % n)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScan100 seeks to a random key and reads the next 100 rows.
func BenchmarkScan100(b *testing.B) {
	const n = 100000
	val := benchValue()
	for _, eng := range engines {
		b.Run(eng, func(b *testing.B) {
			s := openBench(b, eng)
			defer s.close()
			for i := 0; i < n; i++ {
				if err := s.put(benchKey(i), val, false); err != nil {
					b.Fatal(err)
				}
			}
			if err := s.flush(); err != nil {
				b.Fatal(err)
			}
			if err := s.compact(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := benchKey((i * 7919) % (n - 100))
				switch st := s.(type) {
				case lsmStore:
					it, err := st.tbl.NewIter(nil, nil)
					if err != nil {
						b.Fatal(err)
					}
					c := 0
					for ok := it.SeekGE(start); ok && c < 100; ok = it.Next() {
						c++
					}
					it.Close()
				case pebbleStore:
					it, err := st.db.NewIter(nil)
					if err != nil {
						b.Fatal(err)
					}
					c := 0
					for ok := it.SeekGE(start); ok && c < 100; ok = it.Next() {
						_ = it.Value()
						c++
					}
					it.Close()
				}
			}
		})
	}
}

// BenchmarkWriteSustained overwrites random keys of a 200k-key space with
// compactions running (the engine: UCS T4; Pebble: leveled), waits for
// compactions to finish, and reports write amplification as
// (flushed + compacted) / flushed bytes. Run with a fixed count, for
// example -benchtime=1000000x.
func BenchmarkWriteSustained(b *testing.B) {
	benchWriteSustained(b, benchKey, []string{"lsm", "pebble"})
}

// tokenBenchKey prefixes the key with a hash of it, as the schema layer
// prefixes keys with the partition's Murmur3 token, so keys spread over the
// token space and UCS can shard.
func tokenBenchKey(i int) []byte {
	k := benchKey(i)
	return append(tokenKey(xxhash.Sum64(k)), k...)
}

// BenchmarkWriteSustainedTokens is BenchmarkWriteSustained with keys spread
// over the token space. "lsm" uses the default sharding (which, at this data
// size, does not shard: min_sstable_size is 100 MiB); "lsm-sharded" scales
// the shard targets down to the benchmark (4 base shards, 4 MiB target, no
// minimum) so flushes and compactions split and run in parallel.
func BenchmarkWriteSustainedTokens(b *testing.B) {
	benchWriteSustained(b, tokenBenchKey, []string{"lsm", "lsm-sharded", "pebble"})
}

func benchWriteSustained(b *testing.B, key func(int) []byte, engines []string) {
	val := benchValue()
	for _, eng := range engines {
		b.Run(eng, func(b *testing.B) {
			var put func(k []byte) error
			var wait func()
			var wa func() float64
			var closeFn func() error
			switch eng {
			case "lsm", "lsm-sharded":
				opts := &Options{FS: vfs.NewMem(), MemTableBudget: 12 << 20}
				if eng == "lsm-sharded" {
					opts.TableOptions = func(TableID) TableOptions {
						return TableOptions{BaseShardCount: 4, TargetSSTableSize: 4 << 20, MinSSTableSize: -1}
					}
				}
				e, err := Open("db", opts)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := e.CreateTable(1); err != nil {
					b.Fatal(err)
				}
				put = func(k []byte) error { return e.Put(1, k, val, NoSync) }
				wait = func() {
					if err := e.Flush(); err != nil {
						b.Fatal(err)
					}
					if err := e.WaitForCompactions(); err != nil {
						b.Fatal(err)
					}
				}
				wa = func() float64 { return e.Metrics().WriteAmplification }
				closeFn = e.Close
			default:
				c := pebble.NewCache(64 << 20)
				defer c.Unref()
				opts := &pebble.Options{FS: vfs.NewMem(), Cache: c, MemTableSize: 4 << 20}
				for i := range opts.Levels {
					opts.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) }
				}
				db, err := pebble.Open("db", opts)
				if err != nil {
					b.Fatal(err)
				}
				put = func(k []byte) error { return db.Set(k, val, pebble.NoSync) }
				wait = func() {
					if err := db.Flush(); err != nil {
						b.Fatal(err)
					}
					waitPebble(b, db)
				}
				wa = func() float64 {
					// Sum the levels directly: Metrics.Total adds WAL bytes to
					// the flushed bytes, which the engine's metric does not.
					var flushed, compacted float64
					m := db.Metrics()
					for i := range m.Levels {
						l := &m.Levels[i]
						flushed += float64(l.TablesFlushed.Bytes + l.BlobBytesFlushed)
						compacted += float64(l.TablesCompacted.Bytes + l.BlobBytesCompacted)
					}
					if flushed == 0 {
						return 0
					}
					return (flushed + compacted) / flushed
				}
				closeFn = db.Close
			}
			defer closeFn()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := put(key((i * 7919) % 200000)); err != nil {
					b.Fatal(err)
				}
			}
			wait()
			b.StopTimer()
			b.ReportMetric(wa(), "write-amp")
		})
	}
}

// BenchmarkDeleteHeavy overwrites and deletes random keys of a 200k-key,
// token-spread key space (one operation in three is a delete), waits for
// compactions, and reports the sstable bytes left. The engine writes
// Cassandra tombstones with gc_grace_seconds 0, so compaction purges them
// (and the data they shadow) once nothing older remains elsewhere; Pebble
// deletes with its DELETE tombstones, dropped when they reach the bottom of
// the LSM. "lsm-nopurge" keeps the default 10-day gc_grace_seconds, so
// nothing is purged: the difference with "lsm" is what purging reclaims.
// After the timed part, the clock moves an hour ahead (past gc grace for
// "lsm") and both stores run a major compaction; "MiB-left" is measured
// after it, "MiB-before-major" before.
func BenchmarkDeleteHeavy(b *testing.B) {
	// Every operation gets the next timestamp, so the last one wins in both
	// stores (Cassandra reconciles by timestamp, Pebble by arrival).
	var ts atomic.Int64
	ts.Store(time.Now().UnixMicro())
	value := func() []byte {
		return row.Append(nil, &row.Row{
			Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
			Cells:    []row.Cell{{Column: 1, Timestamp: ts.Add(1), LocalDeletionTime: row.NoDeletionTime, Value: make([]byte, 100)}},
		})
	}
	for _, eng := range []string{"lsm", "lsm-nopurge", "pebble"} {
		b.Run(eng, func(b *testing.B) {
			var put, del func(k []byte) error
			var wait func()
			var tableBytes func() float64
			var major func() error
			var closeFn func() error
			switch eng {
			case "lsm", "lsm-nopurge":
				grace := int64(-1)
				if eng == "lsm-nopurge" {
					grace = 0 // the default
				}
				var skew atomic.Int64
				e, err := Open("db", &Options{FS: vfs.NewMem(), MemTableBudget: 12 << 20,
					Now:          func() time.Time { return time.Now().Add(time.Duration(skew.Load())) },
					TableOptions: func(TableID) TableOptions { return TableOptions{GCGraceSeconds: grace} }})
				if err != nil {
					b.Fatal(err)
				}
				tbl, err := e.CreateTable(1)
				if err != nil {
					b.Fatal(err)
				}
				put = func(k []byte) error { return e.Put(1, k, value(), NoSync) }
				del = func(k []byte) error {
					tomb := row.Append(nil, &row.Row{
						Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
						Cells:    []row.Cell{{Column: 1, Timestamp: ts.Add(1), LocalDeletionTime: time.Now().Unix()}},
					})
					return e.Put(1, k, tomb, NoSync)
				}
				wait = func() {
					if err := e.Flush(); err != nil {
						b.Fatal(err)
					}
					if err := e.WaitForCompactions(); err != nil {
						b.Fatal(err)
					}
				}
				tableBytes = func() float64 {
					var n uint64
					e.mu.Lock()
					for _, f := range tbl.files {
						n += f.size
					}
					e.mu.Unlock()
					return float64(n)
				}
				major = func() error {
					skew.Store(int64(time.Hour))
					return tbl.Compact()
				}
				closeFn = e.Close
			default:
				c := pebble.NewCache(64 << 20)
				defer c.Unref()
				opts := &pebble.Options{FS: vfs.NewMem(), Cache: c, MemTableSize: 4 << 20}
				for i := range opts.Levels {
					opts.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) }
				}
				db, err := pebble.Open("db", opts)
				if err != nil {
					b.Fatal(err)
				}
				put = func(k []byte) error { return db.Set(k, value(), pebble.NoSync) }
				del = func(k []byte) error { return db.Delete(k, pebble.NoSync) }
				wait = func() {
					if err := db.Flush(); err != nil {
						b.Fatal(err)
					}
					waitPebble(b, db)
				}
				tableBytes = func() float64 { return float64(db.Metrics().Total().Tables.Bytes) }
				major = func() error {
					return db.Compact(context.Background(), []byte{0}, bytes.Repeat([]byte{0xff}, 32), true)
				}
				closeFn = db.Close
			}
			defer closeFn()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := tokenBenchKey((i * 7919) % 200000)
				var err error
				if i%3 == 2 {
					err = del(k)
				} else {
					err = put(k)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			wait()
			b.StopTimer()
			b.ReportMetric(tableBytes()/(1<<20), "MiB-before-major")
			if err := major(); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(tableBytes()/(1<<20), "MiB-left")
		})
	}
}

// waitPebble waits until Pebble has no compaction running or owed, so both
// engines are measured after the same work. With keys spread over the key
// space Pebble can report a debt it never schedules a compaction for, so it
// is also done once it has started none for 500 ms; the timer is paused
// while it is idle, so that wait is not measured.
func waitPebble(b *testing.B, db *pebble.DB) {
	idleSince, lastCount, paused := time.Now(), int64(-1), false
	for {
		m := db.Metrics()
		if m.Compact.NumInProgress == 0 && m.Compact.EstimatedDebt == 0 {
			break
		}
		if m.Compact.NumInProgress > 0 || m.Compact.Count != lastCount {
			idleSince, lastCount = time.Now(), m.Compact.Count
			if paused {
				b.StartTimer()
				paused = false
			}
		} else if !paused {
			b.StopTimer()
			paused = true
		} else if time.Since(idleSince) > 500*time.Millisecond {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if paused {
		b.StartTimer()
	}
}

// BenchmarkStreamRange moves a table of 100k token-spread rows to a second
// store, as streaming does for repair or bootstrap, and reports time per
// row. The engine exports the rows to sstables (ExportRange) and imports
// them (Import: validated, merged and rewritten into shards). Pebble writes
// an external sstable from an iterator and ingests it (DB.Ingest, which
// links the file without rewriting it).
func BenchmarkStreamRange(b *testing.B) {
	const rows = 100000
	val := benchValue()
	for _, eng := range []string{"lsm", "pebble"} {
		b.Run(eng, func(b *testing.B) {
			fs := vfs.NewMem()
			switch eng {
			case "lsm":
				open := func(dir string) (*Engine, *Table) {
					e, err := Open(dir, &Options{FS: fs, DisableCompactions: true})
					if err != nil {
						b.Fatal(err)
					}
					tbl, err := e.CreateTable(1)
					if err != nil {
						b.Fatal(err)
					}
					return e, tbl
				}
				src, st := open("src")
				defer src.Close()
				for i := 0; i < rows; i++ {
					if err := src.Put(1, tokenBenchKey(i), val, NoSync); err != nil {
						b.Fatal(err)
					}
				}
				if err := st.Flush(); err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					dst, dt := open(fmt.Sprintf("dst%d", n))
					dir := fmt.Sprintf("stream%d", n)
					if err := fs.MkdirAll(dir, 0755); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					paths, err := st.ExportRange(nil, nil, fs, dir)
					if err != nil {
						b.Fatal(err)
					}
					if err := dt.Import(fs, paths); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					dst.Close()
					b.StartTimer()
				}
			default:
				c := pebble.NewCache(64 << 20)
				defer c.Unref()
				open := func(dir string) *pebble.DB {
					opts := &pebble.Options{FS: fs, Cache: c, DisableAutomaticCompactions: true}
					for i := range opts.Levels {
						opts.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) }
					}
					db, err := pebble.Open(dir, opts)
					if err != nil {
						b.Fatal(err)
					}
					return db
				}
				src := open("src")
				defer src.Close()
				for i := 0; i < rows; i++ {
					if err := src.Set(tokenBenchKey(i), val, pebble.NoSync); err != nil {
						b.Fatal(err)
					}
				}
				if err := src.Flush(); err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					dst := open(fmt.Sprintf("dst%d", n))
					path := fmt.Sprintf("stream%d.sst", n)
					b.StartTimer()
					f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
					if err != nil {
						b.Fatal(err)
					}
					w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
						TableFormat:  dst.TableFormat(),
						FilterPolicy: bloom.FilterPolicy(10),
					})
					it, err := src.NewIter(nil)
					if err != nil {
						b.Fatal(err)
					}
					for ok := it.First(); ok; ok = it.Next() {
						if err := w.Set(it.Key(), it.Value()); err != nil {
							b.Fatal(err)
						}
					}
					if err := it.Close(); err != nil {
						b.Fatal(err)
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
					if err := dst.Ingest(context.Background(), []string{path}); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					dst.Close()
					b.StartTimer()
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
		})
	}
}

// BenchmarkPutParallelDisk is the synced 4-writer benchmark on a real disk
// (the test's temporary directory) instead of the in-memory file system, so
// fsync and file growth cost what they do in production.
func BenchmarkPutParallelDisk(b *testing.B) {
	val := benchValue()
	for _, eng := range engines {
		b.Run(eng+"/sync=true", func(b *testing.B) {
			dir := b.TempDir()
			var put func(k []byte) error
			var closeFn func() error
			if eng == "lsm" {
				e, err := Open(dir, &Options{FS: vfs.Default, DisableCompactions: true})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := e.CreateTable(1); err != nil {
					b.Fatal(err)
				}
				put = func(k []byte) error { return e.Put(1, k, val, Sync) }
				closeFn = e.Close
			} else {
				c := pebble.NewCache(64 << 20)
				defer c.Unref()
				db, err := pebble.Open(dir, &pebble.Options{FS: vfs.Default, Cache: c, DisableAutomaticCompactions: true})
				if err != nil {
					b.Fatal(err)
				}
				put = func(k []byte) error { return db.Set(k, val, pebble.Sync) }
				closeFn = db.Close
			}
			defer closeFn()
			var n atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := put(benchKey(int(n.Add(1)))); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// dirBytes sums the sizes of the files under dir (one level of
// subdirectories, as the engine's tables directory has).
func dirBytes(fs vfs.FS, dir string) int64 {
	var n int64
	var walk func(d string, depth int)
	walk = func(d string, depth int) {
		names, err := fs.List(d)
		if err != nil {
			return
		}
		for _, name := range names {
			p := fs.PathJoin(d, name)
			st, err := fs.Stat(p)
			if err != nil {
				continue
			}
			if st.IsDir() {
				if depth < 2 {
					walk(p, depth+1)
				}
				continue
			}
			if strings.HasSuffix(name, ".sst") {
				n += st.Size()
			}
		}
	}
	walk(dir, 0)
	return n
}

// BenchmarkMajorCompactionSpace overwrites a 200k-row token-spread table in
// 8 rounds, one flush each, then compacts everything at once, sampling the
// sstable bytes on disk every millisecond. It reports the peak over the
// data before compaction: how much extra space a full compaction needs.
// "lsm" writes each shard's output whole (the inputs go when it ends);
// "lsm-fragments" cuts output into 1 MiB fragments and releases inputs as it
// goes (ScyllaDB ICS's incremental compaction); Pebble compacts manually.
func BenchmarkMajorCompactionSpace(b *testing.B) {
	for _, eng := range []string{"lsm", "lsm-fragments", "pebble"} {
		b.Run(eng, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				fs := vfs.NewMem()
				var put func(k, v []byte) error
				var flush, compact, closeFn func() error
				switch eng {
				case "lsm", "lsm-fragments":
					frag := int64(-1)
					if eng == "lsm-fragments" {
						frag = 1 << 20
					}
					e, err := Open("db", &Options{FS: fs, DisableCompactions: true,
						TableOptions: func(TableID) TableOptions { return TableOptions{FragmentSize: frag} }})
					if err != nil {
						b.Fatal(err)
					}
					tbl, err := e.CreateTable(1)
					if err != nil {
						b.Fatal(err)
					}
					put = func(k, v []byte) error { return e.Put(1, k, v, NoSync) }
					flush, compact, closeFn = tbl.Flush, tbl.Compact, e.Close
				default:
					db, err := pebble.Open("db", &pebble.Options{FS: fs, DisableAutomaticCompactions: true})
					if err != nil {
						b.Fatal(err)
					}
					put = func(k, v []byte) error { return db.Set(k, v, pebble.NoSync) }
					flush = db.Flush
					compact = func() error {
						return db.Compact(context.Background(), []byte{0}, bytes.Repeat([]byte{0xff}, 32), false)
					}
					closeFn = db.Close
				}
				for round := 0; round < 8; round++ {
					for i := 0; i < 200000; i++ {
						v := row.Append(nil, &row.Row{
							Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
							Cells: []row.Cell{{Column: 1, Timestamp: int64(round*200000 + i), LocalDeletionTime: row.NoDeletionTime,
								Value: bytes.Repeat([]byte{byte(i), byte(round)}, 50)}},
						})
						if err := put(tokenBenchKey(i), v); err != nil {
							b.Fatal(err)
						}
					}
					if err := flush(); err != nil {
						b.Fatal(err)
					}
				}
				before := dirBytes(fs, "db")
				var peak atomic.Int64
				peak.Store(before)
				stop := make(chan struct{})
				sampled := make(chan struct{})
				go func() {
					defer close(sampled)
					for {
						select {
						case <-stop:
							return
						case <-time.After(time.Millisecond):
							if v := dirBytes(fs, "db"); v > peak.Load() {
								peak.Store(v)
							}
						}
					}
				}()
				b.StartTimer()
				if err := compact(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				close(stop)
				<-sampled
				after := dirBytes(fs, "db")
				b.ReportMetric(float64(before)/(1<<20), "MiB-before")
				b.ReportMetric(float64(peak.Load())/(1<<20), "MiB-peak")
				b.ReportMetric(float64(after)/(1<<20), "MiB-after")
				b.ReportMetric(float64(peak.Load()-before)/(1<<20), "MiB-extra")
				if err := closeFn(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
