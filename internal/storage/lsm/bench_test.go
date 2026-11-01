package lsm

// Benchmarks comparing the engine with Pebble (pinned in go.mod) on the same
// workload: the same keys and the same encoded row values (one 100-byte cell).
// Run with: go test -run x -bench . ./internal/storage/lsm/

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
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
					// Wait until Pebble has no compaction running or owed, so
					// both engines are measured after the same work. With
					// keys spread over the key space Pebble can report a
					// debt it never schedules a compaction for, so it is
					// also done once it has started none for 500 ms.
					// The timer is paused while Pebble is idle, so that
					// wait is not measured.
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
