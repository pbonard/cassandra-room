package lsm

import (
	"bytes"
	"context"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"io"
	"math"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

func makeWriterOptions(o *Options) sstable.WriterOptions {
	return sstable.WriterOptions{
		TableFormat:  tableFormat,
		BlockSize:    o.BlockSize,
		FilterPolicy: bloom.FilterPolicy(o.BloomBitsPerKey),
		// Value blocks only pay off for MVCC-style older versions; without
		// them the reader needs no value-block provider.
		DisableValueBlocks: true,
	}
}

func makeReaderOptions() sstable.ReaderOptions {
	var o sstable.ReaderOptions
	o.FilterDecoders = append(o.FilterDecoders, bloom.Decoder)
	return o
}

// setFileNum stores num into a field of Pebble's DiskFileNum type, which the
// public API uses but does not export.
func setFileNum[T ~uint64](dst *T, num uint64) { *dst = T(num) }

// newLogWriter and newLogReader create WAL writers and readers for segment
// num; the segment number is part of every record's header.
func newLogWriter(f vfs.File, num uint64, cfg record.LogWriterConfig) *record.LogWriter {
	var o sstable.ReaderOptions
	setFileNum(&o.CacheOpts.FileNum, num)
	return record.NewLogWriter(f, o.CacheOpts.FileNum, cfg)
}

func newLogReader(r io.Reader, num uint64) *record.Reader {
	var o sstable.ReaderOptions
	setFileNum(&o.CacheOpts.FileNum, num)
	return record.NewReader(r, o.CacheOpts.FileNum)
}

// memIter iterates a memtable's keys, merging each key's versions.
type memIter struct {
	merger Merger
	n      *memNode
}

// next returns the next key, its merged value, and the trailer of its newest
// version, or ok=false at the end.
func (it *memIter) next() (key, value []byte, trailer uint64, ok bool, err error) {
	if it.n == nil {
		return nil, nil, 0, false, nil
	}
	key, trailer, value = it.n.key, it.n.trailer, it.n.value
	for it.n = it.n.next(); it.n != nil && bytes.Equal(it.n.key, key); it.n = it.n.next() {
		if value, err = it.merger.Merge(nil, value, it.n.value); err != nil {
			return nil, nil, 0, false, err
		}
	}
	return key, value, trailer, true, nil
}

// sstBuilder writes one sstable.
type sstBuilder struct {
	fs   vfs.FS
	path string
	num  uint64
	w    sstable.RawWriter
	// With computeTS, the file's timestamp range is decoded from its rows;
	// otherwise the caller supplies it.
	computeTS    bool
	merger       Merger
	minTS, maxTS int64
	// stats is computed from the rows with computeTS and a Purger merger,
	// and otherwise supplied by the caller.
	stats fileStats
	n     int
}

func newSSTBuilder(fs vfs.FS, dir string, num uint64, wo sstable.WriterOptions, merger Merger, computeTS bool, created int64) (*sstBuilder, error) {
	path := fs.PathJoin(dir, tableFileName(num))
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	return &sstBuilder{
		fs: fs, path: path, num: num,
		w:         sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), wo),
		computeTS: computeTS, merger: merger,
		minTS: math.MaxInt64, maxTS: math.MinInt64,
		stats: newFileStats(created),
	}, nil
}

// add writes one row (all of the key's fragments merged) as a MERGE entry:
// other sstables may hold other fragments of the row.
func (b *sstBuilder) add(key, value []byte, trailer uint64) error {
	if b.computeTS {
		if p, ok := b.merger.(Purger); ok {
			rs, err := p.Summarize(value, func(ldt int64) { b.stats.hist.add(ldt, 1) })
			if err != nil {
				return err
			}
			b.stats.add(&rs)
			if rs.HasTS {
				b.minTS, b.maxTS = min(b.minTS, rs.MinTS), max(b.maxTS, rs.MaxTS)
			}
		} else {
			b.stats.valid = false
			lo, hi, has, err := b.merger.Timestamps(value)
			if err != nil {
				return err
			}
			if has {
				b.minTS, b.maxTS = min(b.minTS, lo), max(b.maxTS, hi)
			}
		}
	}
	b.n++
	k := pebble.InternalKey{
		UserKey: key,
		Trailer: pebble.InternalKeyTrailer(trailer&^0xff | uint64(pebble.InternalKeyKindMerge)),
	}
	return b.w.Add(k, value, false, sstable.KVMeta{})
}

// addSummarized adds a row whose statistics the caller computed (with a
// Purger), so the row is not decoded again.
func (b *sstBuilder) addSummarized(key, value []byte, trailer uint64, rs *row.Stats) error {
	b.stats.add(rs)
	if rs.HasTS {
		b.minTS, b.maxTS = min(b.minTS, rs.MinTS), max(b.maxTS, rs.MaxTS)
	}
	b.n++
	k := pebble.InternalKey{
		UserKey: key,
		Trailer: pebble.InternalKeyTrailer(trailer&^0xff | uint64(pebble.InternalKeyKindMerge)),
	}
	return b.w.Add(k, value, false, sstable.KVMeta{})
}

// finish closes the file (which syncs it) and returns its metadata, or nil
// (and removes the file) if no row was added.
func (b *sstBuilder) finish() (*fileMeta, error) {
	if err := b.w.Close(); err != nil {
		_ = b.fs.Remove(b.path)
		return nil, err
	}
	if b.n == 0 {
		return nil, b.fs.Remove(b.path)
	}
	md, err := b.w.Metadata()
	if err != nil {
		_ = b.fs.Remove(b.path)
		return nil, err
	}
	return &fileMeta{
		num:      b.num,
		size:     md.Size,
		smallest: md.SmallestPoint.Clone(),
		largest:  md.LargestPoint.Clone(),
		minSeq:   uint64(md.SeqNums.Low),
		maxSeq:   uint64(md.SeqNums.High),
		minTS:    b.minTS,
		maxTS:    b.maxTS,
		stats:    b.stats,
	}, nil
}

// abort closes and removes the file.
func (b *sstBuilder) abort() {
	_ = b.w.Close()
	_ = b.fs.Remove(b.path)
}

// writeTable writes the memtable to sstables, one entry per key: the merge of
// the key's versions. A new sstable starts at each shard boundary (sorted
// keys; see shardBoundaries), so no sstable crosses one. alloc returns file
// numbers. It returns no files if the memtable is empty; on error it removes
// what it wrote.
//
// With split (the time-window mode), rows are instead split by time window
// and each window's parts go to their own sstables (writeByWindow).
func writeTable(fs vfs.FS, dir string, alloc func() uint64, wo sstable.WriterOptions, merger Merger, mem *memTable, boundaries [][]byte, fragment int64, split func([]byte) ([]row.Part, error), created int64) ([]*fileMeta, error) {
	it := memIter{merger: merger, n: mem.first()}
	if split != nil {
		return writeByWindow(fs, dir, alloc, wo, merger, it.next, split, fragment, created)
	}
	return writeSharded(fs, dir, alloc, wo, merger, it.next, boundaries, fragment, created)
}

// writeByWindow writes the rows next returns, in key order, split by time
// window, to one run of sstables per window (ScyllaDB's
// segregate_by_timestamp), each cut into fragments of about fragment bytes
// at partition boundaries. Late or out-of-order writes thus never share an
// sstable with another window's data, so each window's sstables expire
// together. At most maxSegregatedWindows windows get their own run; the
// parts of older windows beyond that share the run of the oldest one kept.
func writeByWindow(fs vfs.FS, dir string, alloc func() uint64, wo sstable.WriterOptions, merger Merger,
	next func() (key, value []byte, trailer uint64, ok bool, err error), split func([]byte) ([]row.Part, error), fragment int64, created int64) ([]*fileMeta, error) {
	type chain struct {
		b       *sstBuilder
		lastKey []byte
		run     uint64
	}
	chains := map[int64]*chain{}
	var out []*fileMeta
	fail := func(err error) ([]*fileMeta, error) {
		for _, c := range chains {
			if c.b != nil {
				c.b.abort()
			}
		}
		for _, f := range out {
			_ = fs.Remove(fs.PathJoin(dir, tableFileName(f.num)))
		}
		return nil, err
	}
	finish := func(c *chain) error {
		meta, err := c.b.finish()
		c.b = nil
		if err != nil {
			return err
		}
		meta.run = c.run
		out = append(out, meta)
		return nil
	}
	for {
		key, value, trailer, ok, err := next()
		if err != nil {
			return fail(err)
		}
		if !ok {
			break
		}
		parts, err := split(value)
		if err != nil {
			return fail(err)
		}
		for _, p := range parts {
			w := p.Bucket
			if chains[w] == nil && len(chains) >= maxSegregatedWindows {
				// Too many windows: share the oldest kept window's run.
				oldest := int64(math.MaxInt64)
				for k := range chains {
					oldest = min(oldest, k)
				}
				if w > oldest {
					// Newer than the oldest kept: replace nothing, co-locate
					// with the newest window not after it.
					best := oldest
					for k := range chains {
						if k <= w && k > best {
							best = k
						}
					}
					w = best
				} else {
					w = oldest
				}
			}
			c := chains[w]
			if c == nil {
				c = &chain{}
				chains[w] = c
			}
			if c.b != nil && fragment > 0 && c.b.w.EstimatedSize() >= uint64(fragment) && keyToken(key) != keyToken(c.lastKey) {
				if err := finish(c); err != nil {
					return fail(err)
				}
			}
			if c.b == nil {
				num := alloc()
				if c.run == 0 {
					c.run = num
				}
				if c.b, err = newSSTBuilder(fs, dir, num, wo, merger, true, created); err != nil {
					return fail(err)
				}
			}
			if err := c.b.add(key, p.Value, trailer); err != nil {
				return fail(err)
			}
			c.lastKey = append(c.lastKey[:0], key...)
		}
	}
	ws := make([]int64, 0, len(chains))
	for w := range chains {
		ws = append(ws, w)
	}
	slices.Sort(ws)
	for _, w := range ws {
		if c := chains[w]; c.b != nil {
			if err := finish(c); err != nil {
				return fail(err)
			}
		}
	}
	return out, nil
}

// maxSegregatedWindows bounds the windows one flush or compaction writes
// separate sstables for (ScyllaDB's max_data_segregation_window_count).
const maxSegregatedWindows = 100

// writeSharded writes the rows next returns, in key order, to sstables, a
// new one starting at each shard boundary and, with a positive fragment
// size, at the first partition boundary past that size (so later
// compactions can release the output piece by piece). It returns their
// metadata; on error it removes what it wrote.
func writeSharded(fs vfs.FS, dir string, alloc func() uint64, wo sstable.WriterOptions, merger Merger,
	next func() (key, value []byte, trailer uint64, ok bool, err error), boundaries [][]byte, fragment int64, created int64) ([]*fileMeta, error) {
	var out []*fileMeta
	var lastKey []byte
	var b *sstBuilder
	fail := func(err error) ([]*fileMeta, error) {
		if b != nil {
			b.abort()
		}
		for _, f := range out {
			_ = fs.Remove(fs.PathJoin(dir, tableFileName(f.num)))
		}
		return nil, err
	}
	nextBound := 0
	for {
		key, value, trailer, ok, err := next()
		if err != nil {
			return fail(err)
		}
		if !ok {
			break
		}
		crossed := false
		for nextBound < len(boundaries) && bytes.Compare(key, boundaries[nextBound]) >= 0 {
			nextBound++
			crossed = true
		}
		if !crossed && b != nil && fragment > 0 && b.w.EstimatedSize() >= uint64(fragment) && keyToken(key) != keyToken(lastKey) {
			crossed = true
		}
		if crossed && b != nil {
			meta, err := b.finish()
			b = nil
			if err != nil {
				return fail(err)
			}
			meta.run = out0(out, meta)
			out = append(out, meta)
		}
		if b == nil {
			if b, err = newSSTBuilder(fs, dir, alloc(), wo, merger, true, created); err != nil {
				return fail(err)
			}
		}
		if err := b.add(key, value, trailer); err != nil {
			return fail(err)
		}
		lastKey = append(lastKey[:0], key...)
	}
	if b != nil {
		meta, err := b.finish()
		b = nil
		if err != nil {
			return fail(err)
		}
		meta.run = out0(out, meta)
		out = append(out, meta)
	}
	return out, nil
}

// tableReader is an open sstable.
type tableReader struct {
	r *sstable.Reader
}

func openTable(fs vfs.FS, dir string, num uint64, ro sstable.ReaderOptions) (*tableReader, error) {
	path := fs.PathJoin(dir, tableFileName(num))
	f, err := fs.Open(path, vfs.RandomReadsOption)
	if err != nil {
		return nil, err
	}
	readable, err := objstorageprovider.NewFileReadable(f, fs, objstorageprovider.NewReadaheadConfig(), path)
	if err != nil {
		f.Close()
		return nil, err
	}
	// Blocks are cached under the file number, which is unique per engine.
	setFileNum(&ro.CacheOpts.FileNum, num)
	r, err := sstable.NewReader(context.Background(), readable, ro)
	if err != nil {
		readable.Close()
		return nil, err
	}
	return &tableReader{r: r}, nil
}

func (t *tableReader) close() error { return t.r.Close() }

// getMerged merges the table's entry for key, if any, into acc (see
// mergeInto), after checking the Bloom filter. The entry is merged while the
// iterator still pins its block, so the result never aliases cache memory.
func (t *tableReader) getMerged(merger Merger, key, acc []byte) (_ []byte, found bool, err error) {
	it, err := t.r.NewPointIter(context.Background(), sstable.IterOptions{
		FilterBlockSizeLimit: sstable.AlwaysUseFilterBlock,
		Env:                  sstable.NoReadEnv,
		ReaderProvider:       sstable.MakeTrivialReaderProvider(t.r),
	})
	if err != nil {
		return nil, false, err
	}
	if kv := it.SeekPrefixGE(key, key, 0); kv != nil && bytes.Equal(kv.K.UserKey, key) {
		v, _, verr := kv.Value(nil)
		if verr == nil {
			acc, verr = mergeInto(merger, acc, v)
		}
		if verr != nil {
			err = verr
		} else {
			found = true
		}
	}
	if err == nil {
		err = it.Error()
	}
	// Close returns the iterator to a pool; it must not be used afterwards.
	if cerr := it.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, false, err
	}
	return acc, found, nil
}

// mayContain reports whether key is inside the table's key range.
func (f *fileMeta) mayContain(key []byte) bool {
	return bytes.Compare(key, f.smallest.UserKey) >= 0 && bytes.Compare(key, f.largest.UserKey) <= 0
}

// out0 returns the run of a writer's output: the number of its first file.
func out0(out []*fileMeta, meta *fileMeta) uint64 {
	if len(out) > 0 {
		return out[0].num
	}
	return meta.num
}
