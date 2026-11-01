package lsm

import (
	"bytes"
	"context"
	"io"
	"math"

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
	n            int
}

func newSSTBuilder(fs vfs.FS, dir string, num uint64, wo sstable.WriterOptions, merger Merger, computeTS bool) (*sstBuilder, error) {
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
	}, nil
}

// add writes one row (all of the key's fragments merged) as a MERGE entry:
// other sstables may hold other fragments of the row.
func (b *sstBuilder) add(key, value []byte, trailer uint64) error {
	if b.computeTS {
		lo, hi, has, err := b.merger.Timestamps(value)
		if err != nil {
			return err
		}
		if has {
			b.minTS, b.maxTS = min(b.minTS, lo), max(b.maxTS, hi)
		}
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
func writeTable(fs vfs.FS, dir string, alloc func() uint64, wo sstable.WriterOptions, merger Merger, mem *memTable, boundaries [][]byte) ([]*fileMeta, error) {
	it := memIter{merger: merger, n: mem.first()}
	var out []*fileMeta
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
	next := 0 // the next boundary
	for {
		key, value, trailer, ok, err := it.next()
		if err != nil {
			return fail(err)
		}
		if !ok {
			break
		}
		crossed := false
		for next < len(boundaries) && bytes.Compare(key, boundaries[next]) >= 0 {
			next++
			crossed = true
		}
		if crossed && b != nil {
			meta, err := b.finish()
			b = nil
			if err != nil {
				return fail(err)
			}
			out = append(out, meta)
		}
		if b == nil {
			if b, err = newSSTBuilder(fs, dir, alloc(), wo, merger, true); err != nil {
				return fail(err)
			}
		}
		if err := b.add(key, value, trailer); err != nil {
			return fail(err)
		}
	}
	if b != nil {
		meta, err := b.finish()
		b = nil
		if err != nil {
			return fail(err)
		}
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
