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

// writeTable writes the memtable to sstable num, one entry per key: the merge
// of the key's versions, as a MERGE entry (other sstables may hold other
// fragments of the row). It returns nil if the memtable is empty.
func writeTable(fs vfs.FS, dir string, num uint64, wo sstable.WriterOptions, merger Merger, mem *memTable) (*fileMeta, error) {
	it := memIter{merger: merger, n: mem.first()}
	if it.n == nil {
		return nil, nil
	}
	f, err := fs.Create(fs.PathJoin(dir, tableFileName(num)), vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), wo)
	minTS, maxTS := int64(math.MaxInt64), int64(math.MinInt64)
	for {
		key, value, trailer, ok, err := it.next()
		if err == nil && ok {
			var lo, hi int64
			var has bool
			if lo, hi, has, err = merger.Timestamps(value); has {
				minTS, maxTS = min(minTS, lo), max(maxTS, hi)
			}
		}
		if err == nil && ok {
			k := pebble.InternalKey{
				UserKey: key,
				Trailer: pebble.InternalKeyTrailer(trailer&^0xff | uint64(pebble.InternalKeyKindMerge)),
			}
			err = w.Add(k, value, false, sstable.KVMeta{})
		}
		if err != nil {
			w.Close()
			return nil, err
		}
		if !ok {
			break
		}
	}
	// Close finishes the table: it syncs and closes the file.
	if err := w.Close(); err != nil {
		return nil, err
	}
	md, err := w.Metadata()
	if err != nil {
		return nil, err
	}
	return &fileMeta{
		num:      num,
		size:     md.Size,
		smallest: md.SmallestPoint.Clone(),
		largest:  md.LargestPoint.Clone(),
		minSeq:   uint64(md.SeqNums.Low),
		maxSeq:   uint64(md.SeqNums.High),
		minTS:    minTS,
		maxTS:    maxTS,
	}, nil
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
