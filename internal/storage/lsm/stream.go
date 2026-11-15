package lsm

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

// ExportRange writes the table's data in [lower, upper) (nil bounds are
// open) to sstables in dir on fs and returns their paths: the sending side
// of Cassandra's streaming, used by repair, bootstrap and decommission to
// move token ranges between nodes. As Cassandra does
// (StreamSession.addTransferRanges, then CassandraOutgoingFile), the table
// is flushed first, so what is sent is durable at the sender, and its
// sstables are sent as they are rather than merged: an sstable wholly
// inside the range is hard-linked into dir (copied, if fs is not the
// engine's file system), as Cassandra's entire-sstable streaming
// (CASSANDRA-14556) sends whole files; from one that crosses a bound, the
// section inside the range is written to a new file, as Cassandra streams
// sstable sections. The files hold row fragments, which the receiver
// reconciles by timestamp like any others. Writes that arrive after the
// flush are not sent. dir must exist; existing files are not overwritten.
func (t *Table) ExportRange(lower, upper []byte, fs vfs.FS, dir string) ([]string, error) {
	if err := t.Flush(); err != nil {
		return nil, err
	}
	if t.dropped.Load() {
		return nil, ErrUnknownTable
	}
	e := t.e
	s := t.acquireState()
	defer s.release()
	files := slices.Clone(s.files)
	slices.SortFunc(files, func(a, b *fileMeta) int { return bytes.Compare(a.smallest.UserKey, b.smallest.UserKey) })

	var paths []string
	fail := func(err error) ([]string, error) {
		for _, p := range paths {
			_ = fs.Remove(p)
		}
		return nil, err
	}
	num := uint64(1)
	nextPath := func() string {
		for {
			p := fs.PathJoin(dir, tableFileName(num))
			num++
			if _, err := fs.Stat(p); err != nil {
				return p
			}
		}
	}
	created := e.opts.Now().Unix()
	for _, f := range files {
		if (upper != nil && bytes.Compare(f.smallest.UserKey, upper) >= 0) ||
			(lower != nil && bytes.Compare(f.largest.UserKey, lower) < 0) {
			continue // no overlap
		}
		inside := (lower == nil || bytes.Compare(f.smallest.UserKey, lower) >= 0) &&
			(upper == nil || bytes.Compare(f.largest.UserKey, upper) < 0)
		dst := nextPath()
		if inside {
			var err error
			if fs == e.fs {
				err = fs.Link(f.path, dst)
			} else {
				err = vfs.CopyAcrossFS(e.fs, f.path, fs, dst)
			}
			if err != nil {
				return fail(err)
			}
			paths = append(paths, dst)
			continue
		}
		// The section of f inside the range.
		var src tableSource
		if err := src.init(f.reader, lower, upper, false); err != nil {
			return fail(err)
		}
		var dstNum uint64
		_, _ = fmt.Sscanf(fs.PathBase(dst), "%d.sst", &dstNum)
		b, err := newSSTBuilder(fs, dir, dstNum, e.writerOpts, e.merger, false, created)
		if err != nil {
			src.close()
			return fail(err)
		}
		k, v, tr, ok := src.first()
		if lower != nil {
			k, v, tr, ok = src.seekGE(lower)
		}
		for ; ok; k, v, tr, ok = src.next() {
			if err := b.add(k, v, tr); err != nil {
				b.abort()
				src.close()
				return fail(err)
			}
		}
		err = src.err()
		src.close()
		if err != nil {
			b.abort()
			return fail(err)
		}
		meta, err := b.finish()
		if err != nil {
			return fail(err)
		}
		if meta != nil {
			paths = append(paths, fs.PathJoin(dir, tableFileName(meta.num)))
		}
	}
	if err := syncDir(fs, dir); err != nil {
		return fail(err)
	}
	return paths, nil
}

// externalTable is an sstable outside the engine, opened for import.
type externalTable struct {
	path    string
	r       *sstable.Reader
	size    uint64
	isEmpty bool
	// meta describes the file as an sstable of the table (without a
	// number), from validate.
	meta fileMeta
}

func openExternal(fs vfs.FS, path string) (*externalTable, error) {
	f, err := fs.Open(path, vfs.SequentialReadsOption)
	if err != nil {
		return nil, err
	}
	readable, err := objstorageprovider.NewFileReadable(f, fs, objstorageprovider.NewReadaheadConfig(), path)
	if err != nil {
		f.Close()
		return nil, err
	}
	r, err := sstable.NewReader(context.Background(), readable, makeReaderOptions())
	if err != nil {
		readable.Close()
		return nil, fmt.Errorf("lsm: importing %s: %w", path, err)
	}
	st, err := fs.Stat(path)
	if err != nil {
		r.Close()
		return nil, err
	}
	return &externalTable{path: path, r: r, size: uint64(st.Size())}, nil
}

// validate reads the whole table: keys strictly increasing (one entry per
// key, as the engine's sstables hold), only SET and MERGE entries, and
// values the Merger decodes. It fills in meta: key range, sequence numbers,
// timestamps and, with a Purger, the summary.
func (x *externalTable) validate(merger Merger, created int64) error {
	var bp block.BufferPool
	bp.Init(4, block.ForCompaction)
	defer bp.Release()
	src, err := newCompactionSource(&tableReader{r: x.r}, &bp)
	if err != nil {
		return err
	}
	defer src.close()
	purger, _ := merger.(Purger)
	m := &x.meta
	m.size = x.size
	m.minTS, m.maxTS = math.MaxInt64, math.MinInt64
	m.minSeq = math.MaxUint64
	if purger != nil {
		m.stats = newFileStats(created)
	}
	var prev []byte
	var last uint64
	n := 0
	for k, v, tr, ok := src.first(); ok; k, v, tr, ok = src.next() {
		switch pebble.InternalKeyKind(tr & 0xff) {
		case pebble.InternalKeyKindMerge, pebble.InternalKeyKindSet:
		default:
			return fmt.Errorf("lsm: import: key %q has kind %v", k, pebble.InternalKeyKind(tr&0xff))
		}
		if n > 0 && bytes.Compare(prev, k) >= 0 {
			return fmt.Errorf("lsm: import: keys out of order at %q", k)
		}
		var lo, hi int64
		var has bool
		if purger != nil {
			rs, err := purger.Summarize(v, func(ldt int64) { m.stats.hist.add(ldt, 1) })
			if err != nil {
				return fmt.Errorf("lsm: import: key %q: %w", k, err)
			}
			m.stats.add(&rs)
			lo, hi, has = rs.MinTS, rs.MaxTS, rs.HasTS
		} else if lo, hi, has, err = merger.Timestamps(v); err != nil {
			return fmt.Errorf("lsm: import: key %q: %w", k, err)
		}
		if has {
			m.minTS, m.maxTS = min(m.minTS, lo), max(m.maxTS, hi)
		}
		seq := tr >> 8
		m.minSeq, m.maxSeq = min(m.minSeq, seq), max(m.maxSeq, seq)
		if n == 0 {
			m.smallest = pebble.InternalKey{UserKey: slices.Clone(k), Trailer: pebble.InternalKeyTrailer(tr)}
		}
		prev, last = append(prev[:0], k...), tr
		n++
	}
	if err := src.err(); err != nil {
		return err
	}
	m.largest = pebble.InternalKey{UserKey: slices.Clone(prev), Trailer: pebble.InternalKeyTrailer(last)}
	x.isEmpty = n == 0
	return nil
}

// maxLinkableSeq bounds the sequence numbers of a linked file: the engine
// adopts them (recovery raises its last sequence number to the largest
// file's), so they must leave room below the 56-bit limit.
const maxLinkableSeq = 1 << 48

// Import adds the rows of external sstables to the table: the receiving
// side of streaming, and Cassandra's nodetool import (for example of a
// snapshot). Each file is first read in full and validated (keys in order,
// one entry per key, only SET and MERGE entries, values the Merger decodes),
// which also yields its summary. Files on the engine's file system are then
// hard-linked into the table unchanged, as Cassandra's zero-copy streaming
// of entire sstables does; the others are merged and rewritten into new
// sstables, split at shard boundaries. Everything is installed in one
// manifest edit, or nothing. The source files are not modified (later
// changes to a linked source would change the table: sstables are
// immutable by convention). Imported rows reconcile with the table's by
// timestamp, like any other fragment, so importing the same data twice
// changes nothing.
func (t *Table) Import(fs vfs.FS, paths []string) error {
	e := t.e
	created := e.opts.Now().Unix()
	var ext []*externalTable
	defer func() {
		for _, x := range ext {
			x.r.Close()
		}
	}()
	for _, p := range paths {
		x, err := openExternal(fs, p)
		if err != nil {
			return err
		}
		ext = append(ext, x)
		if err := x.validate(e.merger, created); err != nil {
			return fmt.Errorf("%w (%s)", err, p)
		}
	}
	alloc := func() uint64 {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.mu.nextFileNum++
		return e.mu.nextFileNum - 1
	}
	var metas []*fileMeta
	discard := func() {
		for _, m := range metas {
			if m.reader != nil {
				m.reader.close()
			}
			_ = e.fs.Remove(e.fs.PathJoin(t.dir, tableFileName(m.num)))
		}
	}
	var rewrite []*externalTable
	for _, x := range ext {
		if x.isEmpty {
			continue
		}
		if fs != e.fs || x.meta.maxSeq >= maxLinkableSeq {
			rewrite = append(rewrite, x)
			continue
		}
		m := &x.meta
		num := alloc()
		if err := e.fs.Link(x.path, e.fs.PathJoin(t.dir, tableFileName(num))); err != nil {
			// For example across devices: rewrite instead.
			rewrite = append(rewrite, x)
			continue
		}
		mp := &fileMeta{num: num, size: m.size, smallest: m.smallest, largest: m.largest,
			minSeq: m.minSeq, maxSeq: m.maxSeq, minTS: m.minTS, maxTS: m.maxTS, stats: m.stats}
		metas = append(metas, mp)
		if err := e.openFile(t, mp); err != nil {
			discard()
			return err
		}
	}
	rewritten, err := t.rewriteImported(rewrite, alloc, created)
	if err != nil {
		discard()
		return err
	}
	metas = append(metas, rewritten...)
	if len(metas) == 0 {
		return nil
	}
	if err := syncDir(e.fs, t.dir); err != nil {
		discard()
		return err
	}
	return t.installImported(metas, discard)
}

// rewriteImported merges the rows of external files and writes them into
// new sstables of the table, split at shard boundaries.
func (t *Table) rewriteImported(ext []*externalTable, alloc func() uint64, created int64) ([]*fileMeta, error) {
	e := t.e
	var size uint64
	for _, x := range ext {
		size += x.size
	}

	var bp block.BufferPool
	bp.Init(2*len(ext)+4, block.ForCompaction)
	defer bp.Release()
	mi := mergingIter{merger: e.merger}
	defer mi.close()
	for _, x := range ext {
		if x.isEmpty {
			continue
		}
		src, err := newCompactionSource(&tableReader{r: x.r}, &bp)
		if err != nil {
			return nil, err
		}
		mi.sources = append(mi.sources, src)
	}
	if len(mi.sources) == 0 {
		return nil, nil
	}
	var boundaries [][]byte
	if t.o().TimeWindow <= 0 {
		boundaries = shardBoundaries(t.o().numShards(float64(size)))
	}
	started := false
	next := func() ([]byte, []byte, uint64, bool, error) {
		var ok bool
		if !started {
			started, ok = true, mi.first()
		} else {
			ok = mi.next()
		}
		if !ok {
			return nil, nil, 0, false, mi.e
		}
		// Imported entries carry the sender's sequence numbers, which mean
		// nothing here; rows reconcile by timestamp, so use 0.
		return mi.key, mi.value, uint64(pebble.InternalKeyKindMerge), true, nil
	}
	var metas []*fileMeta
	var err error
	if split := t.timeSplitter(); split != nil {
		metas, err = writeByWindow(e.fs, t.dir, alloc, e.writerOpts, e.merger, next, split, t.o().fragmentSize(), created)
	} else {
		metas, err = writeSharded(e.fs, t.dir, alloc, e.writerOpts, e.merger, next, boundaries, t.o().fragmentSize(), created)
	}
	if err != nil {
		return nil, err
	}
	for i, m := range metas {
		if err := e.openFile(t, m); err != nil {
			for _, m := range metas[:i] {
				m.reader.close()
			}
			for _, m := range metas {
				_ = e.fs.Remove(e.fs.PathJoin(t.dir, tableFileName(m.num)))
			}
			return nil, err
		}
	}
	return metas, nil
}

// installImported records imported sstables in the manifest and publishes
// them; discard removes them on failure.
func (t *Table) installImported(metas []*fileMeta, discard func()) error {
	e := t.e
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if err := e.checkUsableLocked(); err != nil {
		e.mu.Unlock()
		discard()
		return err
	}
	if t.dropped.Load() {
		e.mu.Unlock()
		discard()
		return ErrUnknownTable
	}
	manifest, nextFileNum := e.mu.manifest, e.mu.nextFileNum
	e.mu.Unlock()
	edit := &versionEdit{tables: []*tableEdit{{id: t.id, added: metas}}, nextFileNum: nextFileNum}
	if err := manifest.write(edit); err != nil {
		discard()
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t.files = append(t.files, metas...)
	sortFilesNewestFirst(t.files)
	t.publishLocked()
	e.maybeScheduleCompactionsLocked()
	return nil
}
