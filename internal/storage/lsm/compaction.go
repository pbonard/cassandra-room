package lsm

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

// TableOptions configures a table's compaction (Cassandra's per-table
// compaction options for UnifiedCompactionStrategy).
type TableOptions struct {
	// ScalingParameters are UCS's W per level; the last one applies to all
	// further levels. W < 0 is leveled, W > 0 tiered. Defaults to {2}, which
	// is Cassandra's T4 (scaling_parameters).
	ScalingParameters []int
	// MaxSSTablesToCompact caps the inputs of one compaction. Defaults to 64
	// (max_sstables_to_compact in cassandra_latest.yaml).
	MaxSSTablesToCompact int
	// FlushSizeOverride fixes UCS's flush size m. Zero uses the observed flush
	// size, rounded up to a MiB (flush_size_override).
	FlushSizeOverride int64
}

func (o *TableOptions) ensureDefaults() {
	if len(o.ScalingParameters) == 0 {
		o.ScalingParameters = []int{2}
	}
	if o.MaxSSTablesToCompact <= 0 {
		o.MaxSSTablesToCompact = 64
	}
}

func (o *TableOptions) scalingParameter(level int) int {
	return o.ScalingParameters[min(level, len(o.ScalingParameters)-1)]
}

// fanout and threshold follow UnifiedCompactionStrategy.fanoutFromScaling-
// Parameter and thresholdFromScalingParameter.
func (o *TableOptions) fanout(level int) int {
	w := o.scalingParameter(level)
	if w < 0 {
		return 2 - w
	}
	return 2 + w
}

func (o *TableOptions) threshold(level int) int {
	if o.scalingParameter(level) < 0 {
		return 2
	}
	return o.fanout(level)
}

// flushSizeBytes is Controller.getFlushSizeBytes: the override, or the
// observed flush size rounded up to a whole MiB.
func (t *Table) flushSizeBytesLocked() float64 {
	if t.opts.FlushSizeOverride > 0 {
		return float64(t.opts.FlushSizeOverride)
	}
	return math.Ceil(t.flushSize/(1<<20)) * (1 << 20)
}

// observeFlushLocked updates the moving average of flush sizes.
func (t *Table) observeFlushLocked(size uint64) {
	if t.flushSize == 0 {
		t.flushSize = float64(size)
		return
	}
	t.flushSize = 0.8*t.flushSize + 0.2*float64(size)
}

// density is an sstable's size divided by the fraction of the table's key
// space it covers. With one shard per table (M3) every sstable is treated as
// covering the whole space, so density is size.
func density(f *fileMeta) float64 { return float64(f.size) }

// ucsLevel is a band of densities, as in UnifiedCompactionStrategy.Level.
type ucsLevel struct {
	index    int
	min, max float64
	files    []*fileMeta
}

// formLevels follows UnifiedCompactionStrategy.formLevels: files sorted by
// density fall into consecutive bands, the first ending at base·F and each
// next one F times higher.
func (t *Table) formLevelsLocked(files []*fileMeta) []*ucsLevel {
	slices.SortFunc(files, func(a, b *fileMeta) int { return cmp.Compare(density(a), density(b)) })
	f0 := float64(t.opts.fanout(0))
	base := max(1<<20, t.flushSizeBytesLocked()) * (1 - 0.9/f0)
	maxDensity := math.Floor(base * f0)
	levels := []*ucsLevel{}
	level := &ucsLevel{index: 0, min: 0, max: maxDensity}
	for _, f := range files {
		d := density(f)
		for d >= level.max {
			levels = append(levels, level)
			next := level.index + 1
			minDensity := level.max
			level = &ucsLevel{
				index: next, min: minDensity,
				max: math.Floor(minDensity * float64(t.opts.fanout(next))),
			}
		}
		level.files = append(level.files, f)
	}
	return append(levels, level)
}

// maxOverlap returns the largest number of the files whose key ranges cover a
// common key, and those files.
func maxOverlap(files []*fileMeta) (int, []*fileMeta) {
	type event struct {
		key   []byte
		start bool
		f     *fileMeta
	}
	events := make([]event, 0, 2*len(files))
	for _, f := range files {
		events = append(events, event{f.smallest.UserKey, true, f}, event{f.largest.UserKey, false, f})
	}
	// Starts sort before ends at the same key: ranges are inclusive.
	slices.SortFunc(events, func(a, b event) int {
		if c := bytes.Compare(a.key, b.key); c != 0 {
			return c
		}
		if a.start == b.start {
			return 0
		}
		if a.start {
			return -1
		}
		return 1
	})
	best, cur := 0, 0
	var active, bestSet []*fileMeta
	for _, ev := range events {
		if ev.start {
			cur++
			active = append(active, ev.f)
			if cur > best {
				best, bestSet = cur, slices.Clone(active)
			}
		} else {
			cur--
			active = slices.DeleteFunc(active, func(f *fileMeta) bool { return f == ev.f })
		}
	}
	return best, bestSet
}

// pickCompactionLocked chooses the next compaction for the table, or nil.
// Like UnifiedCompactionStrategy, it looks for levels whose maximum overlap
// reaches the level's threshold, prefers the highest overlap (ties go to the
// lower level), and sizes the pick as Bucket.constructPick does.
func (t *Table) pickCompactionLocked() []*fileMeta {
	var candidates []*fileMeta
	for _, f := range t.files {
		if !f.compacting {
			candidates = append(candidates, f)
		}
	}
	var bestPick []*fileMeta
	bestOverlap := 0
	for _, level := range t.formLevelsLocked(candidates) {
		overlap, set := maxOverlap(level.files)
		if overlap < t.opts.threshold(level.index) || overlap <= bestOverlap {
			continue
		}
		bestOverlap = overlap
		bestPick = t.constructPickLocked(level.index, overlap, set)
	}
	return bestPick
}

// constructPickLocked follows Bucket.constructPick.
func (t *Table) constructPickLocked(index, count int, set []*fileMeta) []*fileMeta {
	// Oldest first, so a capped pick compacts the oldest data.
	slices.SortFunc(set, func(a, b *fileMeta) int { return cmp.Compare(a.maxSeq, b.maxSeq) })
	fanout := t.opts.fanout(index)
	maxToCompact := max(fanout, t.opts.MaxSSTablesToCompact)
	switch {
	case count <= fanout:
		return set
	case count <= fanout*t.opts.fanout(index+1) || maxToCompact == fanout:
		return set[:min(count, maxToCompact)]
	default:
		return set[:t.selectPickSize(index, count, maxToCompact)]
	}
}

// selectPickSize follows Bucket.selectPickSize: when compaction is far
// behind, compact as many sstables as needed to reach a level directly.
func (t *Table) selectPickSize(index, count, maxToCompact int) int {
	limit := min(maxToCompact, count)
	pickSize, nextStep := 0, t.opts.fanout(index)
	for i := index; ; {
		pickSize = nextStep
		i++
		nextStep *= t.opts.fanout(i)
		if nextStep > limit {
			break
		}
	}
	if t.opts.scalingParameter(index) < 0 {
		pickSize *= limit / pickSize
	}
	return pickSize
}

// maybeScheduleCompactionsLocked starts compactions for tables that need one,
// up to MaxConcurrentCompactions, at most one per table.
func (e *Engine) maybeScheduleCompactionsLocked() {
	if e.mu.bgErr != nil || e.mu.closed || e.opts.DisableCompactions {
		return
	}
	for _, t := range e.mu.tables {
		if e.mu.compactions >= e.opts.MaxConcurrentCompactions {
			return
		}
		if t.compacting {
			continue
		}
		inputs := t.pickCompactionLocked()
		if len(inputs) < 2 {
			continue
		}
		for _, f := range inputs {
			f.compacting = true
			f.refs.Add(1)
		}
		t.compacting = true
		e.mu.compactions++
		e.compactWG.Add(1)
		go e.runCompaction(t, inputs)
	}
}

func (e *Engine) runCompaction(t *Table, inputs []*fileMeta) {
	defer e.compactWG.Done()
	err := e.compact(t, inputs)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, f := range inputs {
		f.compacting = false
		f.unref()
	}
	t.compacting = false
	e.mu.compactions--
	if err != nil && !e.mu.closed && !t.dropped.Load() {
		e.setBackgroundErrorLocked(fmt.Errorf("lsm: compaction failed: %w", err))
	}
	e.mu.cond.Broadcast()
	e.maybeScheduleCompactionsLocked()
}

// errCompactionAborted stops a compaction when the engine closes or the
// table is dropped.
var errCompactionAborted = fmt.Errorf("lsm: compaction aborted")

// compact merges the inputs into one sstable and installs it. Fragments of
// each key are merged with the Merger; nothing is purged yet (M5).
func (e *Engine) compact(t *Table, inputs []*fileMeta) error {
	e.mu.Lock()
	num := e.mu.nextFileNum
	e.mu.nextFileNum++
	e.mu.Unlock()

	out, err := e.writeCompactionOutput(t, num, inputs)
	if err != nil {
		if err == errCompactionAborted {
			return nil
		}
		return err
	}

	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if e.mu.closed || t.dropped.Load() {
		e.mu.Unlock()
		out.reader.close()
		_ = e.fs.Remove(out.path)
		return nil
	}
	te := &tableEdit{id: t.id, added: []*fileMeta{out}}
	for _, f := range inputs {
		te.deleted = append(te.deleted, f.num)
	}
	manifest := e.mu.manifest
	e.mu.Unlock()
	if err := manifest.write(&versionEdit{tables: []*tableEdit{te}, nextFileNum: num + 1}); err != nil {
		out.reader.close()
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	files := []*fileMeta{out}
	for _, f := range t.files {
		if !slices.Contains(inputs, f) {
			files = append(files, f)
		}
	}
	sortFilesNewestFirst(files)
	t.files = files
	for _, f := range inputs {
		f.obsolete.Store(true)
	}
	t.publishLocked()
	e.metrics.bytesCompacted.Add(int64(out.size))
	e.metrics.compactions.Add(1)
	return nil
}

func (e *Engine) writeCompactionOutput(t *Table, num uint64, inputs []*fileMeta) (*fileMeta, error) {
	// The buffer pool is released after the iterators that use it close
	// (deferred calls run last-in, first-out).
	var bp block.BufferPool
	bp.Init(2*len(inputs)+4, block.ForCompaction)
	defer bp.Release()
	mi := mergingIter{merger: e.merger}
	defer mi.close()
	// The output's timestamp range is the union of the inputs': merging only
	// keeps or drops existing cells and deletions, so the union bounds the
	// output's range. A wider range is safe: reads stop early only below a
	// file's maxTS, and purging (M5) treats minTS as possibly-older data.
	minTS, maxTS := int64(math.MaxInt64), int64(math.MinInt64)
	for _, f := range inputs {
		src, err := newCompactionSource(f.reader, &bp)
		if err != nil {
			return nil, err
		}
		mi.sources = append(mi.sources, src)
		minTS, maxTS = min(minTS, f.minTS), max(maxTS, f.maxTS)
	}

	path := e.fs.PathJoin(t.dir, tableFileName(num))
	file, err := e.fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(file), e.writerOpts)
	n := 0
	for ok := mi.first(); ok; ok = mi.next() {
		if n++; n%1024 == 0 && (e.unusable.Load() || t.dropped.Load()) {
			w.Close()
			_ = e.fs.Remove(path)
			return nil, errCompactionAborted
		}
		k := pebble.InternalKey{
			UserKey: mi.key,
			Trailer: pebble.InternalKeyTrailer(mi.trailer&^0xff | uint64(pebble.InternalKeyKindMerge)),
		}
		if err := w.Add(k, mi.value, false, sstable.KVMeta{}); err != nil {
			w.Close()
			return nil, err
		}
	}
	if mi.e != nil {
		w.Close()
		return nil, mi.e
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	md, err := w.Metadata()
	if err != nil {
		return nil, err
	}
	out := &fileMeta{
		num:      num,
		size:     md.Size,
		smallest: md.SmallestPoint.Clone(),
		largest:  md.LargestPoint.Clone(),
		minSeq:   uint64(md.SeqNums.Low),
		maxSeq:   uint64(md.SeqNums.High),
		minTS:    minTS,
		maxTS:    maxTS,
	}
	if err := e.openFile(t, out); err != nil {
		return nil, err
	}
	if err := syncDir(e.fs, t.dir); err != nil {
		out.reader.close()
		return nil, err
	}
	return out, nil
}

// openFile opens an sstable of the table and fills in its runtime fields.
func (e *Engine) openFile(t *Table, f *fileMeta) error {
	r, err := openTable(e.fs, t.dir, f.num, e.readerOpts)
	if err != nil {
		return err
	}
	f.reader, f.fs, f.path = r, e.fs, e.fs.PathJoin(t.dir, tableFileName(f.num))
	return nil
}
