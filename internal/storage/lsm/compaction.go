package lsm

import (
	"bytes"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/sstable/block"
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

	// Sharding (UCS's base_shard_count, target_sstable_size,
	// min_sstable_size, sstable_growth). The defaults are Cassandra's: 4,
	// 1 GiB, 100 MiB and 0.333.
	BaseShardCount    int
	TargetSSTableSize int64
	// MinSSTableSize: zero uses the default; negative disables it.
	MinSSTableSize int64
	// SSTableGrowth is λ in [0, 1]: zero uses the default; a negative value
	// means 0 (shard count grows with density, sstable size stays at the
	// target).
	SSTableGrowth float64
	// SerialOutputShards writes a compaction's output shards one after the
	// other instead of in parallel (parallelize_output_shards: false).
	SerialOutputShards bool

	// TimeWindow, if positive, selects the time-window mode for insert-only
	// TTL tables (Cassandra's TimeWindowCompactionStrategy): sstables are
	// grouped by window of their maximum write timestamp (microseconds, as
	// Cassandra's); the newest window is compacted with UCS's rules, every
	// older window into a single sstable, so whole files expire together.
	// Output is not sharded.
	TimeWindow time.Duration
}

func (o *TableOptions) ensureDefaults() {
	if len(o.ScalingParameters) == 0 {
		o.ScalingParameters = []int{2}
	}
	if o.MaxSSTablesToCompact <= 0 {
		o.MaxSSTablesToCompact = 64
	}
	if o.BaseShardCount <= 0 {
		o.BaseShardCount = 4
	}
	if o.TargetSSTableSize <= 0 {
		o.TargetSSTableSize = 1 << 30
	}
	if o.MinSSTableSize == 0 {
		o.MinSSTableSize = 100 << 20
	}
	if o.SSTableGrowth == 0 {
		o.SSTableGrowth = 0.333
	}
}

func (o *TableOptions) sstableGrowth() float64 { return min(max(o.SSTableGrowth, 0), 1) }

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
	files = slices.Clone(files)
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

// byLargest is a min-heap of files by largest key.
type byLargest []*fileMeta

func (h byLargest) Len() int { return len(h) }
func (h byLargest) Less(i, j int) bool {
	return bytes.Compare(h[i].largest.UserKey, h[j].largest.UserKey) < 0
}
func (h byLargest) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *byLargest) Push(x any)   { *h = append(*h, x.(*fileMeta)) }
func (h *byLargest) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// overlapSets follows Overlaps.constructOverlapSets: the minimal list of
// sets, in key order, such that files that do not overlap never share a set
// and, for every key, some set holds every file covering it. The largest
// set's size is the read amplification UCS's threshold controls.
func overlapSets(files []*fileMeta) [][]*fileMeta {
	if len(files) == 0 {
		return nil
	}
	files = slices.Clone(files)
	slices.SortFunc(files, func(a, b *fileMeta) int { return bytes.Compare(a.smallest.UserKey, b.smallest.UserKey) })
	var sets [][]*fileMeta
	var active byLargest
	startsAfter := func(f, g *fileMeta) bool { return bytes.Compare(f.smallest.UserKey, g.largest.UserKey) > 0 }
	for _, f := range files {
		if len(active) > 0 && startsAfter(f, active[0]) {
			sets = append(sets, slices.Clone([]*fileMeta(active)))
			for len(active) > 0 && startsAfter(f, active[0]) {
				heap.Pop(&active)
			}
		}
		heap.Push(&active, f)
	}
	return append(sets, slices.Clone([]*fileMeta(active)))
}

// maxOverlap returns the largest number of files that cover a common key,
// and those files.
func maxOverlap(files []*fileMeta) (int, []*fileMeta) {
	var best []*fileMeta
	for _, s := range overlapSets(files) {
		if len(s) > len(best) {
			best = s
		}
	}
	return len(best), best
}

// ucsBucket is a set of overlap sections compacted together.
type ucsBucket struct {
	sets       [][]*fileMeta
	maxOverlap int
}

// assignBuckets follows Overlaps.assignOverlapsIntoBuckets with the
// TRANSITIVE inclusion method: every set reaching the threshold is extended
// with neighbouring sets that share a file with it, transitively. The
// resulting buckets share no file, so they can be compacted in parallel.
func assignBuckets(threshold int, sets [][]*fileMeta) []ucsBucket {
	var buckets []ucsBucket
	lastEnd := -1
	for i := 0; i < len(sets); i++ {
		if len(sets[i]) < threshold {
			continue
		}
		all := map[*fileMeta]bool{}
		for _, f := range sets[i] {
			all[f] = true
		}
		intersects := func(s []*fileMeta) bool {
			for _, f := range s {
				if all[f] {
					return true
				}
			}
			return false
		}
		j := i - 1
		for ; j > lastEnd && intersects(sets[j]); j-- {
			for _, f := range sets[j] {
				all[f] = true
			}
		}
		start := j + 1
		for j = i + 1; j < len(sets) && intersects(sets[j]); j++ {
			for _, f := range sets[j] {
				all[f] = true
			}
		}
		b := ucsBucket{sets: sets[start:j]}
		for _, s := range b.sets {
			b.maxOverlap = max(b.maxOverlap, len(s))
		}
		buckets = append(buckets, b)
		i = j - 1
		lastEnd = i
	}
	return buckets
}

// files returns the bucket's files, newest (by maximum timestamp) first, as
// Bucket.allSSTablesSorted.
func (b *ucsBucket) files() []*fileMeta {
	seen := map[*fileMeta]bool{}
	var out []*fileMeta
	for _, s := range b.sets {
		for _, f := range s {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	slices.SortStableFunc(out, func(a, b *fileMeta) int {
		if c := cmp.Compare(b.maxTS, a.maxTS); c != 0 {
			return c
		}
		return cmp.Compare(b.maxSeq, a.maxSeq)
	})
	return out
}

// pullOldest follows Overlaps.pullLastWithOverlapLimit: the oldest files
// such that no overlap section holds more than limit of them.
func (b *ucsBucket) pullOldest(limit int) []*fileMeta {
	all := b.files()
	selected := make([]int, len(b.sets))
	for n := 0; n < len(all); n++ {
		f := all[len(all)-1-n]
		for i, s := range b.sets {
			if slices.Contains(s, f) {
				if selected[i]++; selected[i] > limit {
					return all[len(all)-n:]
				}
			}
		}
	}
	return all
}

// constructPick follows Bucket.constructPick: compact the whole bucket,
// unless compaction is late, in which case pick the oldest files that
// preserve the hierarchy's structure.
func (t *Table) constructPick(index int, b *ucsBucket) []*fileMeta {
	count := b.maxOverlap
	fanout := t.opts.fanout(index)
	maxToCompact := max(fanout, t.opts.MaxSSTablesToCompact)
	switch {
	case count <= fanout:
		return b.files()
	case count <= fanout*t.opts.fanout(index+1) || maxToCompact == fanout:
		if count <= maxToCompact {
			return b.files()
		}
		return b.pullOldest(maxToCompact)
	default:
		return b.pullOldest(t.selectPickSize(index, count, maxToCompact))
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

// ucsPick chooses a compaction among files with UCS's rules
// (UnifiedCompactionStrategy.chooseCompactionPick): in each level, the
// bucket with the highest overlap reaching the threshold; across levels, the
// highest overlap, ties going to the lower level.
func (t *Table) ucsPickLocked(files []*fileMeta) []*fileMeta {
	var best []*fileMeta
	bestOverlap := 0
	for _, level := range t.formLevelsLocked(files) {
		threshold := t.opts.threshold(level.index)
		var chosen *ucsBucket
		buckets := assignBuckets(threshold, overlapSets(level.files))
		for i := range buckets {
			if chosen == nil || buckets[i].maxOverlap > chosen.maxOverlap {
				chosen = &buckets[i]
			}
		}
		if chosen == nil || chosen.maxOverlap <= bestOverlap {
			continue
		}
		bestOverlap = chosen.maxOverlap
		best = t.constructPick(level.index, chosen)
	}
	return best
}

// timeWindow returns the window of a file's newest data.
func (o *TableOptions) timeWindow(f *fileMeta) int64 {
	w := o.TimeWindow.Microseconds()
	q := f.maxTS / w
	if f.maxTS%w < 0 {
		q-- // floor
	}
	return q
}

// timeWindowPickLocked follows TimeWindowCompactionStrategy: windows are
// visited newest first; the newest is compacted with UCS's rules, an older
// one once it holds two or more sstables, all together (oldest first, up to
// MaxSSTablesToCompact).
func (t *Table) timeWindowPickLocked(files []*fileMeta) []*fileMeta {
	windows := map[int64][]*fileMeta{}
	var keys []int64
	for _, f := range files {
		w := t.opts.timeWindow(f)
		if windows[w] == nil {
			keys = append(keys, w)
		}
		windows[w] = append(windows[w], f)
	}
	slices.Sort(keys)
	slices.Reverse(keys)
	for i, w := range keys {
		fs := windows[w]
		if i == 0 {
			if p := t.ucsPickLocked(fs); p != nil {
				return p
			}
			continue
		}
		if len(fs) >= 2 {
			slices.SortFunc(fs, func(a, b *fileMeta) int { return cmp.Compare(a.maxTS, b.maxTS) })
			return fs[:min(len(fs), t.opts.MaxSSTablesToCompact)]
		}
	}
	return nil
}

// pickCompactionLocked chooses the next compaction for the table among the
// files no compaction is using, or returns nil.
func (t *Table) pickCompactionLocked() []*fileMeta {
	var candidates []*fileMeta
	for _, f := range t.files {
		if !f.compacting {
			candidates = append(candidates, f)
		}
	}
	if t.opts.TimeWindow > 0 {
		return t.timeWindowPickLocked(candidates)
	}
	return t.ucsPickLocked(candidates)
}

// maybeScheduleCompactionsLocked starts compactions, round-robin across
// tables, up to MaxConcurrentCompactions. A table may run several at once:
// each pick excludes files other compactions use, and UCS's buckets do not
// share files.
func (e *Engine) maybeScheduleCompactionsLocked() {
	if e.mu.bgErr != nil || e.mu.closed || e.opts.DisableCompactions {
		return
	}
	ids := make([]TableID, 0, len(e.mu.tables))
	for id := range e.mu.tables {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for progress := true; progress; {
		progress = false
		for _, id := range ids {
			if e.mu.compactions >= e.opts.MaxConcurrentCompactions {
				return
			}
			t := e.mu.tables[id]
			inputs := t.pickCompactionLocked()
			if len(inputs) < 2 {
				continue
			}
			for _, f := range inputs {
				f.compacting = true
				f.refs.Add(1)
			}
			e.mu.compactions++
			e.compactWG.Add(1)
			go e.runCompaction(t, inputs)
			progress = true
		}
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
	e.mu.compactions--
	if err != nil && !e.mu.closed && !t.dropped.Load() {
		e.setBackgroundErrorLocked(fmt.Errorf("lsm: compaction failed: %w", err))
	}
	e.mu.cond.Broadcast()
	e.maybeScheduleCompactionsLocked()
}

// errCompactionAborted stops a compaction when the engine closes or the
// table is dropped.
var errCompactionAborted = errors.New("lsm: compaction aborted")

// outputShards returns the key ranges, [lower, upper) with nil for open
// ends, of the shards a compaction of inputs writes: UCS splits output at
// fixed token boundaries chosen from the output's estimated density, so a
// partition is never split and shards can be written in parallel.
func (t *Table) outputShards(inputs []*fileMeta) [][2][]byte {
	if t.opts.TimeWindow > 0 {
		return [][2][]byte{{nil, nil}}
	}
	lo, hi := inputs[0].smallest.UserKey, inputs[0].largest.UserKey
	for _, f := range inputs {
		if bytes.Compare(f.smallest.UserKey, lo) < 0 {
			lo = f.smallest.UserKey
		}
		if bytes.Compare(f.largest.UserKey, hi) > 0 {
			hi = f.largest.UserKey
		}
	}
	bounds := shardBoundaries(t.opts.numShards(combinedDensity(inputs)))
	var out [][2][]byte
	var lower []byte
	for _, b := range bounds {
		if bytes.Compare(b, lo) <= 0 {
			lower = b
			continue
		}
		out = append(out, [2][]byte{lower, b})
		lower = b
		if bytes.Compare(b, hi) > 0 {
			return out
		}
	}
	return append(out, [2][]byte{lower, nil})
}

// compact merges the inputs into one sstable per output shard and installs
// them in one manifest edit. Fragments of each key are merged with the
// Merger; nothing is purged yet (M5).
func (e *Engine) compact(t *Table, inputs []*fileMeta) error {
	shards := t.outputShards(inputs)
	outs := make([]*fileMeta, len(shards))
	errs := make([]error, len(shards))
	run := func(i int) {
		e.compactionSem <- struct{}{}
		defer func() { <-e.compactionSem }()
		outs[i], errs[i] = e.writeCompactionShard(t, inputs, shards[i][0], shards[i][1])
	}
	if t.opts.SerialOutputShards || len(shards) == 1 {
		for i := range shards {
			if run(i); errs[i] != nil {
				break
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range shards {
			wg.Add(1)
			go func() { defer wg.Done(); run(i) }()
		}
		wg.Wait()
	}
	var out []*fileMeta
	for _, f := range outs {
		if f != nil {
			out = append(out, f)
		}
	}
	discard := func() {
		for _, f := range out {
			f.reader.close()
			_ = e.fs.Remove(f.path)
		}
	}
	if err := errors.Join(errs...); err != nil {
		discard()
		if errors.Is(err, errCompactionAborted) {
			return nil
		}
		return err
	}
	if err := syncDir(e.fs, t.dir); err != nil {
		discard()
		return err
	}

	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if e.mu.closed || t.dropped.Load() {
		e.mu.Unlock()
		discard()
		return nil
	}
	te := &tableEdit{id: t.id, added: out}
	for _, f := range inputs {
		te.deleted = append(te.deleted, f.num)
	}
	manifest, nextFileNum := e.mu.manifest, e.mu.nextFileNum
	e.mu.Unlock()
	if err := manifest.write(&versionEdit{tables: []*tableEdit{te}, nextFileNum: nextFileNum}); err != nil {
		discard()
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	files := slices.Clone(out)
	var size uint64
	for _, f := range out {
		size += f.size
	}
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
	e.metrics.bytesCompacted.Add(int64(size))
	e.metrics.compactions.Add(1)
	return nil
}

// writeCompactionShard merges the inputs' rows in [lower, upper) into one
// sstable, or returns nil if there are none.
func (e *Engine) writeCompactionShard(t *Table, inputs []*fileMeta, lower, upper []byte) (*fileMeta, error) {
	var in []*fileMeta
	for _, f := range inputs {
		if (upper == nil || bytes.Compare(f.smallest.UserKey, upper) < 0) &&
			(lower == nil || bytes.Compare(f.largest.UserKey, lower) >= 0) {
			in = append(in, f)
		}
	}
	if len(in) == 0 {
		return nil, nil
	}
	// The buffer pool is released after the iterators that use it close
	// (deferred calls run last-in, first-out).
	var bp block.BufferPool
	bp.Init(2*len(in)+4, block.ForCompaction)
	defer bp.Release()
	mi := mergingIter{merger: e.merger, upper: upper}
	defer mi.close()
	// The output's timestamp range is the union of the inputs': merging only
	// keeps or drops existing cells and deletions, so the union bounds the
	// output's range. A wider range is safe: reads stop early only below a
	// file's maxTS, and purging treats minTS as possibly-older data.
	minTS, maxTS := int64(math.MaxInt64), int64(math.MinInt64)
	for _, f := range in {
		src, err := newCompactionSource(f.reader, &bp)
		if err != nil {
			return nil, err
		}
		mi.sources = append(mi.sources, src)
		minTS, maxTS = min(minTS, f.minTS), max(maxTS, f.maxTS)
	}

	e.mu.Lock()
	num := e.mu.nextFileNum
	e.mu.nextFileNum++
	e.mu.Unlock()
	b, err := newSSTBuilder(e.fs, t.dir, num, e.writerOpts, e.merger, false)
	if err != nil {
		return nil, err
	}
	b.minTS, b.maxTS = minTS, maxTS
	ok := false
	if lower == nil {
		ok = mi.first()
	} else {
		ok = mi.seekGE(lower)
	}
	for n := 1; ok; ok, n = mi.next(), n+1 {
		if n%1024 == 0 && (e.unusable.Load() || t.dropped.Load()) {
			b.abort()
			return nil, errCompactionAborted
		}
		if err := b.add(mi.key, mi.value, mi.trailer); err != nil {
			b.abort()
			return nil, err
		}
	}
	if mi.e != nil {
		b.abort()
		return nil, mi.e
	}
	out, err := b.finish()
	if err != nil || out == nil {
		return nil, err
	}
	if err := e.openFile(t, out); err != nil {
		_ = e.fs.Remove(b.path)
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
