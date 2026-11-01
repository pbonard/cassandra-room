package lsm

import (
	"bytes"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
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
	// Cassandra's); the newest window seen is compacted with size-tiered
	// rules, every older window down to one sstable, so whole files expire
	// together. Output is not sharded. Cassandra's default window is one day.
	TimeWindow time.Duration
	// MinThreshold and MaxThreshold are the table's min_threshold and
	// max_threshold (4 and 32), used by the time-window mode.
	MinThreshold, MaxThreshold int
	// Size-tiered options used in the newest time window (bucket_low 0.5,
	// bucket_high 1.5, min_sstable_size 50 MiB).
	STCSBucketLow, STCSBucketHigh float64
	STCSMinSSTableSize            int64

	// Purging (M5). GCGraceSeconds is gc_grace_seconds: zero uses the
	// default, 864000 (10 days); negative means 0.
	GCGraceSeconds int64
	// UncheckedTombstoneCompaction is unchecked_tombstone_compaction: skip
	// the overlap check before compacting an sstable for its tombstones.
	UncheckedTombstoneCompaction bool
	// TombstoneThreshold is tombstone_threshold: an sstable whose estimated
	// ratio of droppable tombstones exceeds it is compacted on its own when
	// nothing else is due. Zero uses the default, 0.2; negative disables it.
	TombstoneThreshold float64
	// TombstoneCompactionInterval is tombstone_compaction_interval: the
	// minimum age of an sstable compacted for its tombstones. Zero uses the
	// default, one day; negative means 0.
	TombstoneCompactionInterval time.Duration
	// ExpiredCheckInterval is expired_sstable_check_frequency_seconds: how
	// often fully expired sstables are looked for. Zero uses the default,
	// 10 minutes; negative checks at every opportunity.
	ExpiredCheckInterval time.Duration
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
	if o.MinThreshold <= 0 {
		o.MinThreshold = 4
	}
	if o.MaxThreshold <= 0 {
		o.MaxThreshold = 32
	}
	if o.STCSBucketLow <= 0 {
		o.STCSBucketLow = 0.5
	}
	if o.STCSBucketHigh <= 0 {
		o.STCSBucketHigh = 1.5
	}
	if o.STCSMinSSTableSize <= 0 {
		o.STCSMinSSTableSize = 50 << 20
	}
	if o.GCGraceSeconds == 0 {
		o.GCGraceSeconds = 864000
	}
	if o.TombstoneThreshold == 0 {
		o.TombstoneThreshold = 0.2
	}
	if o.TombstoneCompactionInterval == 0 {
		o.TombstoneCompactionInterval = 24 * time.Hour
	}
	if o.ExpiredCheckInterval == 0 {
		o.ExpiredCheckInterval = 10 * time.Minute
	}
}

// gcBefore returns the local deletion time before which tombstones may be
// purged.
func (o *TableOptions) gcBefore(nowSec int64) int64 { return nowSec - max(o.GCGraceSeconds, 0) }

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
// (UnifiedCompactionStrategy.chooseCompactionPick and
// Level.getCompactionPick): in each level, a bucket chosen uniformly at
// random among those with the level's highest overlap (reservoir sampling,
// as Cassandra does); across levels, the highest overlap, ties going to the
// lower level.
//
// One deliberate difference: Cassandra compares levels by their maximum
// overlap even when it is below the level's threshold (no pick), which with
// per-level scaling parameters can discard a valid pick from a lower level;
// here only levels with a pick compete.
func (t *Table) ucsPickLocked(files []*fileMeta) []*fileMeta {
	var best []*fileMeta
	bestOverlap := 0
	for _, level := range t.formLevelsLocked(files) {
		threshold := t.opts.threshold(level.index)
		var chosen *ucsBucket
		matching := 0
		buckets := assignBuckets(threshold, overlapSets(level.files))
		top := 0
		for i := range buckets {
			top = max(top, buckets[i].maxOverlap)
		}
		for i := range buckets {
			if buckets[i].maxOverlap == top {
				matching++
				if rand.IntN(matching) == 0 {
					chosen = &buckets[i]
				}
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

// timeWindow returns the window of a file's newest data
// (TimeWindowCompactionStrategy.getWindowBoundsInMillis, by maximum
// timestamp in microseconds).
func (o *TableOptions) timeWindow(f *fileMeta) int64 {
	w := o.TimeWindow.Microseconds()
	q := f.maxTS / w
	if f.maxTS%w < 0 {
		q-- // floor
	}
	return q
}

// timeWindowPickLocked follows TimeWindowCompactionStrategy
// (getCompactionCandidates, newestBucket): sstables are grouped by window;
// the newest window ever seen (highestWindowSeen, which only moves forward)
// is compacted with size-tiered rules once it holds MinThreshold sstables;
// any older window with two or more sstables is compacted, its
// MaxThreshold smallest sstables at a time. Windows are visited newest
// first and the first eligible one wins.
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
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)
	slices.Reverse(keys)
	if !t.windowSeen || keys[0] > t.highestWindowSeen {
		t.highestWindowSeen, t.windowSeen = keys[0], true
	}
	now := t.highestWindowSeen
	for _, w := range keys {
		fs := windows[w]
		switch {
		case len(fs) >= t.opts.MinThreshold && w >= now:
			if p := mostInterestingBucket(stcsBuckets(fs, &t.opts), t.opts.MinThreshold, t.opts.MaxThreshold); p != nil {
				return p
			}
		case len(fs) >= 2 && w < now:
			// trimToThreshold: the smallest sstables, up to MaxThreshold.
			slices.SortStableFunc(fs, func(a, b *fileMeta) int { return cmp.Compare(a.size, b.size) })
			return fs[:min(len(fs), t.opts.MaxThreshold)]
		}
	}
	return nil
}

// stcsBuckets follows SizeTieredCompactionStrategy.getBuckets: sstables
// sorted by size join the first bucket whose average size is within
// [bucketLow, bucketHigh] times theirs, or which, like them, is below the
// minimum sstable size; buckets are kept in creation order (Cassandra's
// HashMap order is unspecified).
func stcsBuckets(files []*fileMeta, o *TableOptions) [][]*fileMeta {
	files = slices.Clone(files)
	slices.SortStableFunc(files, func(a, b *fileMeta) int { return cmp.Compare(a.size, b.size) })
	type bucket struct {
		avg   uint64
		files []*fileMeta
	}
	var buckets []*bucket
outer:
	for _, f := range files {
		size := f.size
		for _, b := range buckets {
			if (float64(size) > float64(b.avg)*o.STCSBucketLow && float64(size) < float64(b.avg)*o.STCSBucketHigh) ||
				(int64(size) < o.STCSMinSSTableSize && int64(b.avg) < o.STCSMinSSTableSize) {
				b.avg = (b.avg*uint64(len(b.files)) + size) / uint64(len(b.files)+1)
				b.files = append(b.files, f)
				continue outer
			}
		}
		buckets = append(buckets, &bucket{avg: size, files: []*fileMeta{f}})
	}
	out := make([][]*fileMeta, len(buckets))
	for i, b := range buckets {
		out[i] = b.files
	}
	return out
}

// mostInterestingBucket follows SizeTieredCompactionStrategy's: buckets
// trimmed to maxThreshold sstables, of at least minThreshold, compared by
// read hotness and then by average size. The engine keeps no read meters,
// so hotness is always 0, as for Cassandra's unread sstables, and the
// comparator decides. Collections.max over that comparator picks the
// largest average size (its comment says the smallest; the code is
// followed).
func mostInterestingBucket(buckets [][]*fileMeta, minThreshold, maxThreshold int) []*fileMeta {
	var best []*fileMeta
	var bestAvg uint64
	for _, b := range buckets {
		b = b[:min(len(b), maxThreshold)]
		if len(b) < minThreshold {
			continue
		}
		var total uint64
		for _, f := range b {
			total += f.size
		}
		avg := total / uint64(len(b))
		if best == nil || avg > bestAvg {
			best, bestAvg = b, avg
		}
	}
	return best
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

// compactionPick is a compaction to run.
type compactionPick struct {
	inputs []*fileMeta
	// dropOnly deletes fully expired inputs without reading them.
	dropOnly bool
	// single is a compaction of one sstable for its droppable tombstones.
	single bool
}

// nextCompactionLocked chooses the next compaction for the table: fully
// expired sstables first (checked every ExpiredCheckInterval), then UCS's
// (or the time-window mode's) pick, then an sstable worth compacting for its
// tombstones.
func (t *Table) nextCompactionLocked() *compactionPick {
	now := t.e.opts.Now()
	if t.opts.ExpiredCheckInterval < 0 || now.Sub(t.lastExpiredCheck) >= t.opts.ExpiredCheckInterval {
		t.lastExpiredCheck = now
		if expired := t.fullyExpiredLocked(now.Unix()); len(expired) > 0 {
			return &compactionPick{inputs: expired, dropOnly: true}
		}
	}
	if inputs := t.pickCompactionLocked(); len(inputs) >= 2 {
		return &compactionPick{inputs: inputs}
	}
	if f := t.dueFileLocked(now.Unix()); f != nil {
		return &compactionPick{inputs: []*fileMeta{f}, single: true}
	}
	return nil
}

// fullyExpiredLocked follows CompactionController.getFullyExpiredSSTables:
// candidates are sstables whose every part is deleted or expired before
// gcBefore (maximum local deletion time); the smallest timestamp of the
// overlapping sstables that still hold data (maximum local deletion time at
// or after gcBefore) and of the memtables bounds what they could shadow;
// candidates whose newest data is older than that are dropped without being
// read. Cassandra takes each memtable's minimum timestamp as a whole; the
// engine scans the memtables over the candidate's key range (bounded, and
// treated as holding the oldest data past the bound), which is never less
// strict for the keys concerned.
func (t *Table) fullyExpiredLocked(nowSec int64) []*fileMeta {
	gcBefore := t.opts.gcBefore(nowSec)
	expiredBy := func(f *fileMeta) bool {
		return f.stats.valid && f.stats.maxLDT < gcBefore && f.stats.maxLDT <= nowSec
	}
	var expired []*fileMeta
	for _, c := range t.files {
		if c.compacting || !expiredBy(c) {
			continue
		}
		minOther := t.memtableMinTSLocked(c.smallest.UserKey, c.largest.UserKey)
		for _, f := range t.files {
			if f == c || expiredBy(f) ||
				bytes.Compare(f.largest.UserKey, c.smallest.UserKey) < 0 ||
				bytes.Compare(f.smallest.UserKey, c.largest.UserKey) > 0 {
				continue
			}
			minOther = min(minOther, f.minTS)
		}
		if c.maxTS < minOther {
			expired = append(expired, c)
		}
	}
	return expired
}

// memtableScanLimit bounds the memtable entries memtableMinTSLocked reads;
// past it the memtables are assumed to hold arbitrarily old data.
const memtableScanLimit = 4096

// memtableMinTSLocked returns the smallest timestamp of the memtables'
// entries in [lo, hi], math.MaxInt64 if there are none, or math.MinInt64 if
// that cannot be told cheaply.
func (t *Table) memtableMinTSLocked(lo, hi []byte) int64 {
	mems := []*memTable{t.mem}
	for _, im := range t.imm {
		mems = append(mems, im.mem)
	}
	minTS, n := int64(math.MaxInt64), 0
	for _, m := range mems {
		for nd := m.seekGE(lo, math.MaxUint64); nd != nil && bytes.Compare(nd.key, hi) <= 0; nd = nd.next() {
			if n++; n > memtableScanLimit {
				return math.MinInt64
			}
			ts, _, ok, err := t.e.merger.Timestamps(nd.value)
			if err != nil {
				return math.MinInt64
			}
			if ok {
				minTS = min(minTS, ts)
			}
		}
	}
	return minTS
}

// worthDroppingTombstonesLocked follows
// AbstractCompactionStrategy.worthDroppingTombstones: the sstable must be
// older than TombstoneCompactionInterval and its estimated ratio of
// droppable tombstones to cells above TombstoneThreshold; then, unless
// UncheckedTombstoneCompaction, either nothing overlaps it, or it is fully
// expired, or the share of its keys outside the overlapping sstables'
// token ranges, times the ratio, is still above the threshold (Cassandra
// estimates that share from index samples; the engine assumes keys spread
// evenly over the sstable's token span).
func (t *Table) worthDroppingTombstonesLocked(f *fileMeta, nowSec, gcBefore int64) (float64, bool) {
	o := &t.opts
	if !f.stats.valid {
		return 0, false
	}
	// Cassandra: now < created + interval means not yet. The engine also
	// never picks a file written in the current second, so a file whose
	// tombstones cannot be dropped yet is not rewritten in a loop when the
	// interval is 0.
	if nowSec < f.stats.created+int64(max(o.TombstoneCompactionInterval, 0)/time.Second) || f.stats.created >= nowSec {
		return 0, false
	}
	ratio := f.stats.droppableRatio(gcBefore)
	if ratio <= o.TombstoneThreshold {
		return 0, false
	}
	if o.UncheckedTombstoneCompaction {
		return ratio, true
	}
	var overlaps []*fileMeta
	for _, g := range t.files {
		if g != f && bytes.Compare(g.largest.UserKey, f.smallest.UserKey) >= 0 &&
			bytes.Compare(g.smallest.UserKey, f.largest.UserKey) <= 0 {
			overlaps = append(overlaps, g)
		}
	}
	if len(overlaps) == 0 {
		return ratio, true
	}
	if f.stats.maxLDT < gcBefore && f.stats.maxLDT <= nowSec {
		minOther := t.memtableMinTSLocked(f.smallest.UserKey, f.largest.UserKey)
		for _, g := range overlaps {
			if !(g.stats.valid && g.stats.maxLDT < gcBefore) {
				minOther = min(minOther, g.minTS)
			}
		}
		if f.maxTS < minOther {
			return ratio, true
		}
	}
	remaining := uncoveredTokenShare(f, overlaps)
	return ratio, remaining*ratio > o.TombstoneThreshold
}

// uncoveredTokenShare returns the share of f's token span that no overlap's
// token range covers.
func uncoveredTokenShare(f *fileMeta, overlaps []*fileMeta) float64 {
	lo, hi := keyToken(f.smallest.UserKey), keyToken(f.largest.UserKey)
	type span struct{ a, b uint64 }
	var spans []span
	for _, g := range overlaps {
		a, b := max(keyToken(g.smallest.UserKey), lo), min(keyToken(g.largest.UserKey), hi)
		if a <= b {
			spans = append(spans, span{a, b})
		}
	}
	slices.SortFunc(spans, func(x, y span) int { return cmp.Compare(x.a, y.a) })
	total := float64(hi-lo) + 1
	covered, next := 0.0, lo
	for _, s := range spans {
		a := max(s.a, next)
		if a > s.b {
			continue
		}
		covered += float64(s.b-a) + 1
		if s.b == math.MaxUint64 {
			break
		}
		next = s.b + 1
	}
	return max(0, 1-covered/total)
}

// dueFileLocked returns an sstable worth compacting on its own for its
// tombstones (worthDroppingTombstonesLocked): with the time-window mode the
// smallest such sstable, as TimeWindowCompactionStrategy picks; otherwise
// the largest, as SizeTieredCompactionStrategy picks. Cassandra 5.0's UCS
// has no single-sstable tombstone compactions; the engine adds them to UCS
// tables, as this design's due-file picking.
func (t *Table) dueFileLocked(nowSec int64) *fileMeta {
	if t.opts.TombstoneThreshold < 0 {
		return nil
	}
	gcBefore := t.opts.gcBefore(nowSec)
	var best *fileMeta
	for _, f := range t.files {
		if f.compacting {
			continue
		}
		if _, ok := t.worthDroppingTombstonesLocked(f, nowSec, gcBefore); !ok {
			continue
		}
		switch {
		case best == nil:
			best = f
		case t.opts.TimeWindow > 0 && f.size < best.size:
			best = f
		case t.opts.TimeWindow <= 0 && f.size > best.size:
			best = f
		}
	}
	return best
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
			if t.majorPending {
				continue
			}
			p := t.nextCompactionLocked()
			if p == nil {
				continue
			}
			for _, f := range p.inputs {
				f.compacting = true
				f.refs.Add(1)
			}
			e.mu.compactions++
			e.compactWG.Add(1)
			go e.runCompaction(t, p)
			progress = true
		}
	}
}

func (e *Engine) runCompaction(t *Table, p *compactionPick) {
	defer e.compactWG.Done()
	err := e.compact(t, p)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, f := range p.inputs {
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

// Compact runs a major compaction of the table (Cassandra's nodetool
// compact): it flushes, waits for the table's running compactions, then
// merges all of its sstables, purging what the purge rule allows, into one
// set of shards. Background compactions of the table pause meanwhile.
func (t *Table) Compact() error {
	if err := t.Flush(); err != nil {
		return err
	}
	e := t.e
	e.mu.Lock()
	t.majorPending = true
	defer func() {
		e.mu.Lock()
		t.majorPending = false
		e.maybeScheduleCompactionsLocked()
		e.mu.Unlock()
	}()
	for {
		if err := e.checkUsableLocked(); err != nil {
			e.mu.Unlock()
			return err
		}
		if t.dropped.Load() {
			e.mu.Unlock()
			return ErrUnknownTable
		}
		if !slices.ContainsFunc(t.files, func(f *fileMeta) bool { return f.compacting }) {
			break
		}
		e.mu.cond.Wait()
	}
	files := slices.Clone(t.files)
	if len(files) == 0 {
		e.mu.Unlock()
		return nil
	}
	for _, f := range files {
		f.compacting = true
		f.refs.Add(1)
	}
	e.mu.compactions++
	e.compactWG.Add(1)
	e.mu.Unlock()
	e.runCompaction(t, &compactionPick{inputs: files})
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.checkUsableLocked()
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
// Merger, and what the purge rule allows is dropped (see
// writeCompactionShard). A drop-only pick installs no output.
func (e *Engine) compact(t *Table, p *compactionPick) error {
	inputs := p.inputs
	var shards [][2][]byte
	if !p.dropOnly {
		shards = t.outputShards(inputs)
	}
	outs := make([]*fileMeta, len(shards))
	errs := make([]error, len(shards))
	inputNums := map[uint64]bool{}
	for _, f := range inputs {
		inputNums[f.num] = true
	}
	run := func(i int) {
		e.compactionSem <- struct{}{}
		defer func() { <-e.compactionSem }()
		outs[i], errs[i] = e.writeCompactionShard(t, inputs, inputNums, shards[i][0], shards[i][1])
	}
	if t.opts.SerialOutputShards || len(shards) <= 1 {
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
	switch {
	case p.dropOnly:
		e.metrics.expiredDropped.Add(int64(len(inputs)))
	case p.single:
		e.metrics.tombstoneCompactions.Add(1)
	default:
		e.metrics.compactions.Add(1)
	}
	return nil
}

// writeCompactionShard merges the inputs' rows in [lower, upper) into one
// sstable, or returns nil if there are none.
//
// Rows are purged (row.Purge) when some input may hold a deletion older than
// gcBefore; otherwise rows are copied without being decoded, and the output's
// timestamps and summary are bounded by the inputs'.
func (e *Engine) writeCompactionShard(t *Table, inputs []*fileMeta, inputNums map[uint64]bool, lower, upper []byte) (*fileMeta, error) {
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

	nowSec := e.opts.Now().Unix()
	gcBefore := t.opts.gcBefore(nowSec)
	purger, _ := e.merger.(Purger)
	purge := false
	if purger != nil {
		for _, f := range in {
			// Files without a summary may hold anything.
			if !f.stats.valid || (f.stats.minLDT < gcBefore && f.stats.minLDT <= nowSec) {
				purge = true
			}
		}
	}

	e.mu.Lock()
	num := e.mu.nextFileNum
	e.mu.nextFileNum++
	e.mu.Unlock()
	b, err := newSSTBuilder(e.fs, t.dir, num, e.writerOpts, e.merger, purge, nowSec)
	if err != nil {
		return nil, err
	}
	if !purge {
		b.minTS, b.maxTS = minTS, maxTS
		if purger != nil {
			b.stats = unionStats(nowSec, in)
		} else {
			b.stats = fileStats{}
		}
	}
	var purged []byte
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
		value := mi.value
		if purge {
			key := mi.key
			out, changed, empty, err := purger.Purge(purged[:0], value, nowSec, gcBefore, func(minTS, maxTS int64) (int64, error) {
				return e.maxPurgeable(t, inputNums, key, minTS, maxTS)
			})
			if err != nil {
				b.abort()
				return nil, err
			}
			if empty {
				continue
			}
			if changed {
				purged, value = out, out
			}
		}
		if err := b.add(mi.key, value, mi.trailer); err != nil {
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

// maxPurgeable returns the smallest timestamp of key's data outside a
// compaction (DeletionPurger's maxPurgeableTimestamp): in every memtable,
// including entries not yet visible to readers, and in every sstable of the
// table's current state other than the compaction's inputs. A deletion
// older than that cannot shadow anything stored elsewhere.
//
// Only the decision for deletions with timestamps in [minTS, maxTS]
// matters: sources whose data is all newer than maxTS are skipped, and once
// a source at or below minTS is found nothing can be purged, so it returns
// math.MinInt64.
//
// As in Cassandra, a write arriving later with an older timestamp than a
// purged deletion is not shadowed by it: gc_grace_seconds is the bound on
// how late such writes (hints, repair) may arrive.
func (e *Engine) maxPurgeable(t *Table, inputs map[uint64]bool, key []byte, minTS, maxTS int64) (int64, error) {
	s := t.acquireState()
	defer s.release()
	limit := int64(math.MaxInt64)
	note := func(v []byte) error {
		lo, _, ok, err := e.merger.Timestamps(v)
		if ok {
			limit = min(limit, lo)
		}
		return err
	}
	for _, m := range s.mems {
		v, ok, err := m.getMerged(e.merger, key, math.MaxUint64, nil)
		if err == nil && ok {
			err = note(v)
		}
		if err != nil {
			return 0, err
		}
		if limit <= minTS {
			return math.MinInt64, nil
		}
	}
	for _, f := range s.files {
		if inputs[f.num] || f.minTS >= limit || f.minTS > maxTS || !f.mayContain(key) {
			continue
		}
		v, ok, err := f.reader.getMerged(e.merger, key, nil)
		if err == nil && ok {
			err = note(v)
		}
		if err != nil {
			return 0, err
		}
		if limit <= minTS {
			return math.MinInt64, nil
		}
	}
	return limit, nil
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
