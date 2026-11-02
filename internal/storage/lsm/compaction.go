package lsm

import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cockroachdb/pebble/sstable"
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
	// RowFilter, if set, makes a filter for each compaction shard: it sees
	// every row the shard writes, in key order, after purging, and may
	// rewrite or drop it. The database uses it for deletions that span
	// several rows (partition deletions, range tombstones), which the
	// row-at-a-time merger cannot apply.
	RowFilter func() RowFilter
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
	// FragmentSize, if positive, cuts each shard's compaction output into
	// fragments of about this size and releases inputs as the compaction
	// goes, as ScyllaDB's incremental compaction strategy does
	// (sstable_size_in_mb, 1000 MiB there). Zero uses the default, 1 GiB;
	// negative disables it.
	FragmentSize int64

	// TimeWindow, if positive, selects the time-window mode for insert-only
	// TTL tables (Cassandra's TimeWindowCompactionStrategy): sstables are
	// grouped by window of their maximum write timestamp (microseconds, as
	// Cassandra's); the newest window seen is compacted with size-tiered
	// rules, every older window down to one sstable, so whole files expire
	// together. Output is not sharded. Cassandra's default window is one day.
	TimeWindow time.Duration
	// TimeWindowCassandra selects Cassandra's TWCS behaviour instead of the
	// default ScyllaDB-derived one: sstables are not split by window, every
	// past window with two sstables is compacted whole, and no fragments.
	TimeWindowCassandra bool
	// UnsafeAggressiveExpiration is TWCS's
	// unsafe_aggressive_sstable_expiration: in the time-window mode, fully
	// expired sstables are dropped without checking overlapping data. Only
	// for tables written with TTLs and never deleted from or overwritten:
	// otherwise dropped deletions can resurrect data.
	UnsafeAggressiveExpiration bool
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
	// SpaceAmplificationGoal, if above 1, is ScyllaDB ICS's
	// space_amplification_goal (valid in (1, 2]): when the two highest
	// density levels exceed it ((S0+S1)/S0), they are compacted together.
	SpaceAmplificationGoal float64
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
	if o.FragmentSize == 0 {
		o.FragmentSize = 1 << 30
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

// fragmentSize is the fragment size in effect: none in the time-window
// mode, whose windows must each end as a single sstable (TWCS compacts a
// past window until it holds one, so fragments would recompact forever).
func (o *TableOptions) fragmentSize() int64 {
	if o.TimeWindow > 0 && o.TimeWindowCassandra {
		return 0
	}
	return max(o.FragmentSize, 0)
}

// TimeSplitter is implemented by Mergers that can split a row by write
// timestamp (row.Merger does); the time-window mode uses it to keep each
// sstable within one window.
type TimeSplitter interface {
	SplitByTimestamp(v []byte, bucket func(ts int64) int64) ([]row.Part, error)
}

// windowOfTS returns the time window of timestamp ts (microseconds).
func (o *TableOptions) windowOfTS(ts int64) int64 {
	if ts == math.MinInt64 {
		return math.MinInt64
	}
	w := o.TimeWindow.Microseconds()
	q := ts / w
	if ts%w < 0 {
		q--
	}
	return q
}

// timeSplitter returns the function splitting rows by time window, or nil
// when output is not segregated (outside the time-window mode, in
// Cassandra's TWCS behaviour, or with a Merger that cannot split).
func (t *Table) timeSplitter() func(v []byte) ([]row.Part, error) {
	if t.opts.TimeWindow <= 0 || t.opts.TimeWindowCassandra {
		return nil
	}
	sp, ok := t.e.merger.(TimeSplitter)
	if !ok {
		return nil
	}
	bucket := t.opts.windowOfTS
	return func(v []byte) ([]row.Part, error) { return sp.SplitByTimestamp(v, bucket) }
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

// twUnit is a run within a time window: the files one flush or compaction
// wrote for that window. The time-window mode counts and compacts whole
// runs, so a window compacted into a run of fragments counts as one.
type twUnit struct {
	files []*fileMeta
	size  uint64
}

func runUnits(files []*fileMeta) []*twUnit {
	byRun := map[uint64]*twUnit{}
	var order []uint64
	for _, f := range files {
		r := f.run
		if r == 0 {
			r = f.num
		}
		u := byRun[r]
		if u == nil {
			u = &twUnit{}
			byRun[r] = u
			order = append(order, r)
		}
		u.files = append(u.files, f)
		u.size += f.size
	}
	slices.Sort(order)
	units := make([]*twUnit, len(order))
	for i, r := range order {
		units[i] = byRun[r]
	}
	return units
}

func unitFiles(units []*twUnit) []*fileMeta {
	var out []*fileMeta
	for _, u := range units {
		out = append(out, u.files...)
	}
	return out
}

// timeWindowPickScyllaLocked follows ScyllaDB's
// time_window_compaction_strategy (get_compaction_candidates, newest_bucket,
// compaction_mode, window_min_threshold), over runs rather than sstables:
//
//   - the newest window seen (highest_window_seen, never moving back) is
//     compacted with size-tiered rules once it holds MinThreshold runs;
//   - a past window still "recent" (it was the newest window, and has not
//     yet been compacted into one run since) with two or more runs is
//     compacted whole, its MaxThreshold smallest runs at a time, but only
//     while the table has no other compaction running;
//   - any other past window gets size-tiered compaction from two similar
//     runs, so data arriving late into an old window (repair, hints,
//     streaming) is not rewritten together with the whole window each time.
//
// Windows are visited newest first and the first eligible pick wins.
func (t *Table) timeWindowPickScyllaLocked(files []*fileMeta) []*fileMeta {
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
	if t.recentWindows == nil {
		t.recentWindows = map[int64]bool{}
	}
	now := t.highestWindowSeen
	ongoing := slices.ContainsFunc(t.files, func(f *fileMeta) bool { return f.compacting })
	for _, w := range keys {
		units := runUnits(windows[w])
		last := w >= now
		if last {
			t.recentWindows[w] = true
		}
		threshold := 2
		if last {
			threshold = t.opts.MinThreshold
		}
		switch {
		case len(units) >= 2 && !last && t.recentWindows[w]:
			if ongoing {
				continue // serialized: wait for running compactions
			}
			// trim_to_threshold: the smallest runs, up to MaxThreshold.
			slices.SortStableFunc(units, func(a, b *twUnit) int { return cmp.Compare(a.size, b.size) })
			return unitFiles(units[:min(len(units), t.opts.MaxThreshold)])
		case len(units) >= threshold:
			sizes := make([]uint64, len(units))
			for i, u := range units {
				sizes[i] = u.size
			}
			var picked []*twUnit
			for _, i := range mostInterestingBucketIdx(stcsBucketsIdx(sizes, &t.opts), sizes, threshold, t.opts.MaxThreshold) {
				picked = append(picked, units[i])
			}
			if len(picked) > 0 {
				return unitFiles(picked)
			}
		default:
			// Compacted into one run: no longer needs a whole-window
			// compaction.
			if !last {
				delete(t.recentWindows, w)
			}
		}
	}
	return nil
}

// stcsBucketsIdx is stcsBuckets over sizes, returning indexes.
func stcsBucketsIdx(sizes []uint64, o *TableOptions) [][]int {
	files := make([]*fileMeta, len(sizes))
	idx := map[*fileMeta]int{}
	for i, sz := range sizes {
		files[i] = &fileMeta{size: sz}
		idx[files[i]] = i
	}
	var out [][]int
	for _, b := range stcsBuckets(files, o) {
		var bi []int
		for _, f := range b {
			bi = append(bi, idx[f])
		}
		out = append(out, bi)
	}
	return out
}

// mostInterestingBucketIdx is mostInterestingBucket over indexes into sizes.
func mostInterestingBucketIdx(buckets [][]int, sizes []uint64, minThreshold, maxThreshold int) []int {
	var best []int
	var bestAvg uint64
	for _, b := range buckets {
		b = b[:min(len(b), maxThreshold)]
		if len(b) < minThreshold {
			continue
		}
		var total uint64
		for _, i := range b {
			total += sizes[i]
		}
		avg := total / uint64(len(b))
		if best == nil || avg > bestAvg {
			best, bestAvg = b, avg
		}
	}
	return best
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
		if t.opts.TimeWindowCassandra {
			return t.timeWindowPickLocked(candidates)
		}
		return t.timeWindowPickScyllaLocked(candidates)
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
	// keep, if set, drops every row whose key it rejects (cleanup).
	keep func(key []byte) bool
	// unreffed holds inputs whose reference the compaction dropped when it
	// released them early. Guarded by Engine.mu.
	unreffed map[*fileMeta]bool
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
	// ScyllaDB ICS's cross-tier jobs run only once the table has no other
	// compaction, as there.
	if t.opts.TimeWindow <= 0 && !slices.ContainsFunc(t.files, func(f *fileMeta) bool { return f.compacting }) {
		if inputs := t.garbageCollectionJobLocked(now.Unix()); len(inputs) >= 2 {
			return &compactionPick{inputs: inputs, single: true}
		}
		if inputs := t.spaceAmplificationJobLocked(); len(inputs) >= 2 {
			return &compactionPick{inputs: inputs}
		}
	}
	return nil
}

// levelsWithFiles returns UCS's density levels that hold files, lowest
// density first.
func (t *Table) levelsWithFiles() []*ucsLevel {
	var out []*ucsLevel
	for _, l := range t.formLevelsLocked(t.files) {
		if len(l.files) > 0 {
			out = append(out, l)
		}
	}
	return out
}

// garbageCollectionJobLocked follows ScyllaDB ICS's
// find_garbage_collection_job: an sstable whose tombstones are worth
// dropping by age and ratio, but which worthDroppingTombstones rejects
// because overlapping sstables may hold the data they shadow, is compacted
// with its whole density level and the next one up (the next one down for
// the top level), so the tombstones meet that data and can be purged.
// Levels are searched from the top, as ICS searches tiers from the largest.
func (t *Table) garbageCollectionJobLocked(nowSec int64) []*fileMeta {
	if t.opts.TombstoneThreshold < 0 {
		return nil
	}
	gcBefore := t.opts.gcBefore(nowSec)
	levels := t.levelsWithFiles()
	for i := len(levels) - 1; i >= 0; i-- {
		want := false
		for _, f := range levels[i].files {
			if t.tombstoneCandidateLocked(f, nowSec, gcBefore) {
				if _, ok := t.worthDroppingTombstonesLocked(f, nowSec, gcBefore); !ok {
					want = true
					break
				}
			}
		}
		if !want {
			continue
		}
		inputs := slices.Clone(levels[i].files)
		switch {
		case i+1 < len(levels):
			inputs = append(inputs, levels[i+1].files...)
		case i > 0:
			inputs = append(inputs, levels[i-1].files...)
		}
		return inputs
	}
	return nil
}

// spaceAmplificationJobLocked follows ScyllaDB ICS's space amplification
// goal: with SpaceAmplificationGoal set, if the two highest density levels
// hold more than that many times the highest level's size ((S0+S1)/S0),
// they are compacted together, bounding the space overwritten data takes.
func (t *Table) spaceAmplificationJobLocked() []*fileMeta {
	goal := t.opts.SpaceAmplificationGoal
	if goal <= 0 {
		return nil
	}
	levels := t.levelsWithFiles()
	if len(levels) < 2 {
		return nil
	}
	size := func(l *ucsLevel) (n uint64) {
		for _, f := range l.files {
			n += f.size
		}
		return n
	}
	top, next := levels[len(levels)-1], levels[len(levels)-2]
	// The fragments of one run can fall in different density levels (a
	// shard's last fragment is smaller); compacting a single run with
	// itself would only rewrite it, again and again.
	runOf := func(f *fileMeta) uint64 {
		if f.run != 0 {
			return f.run
		}
		return f.num
	}
	runs := map[uint64]bool{}
	for _, f := range append(slices.Clone(top.files), next.files...) {
		runs[runOf(f)] = true
	}
	if len(runs) < 2 {
		return nil
	}
	s0, s1 := size(top), size(next)
	if s0 == 0 || float64(s0+s1)/float64(s0) <= goal {
		return nil
	}
	return append(slices.Clone(top.files), next.files...)
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
	aggressive := t.opts.TimeWindow > 0 && t.opts.UnsafeAggressiveExpiration
	for _, c := range t.files {
		if c.compacting || !expiredBy(c) {
			continue
		}
		if aggressive {
			// getFullyExpiredSSTables with ignoreOverlaps: no check of the
			// data the sstable's deletions might shadow elsewhere.
			expired = append(expired, c)
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
	if !t.tombstoneCandidateLocked(f, nowSec, gcBefore) {
		return 0, false
	}
	ratio := f.stats.droppableRatio(gcBefore)
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

// tombstoneCandidateLocked is the first half of worthDroppingTombstones:
// old enough, and enough droppable tombstones.
func (t *Table) tombstoneCandidateLocked(f *fileMeta, nowSec, gcBefore int64) bool {
	o := &t.opts
	if !f.stats.valid {
		return false
	}
	if nowSec < f.stats.created+int64(max(o.TombstoneCompactionInterval, 0)/time.Second) || f.stats.created >= nowSec {
		return false
	}
	return f.stats.droppableRatio(gcBefore) > o.TombstoneThreshold
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
		case t.opts.TimeWindow > 0 && !t.opts.TimeWindowCassandra && f.minTS < best.minTS:
			best = f // ScyllaDB's TWCS: the oldest data first
		case t.opts.TimeWindow > 0 && t.opts.TimeWindowCassandra && f.size < best.size:
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
		if !p.unreffed[f] {
			f.unref()
		}
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
func (t *Table) Compact() error { return t.majorCompaction(nil) }

// Cleanup rewrites the table without the rows whose key keep rejects
// (nodetool cleanup: data of ranges the node no longer replicates). Rows
// written meanwhile are not affected.
func (t *Table) Cleanup(keep func(key []byte) bool) error { return t.majorCompaction(keep) }

func (t *Table) majorCompaction(keep func(key []byte) bool) error {
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
	e.runCompaction(t, &compactionPick{inputs: files, keep: keep})
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

// compact merges the inputs into sstables, one run per output shard, and
// installs them. Fragments of each key are merged with the Merger, and what
// the purge rule allows is dropped (see compactShard). A drop-only pick
// installs no output.
//
// With FragmentSize, each shard's output is cut into fragments of about that
// size (at partition boundaries), and inputs are released as the compaction
// goes, as ScyllaDB's incremental compaction does: once every shard covering
// an input has sealed output past the input's last key, the sealed outputs
// and the exhausted inputs are swapped in one manifest edit, and the
// inputs' space can be reclaimed before the compaction ends. Inputs that
// contributed to a row whose deletion was purged are released together
// only, so a purged tombstone never disappears while older data it shadowed
// is still stored in an input (Scylla keeps such tombstones in side
// sstables instead). Every edit leaves a complete table: a crash or failure
// keeps what was installed, and the rest of the inputs.
func (e *Engine) compact(t *Table, p *compactionPick) error {
	c := &compaction{e: e, t: t, p: p, inputs: p.inputs}
	if !p.dropOnly {
		c.shards = t.outputShards(p.inputs)
	}
	c.init()
	errs := make([]error, len(c.shards))
	run := func(i int) {
		e.compactionSem <- struct{}{}
		defer func() { <-e.compactionSem }()
		errs[i] = c.compactShard(i)
		c.shardFinished(i, errs[i])
	}
	if t.opts.SerialOutputShards || len(c.shards) <= 1 {
		for i := range c.shards {
			if run(i); errs[i] != nil {
				break
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range c.shards {
			wg.Add(1)
			go func() { defer wg.Done(); run(i) }()
		}
		wg.Wait()
	}
	if err := errors.Join(errs...); err != nil {
		c.discardPending()
		if errors.Is(err, errCompactionAborted) {
			return nil
		}
		return err
	}
	// Install what is left: the remaining outputs and inputs.
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.installLocked(true); err != nil {
		c.discardPendingLocked()
		return err
	}
	switch {
	case p.dropOnly:
		e.metrics.expiredDropped.Add(int64(len(p.inputs)))
	case p.single:
		e.metrics.tombstoneCompactions.Add(1)
	default:
		e.metrics.compactions.Add(1)
	}
	return nil
}

// compaction is the state of one running compaction, shared by its shard
// tasks.
type compaction struct {
	e      *Engine
	t      *Table
	p      *compactionPick
	inputs []*fileMeta
	shards [][2][]byte
	nums   map[uint64]bool // input file numbers

	mu sync.Mutex
	// Per shard: finished, and the key below which all of its output is
	// sealed.
	done     []bool
	progress [][]byte
	// pending holds sealed outputs not yet installed.
	pending []*fileMeta
	// released marks inputs already swapped out.
	released []bool
	// group is a union-find over inputs: inputs that contributed to a row
	// whose deletion was purged are released together.
	group []int
	// installs counts manifest edits made by this compaction.
	installs int
	// runs maps output buckets to run identifiers (see fileMeta.run).
	runs map[int64]uint64
}

func (c *compaction) init() {
	c.nums = map[uint64]bool{}
	for _, f := range c.inputs {
		c.nums[f.num] = true
	}
	c.done = make([]bool, len(c.shards))
	c.progress = make([][]byte, len(c.shards))
	c.released = make([]bool, len(c.inputs))
	c.group = make([]int, len(c.inputs))
	for i := range c.group {
		c.group[i] = i
	}
}

// runID returns the run identifier of the compaction's output in bucket
// (one run per time window, one in all otherwise): the number of its first
// file.
func (c *compaction) runID(bucket int64, num uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runs == nil {
		c.runs = map[int64]uint64{}
	}
	if c.runs[bucket] == 0 {
		c.runs[bucket] = num
	}
	return c.runs[bucket]
}

func (c *compaction) find(i int) int {
	for c.group[i] != i {
		c.group[i] = c.group[c.group[i]]
		i = c.group[i]
	}
	return i
}

// tie puts the given inputs in one release group.
func (c *compaction) tie(inputs []int) {
	if len(inputs) < 2 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.find(inputs[0])
	for _, i := range inputs[1:] {
		c.group[c.find(i)] = r
	}
}

// exhaustedLocked reports whether every shard covering input i has sealed
// output past its last key, or finished.
func (c *compaction) exhaustedLocked(i int) bool {
	f := c.inputs[i]
	for s, b := range c.shards {
		if (b[1] != nil && bytes.Compare(f.smallest.UserKey, b[1]) >= 0) ||
			(b[0] != nil && bytes.Compare(f.largest.UserKey, b[0]) < 0) {
			continue // shard s does not cover f
		}
		if c.done[s] {
			continue
		}
		// progress[s] is an exclusive bound: every row below it is sealed.
		if c.progress[s] == nil || bytes.Compare(f.largest.UserKey, c.progress[s]) >= 0 {
			return false
		}
	}
	return true
}

// sealed records an output fragment of shard s, after which every row of
// the shard below bound is in sealed output, and releases inputs it
// completes.
func (c *compaction) sealed(s int, out *fileMeta, bound []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = append(c.pending, out)
	c.progress[s] = bound
	before := c.installs
	err := c.installLocked(false)
	if err == nil && c.installs > before && c.e.testAfterEarlyRelease != nil {
		c.e.testAfterEarlyRelease()
	}
	return err
}

func (c *compaction) shardFinished(s int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		c.done[s] = true
	}
}

// installLocked swaps sealed outputs for released inputs in one manifest
// edit. With final, every remaining input is released; otherwise only
// release groups whose inputs are all exhausted, and nothing happens if
// there are none.
func (c *compaction) installLocked(final bool) error {
	e, t := c.e, c.t
	var release []int
	if final {
		for i := range c.inputs {
			if !c.released[i] {
				release = append(release, i)
			}
		}
	} else {
		if t.opts.fragmentSize() <= 0 {
			return nil
		}
		ready := map[int]bool{}
		for i := range c.inputs {
			if c.released[i] {
				continue
			}
			r := c.find(i)
			ok, seen := ready[r]
			if !seen {
				ok = true
				for j := range c.inputs {
					if c.find(j) == r && !c.released[j] && !c.exhaustedLocked(j) {
						ok = false
						break
					}
				}
				ready[r] = ok
			}
			if ok {
				release = append(release, i)
			}
		}
		if len(release) == 0 {
			return nil
		}
	}
	out := c.pending
	if len(out) > 0 {
		if err := syncDir(e.fs, t.dir); err != nil {
			return err
		}
	}
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	e.mu.Lock()
	if e.mu.closed || t.dropped.Load() {
		e.mu.Unlock()
		return errCompactionAborted
	}
	te := &tableEdit{id: t.id, added: out}
	for _, i := range release {
		te.deleted = append(te.deleted, c.inputs[i].num)
	}
	manifest, nextFileNum := e.mu.manifest, e.mu.nextFileNum
	e.mu.Unlock()
	if err := manifest.write(&versionEdit{tables: []*tableEdit{te}, nextFileNum: nextFileNum}); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	gone := map[*fileMeta]bool{}
	for _, i := range release {
		gone[c.inputs[i]] = true
		c.released[i] = true
	}
	if !final {
		// Drop the compaction's reference on inputs released early, so
		// their space is reclaimed once readers finish with them. Every
		// shard's source on them is closed: a shard covering an exhausted
		// input either finished, or has sealed output past its last key,
		// which its source had to be advanced past (and closed) first.
		for _, i := range release {
			f := c.inputs[i]
			f.obsolete.Store(true)
			if c.p.unreffed == nil {
				c.p.unreffed = map[*fileMeta]bool{}
			}
			c.p.unreffed[f] = true
			f.unref()
		}
	}
	files := slices.Clone(out)
	var size uint64
	for _, f := range out {
		size += f.size
	}
	for _, f := range t.files {
		if !gone[f] {
			files = append(files, f)
		}
	}
	sortFilesNewestFirst(files)
	t.files = files
	for f := range gone {
		f.obsolete.Store(true)
	}
	t.publishLocked()
	e.metrics.bytesCompacted.Add(int64(size))
	if !final {
		e.metrics.earlyReleases.Add(int64(len(release)))
	}
	c.pending = nil
	c.installs++
	return nil
}

func (c *compaction) discardPending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.discardPendingLocked()
}

func (c *compaction) discardPendingLocked() {
	for _, f := range c.pending {
		f.reader.close()
		_ = c.e.fs.Remove(f.path)
	}
	c.pending = nil
}

// compactShard merges the inputs' rows in shard s into sstables: one, or a
// run of fragments of about FragmentSize cut at partition boundaries.
//
// Rows are purged (row.PurgeAndSummarize) when some input may hold a
// deletion older than gcBefore; otherwise rows are copied without being
// decoded, and the output's timestamps and summary are bounded by the
// inputs'.
func (c *compaction) compactShard(s int) error {
	e, t := c.e, c.t
	lower, upper := c.shards[s][0], c.shards[s][1]
	var in []*fileMeta
	var inIdx []int // index in c.inputs
	for i, f := range c.inputs {
		if (upper == nil || bytes.Compare(f.smallest.UserKey, upper) < 0) &&
			(lower == nil || bytes.Compare(f.largest.UserKey, lower) >= 0) {
			in = append(in, f)
			inIdx = append(inIdx, i)
		}
	}
	if len(in) == 0 {
		return nil
	}
	// The buffer pool is released after the iterators that use it close
	// (deferred calls run last-in, first-out).
	var bp block.BufferPool
	bp.Init(2*len(in)+4, block.ForCompaction)
	defer bp.Release()
	mi := mergingIter{merger: e.merger, upper: upper, closeExhausted: true}
	defer mi.close()
	srcInput := map[source]int{}
	// The output's timestamp range is the union of the inputs': merging only
	// keeps or drops existing cells and deletions, so the union bounds the
	// output's range. A wider range is safe: reads stop early only below a
	// file's maxTS, and purging treats minTS as possibly-older data.
	minTS, maxTS := int64(math.MaxInt64), int64(math.MinInt64)
	for j, f := range in {
		src, err := newCompactionSource(f.reader, &bp)
		if err != nil {
			return err
		}
		mi.sources = append(mi.sources, src)
		srcInput[src] = inIdx[j]
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
	var lookup *purgeLookup
	if purge {
		lookup = &purgeLookup{e: e, t: t, inputs: c.nums}
		defer lookup.close()
	}

	// Output builders, one per bucket: the time window of the data in the
	// time-window mode (rows split by timestamp), a single bucket otherwise.
	split := t.timeSplitter()
	type outBuilder struct {
		b                 *sstBuilder
		firstKey, lastKey []byte
	}
	builders := map[int64]*outBuilder{}
	var buckets []int64 // open buckets, for a deterministic order
	decode := purge || split != nil
	start := func(bucket int64) (*outBuilder, error) {
		e.mu.Lock()
		num := e.mu.nextFileNum
		e.mu.nextFileNum++
		e.mu.Unlock()
		sb, err := newSSTBuilder(e.fs, t.dir, num, e.writerOpts, e.merger, decode, nowSec)
		if err != nil {
			return nil, err
		}
		if !decode {
			sb.minTS, sb.maxTS = minTS, maxTS
			if purger != nil {
				sb.stats = unionStats(nowSec, in)
			} else {
				sb.stats = fileStats{}
			}
		}
		ob := &outBuilder{b: sb}
		builders[bucket] = ob
		buckets = append(buckets, bucket)
		slices.Sort(buckets)
		return ob, nil
	}
	// seal finishes bucket's builder. next is the key about to be written
	// (nil at the end): every row below it, and below the first key of every
	// other open builder, is then in sealed output.
	seal := func(bucket int64, next []byte) error {
		ob := builders[bucket]
		delete(builders, bucket)
		buckets = slices.DeleteFunc(buckets, func(x int64) bool { return x == bucket })
		out, err := ob.b.finish()
		path := ob.b.path
		if err != nil || out == nil {
			return err
		}
		if err := e.openFile(t, out); err != nil {
			_ = e.fs.Remove(path)
			return err
		}
		out.run = c.runID(bucket, out.num)
		bound := next
		for _, o := range builders {
			if bound == nil || bytes.Compare(o.firstKey, bound) < 0 {
				bound = o.firstKey
			}
		}
		if bound == nil {
			// Nothing pending and no next row: everything is sealed.
			bound = append(slices.Clone(ob.lastKey), 0)
		}
		return c.sealed(s, out, slices.Clone(bound))
	}
	fail := func(err error) error {
		for _, ob := range builders {
			ob.b.abort()
		}
		return err
	}
	fragment := uint64(t.opts.fragmentSize())
	// Inputs can only be released early if some shard seals a fragment
	// before the end; below one fragment's worth of input there is nothing
	// to release, so purged rows need not tie their inputs.
	var inSize uint64
	for _, f := range in {
		inSize += f.size
	}
	incremental := fragment > 0 && inSize > fragment
	var filter RowFilter
	var filterEnv *FilterEnv
	if t.opts.RowFilter != nil {
		filter = t.opts.RowFilter()
		filterEnv = &FilterEnv{NowSec: nowSec, GCBefore: gcBefore, MaxPurgeable: func(lo, hi []byte, minTS, maxTS int64) (int64, error) {
			if lookup == nil {
				lookup = &purgeLookup{e: e, t: t, inputs: c.nums}
			}
			return lookup.maxPurgeableRange(lo, hi, minTS, maxTS)
		}}
		defer func() {
			if lookup != nil && !purge {
				lookup.close()
			}
		}()
	}
	var purged []byte
	var contrib []int
	var hist ldtHistogram // deletion times of the current row, if not split
	ok := false
	if lower == nil {
		ok = mi.first()
	} else {
		ok = mi.seekGE(lower)
	}
	for n := 1; ok; ok, n = mi.next(), n+1 {
		if n%1024 == 0 && (e.unusable.Load() || t.dropped.Load()) {
			return fail(errCompactionAborted)
		}
		if c.p.keep != nil && !c.p.keep(mi.key) {
			continue
		}
		value := mi.value
		var rs *row.Stats
		if purge {
			// One decoding purges the row and yields its statistics.
			key := mi.key
			hist = ldtHistogram{}
			out, changed, empty, st, err := purger.PurgeAndSummarize(purged[:0], value, nowSec, gcBefore, func(minTS, maxTS int64) (int64, error) {
				return lookup.maxPurgeable(key, minTS, maxTS)
			}, func(ldt int64) { hist.add(ldt, 1) })
			if err != nil {
				return fail(err)
			}
			if changed && incremental && len(mi.cur) > 1 {
				// The inputs holding this row's fragments are released
				// together: a purged deletion may have shadowed data in
				// another of them.
				contrib = contrib[:0]
				for _, ent := range mi.cur {
					contrib = append(contrib, srcInput[ent.src])
				}
				c.tie(contrib)
			}
			if empty {
				continue
			}
			if changed {
				purged, value = out, out
			}
			rs = &st
		}
		if filter != nil {
			out, drop, err := filter.Filter(mi.key, value, filterEnv)
			if err != nil {
				return fail(err)
			}
			if drop || out != nil {
				if incremental && len(mi.cur) > 1 {
					contrib = contrib[:0]
					for _, ent := range mi.cur {
						contrib = append(contrib, srcInput[ent.src])
					}
					c.tie(contrib)
				}
			}
			if drop {
				continue
			}
			if out != nil {
				value, rs = out, nil
				if decode {
					st, err := e.merger.(Purger).Summarize(value, nil)
					if err != nil {
						return fail(err)
					}
					rs = &st
				}
			}
		}
		parts := []row.Part{{Value: value}}
		if split != nil {
			var err error
			if parts, err = split(value); err != nil {
				return fail(err)
			}
			if len(parts) > 1 {
				rs = nil // statistics per part, computed by the builders
			}
		}
		for _, part := range parts {
			ob := builders[part.Bucket]
			// Cut a fragment at a partition boundary once it is large.
			if ob != nil && fragment > 0 && ob.b.w.EstimatedSize() >= fragment && keyToken(mi.key) != keyToken(ob.lastKey) {
				if err := seal(part.Bucket, mi.key); err != nil {
					return fail(err)
				}
				ob = nil
			}
			if ob == nil {
				var err error
				if ob, err = start(part.Bucket); err != nil {
					return fail(err)
				}
				ob.firstKey = slices.Clone(mi.key)
			}
			var err error
			if rs != nil {
				for i := range hist.points {
					ob.b.stats.hist.add(hist.points[i], hist.counts[i])
				}
				err = ob.b.addSummarized(mi.key, part.Value, mi.trailer, rs)
			} else {
				err = ob.b.add(mi.key, part.Value, mi.trailer)
			}
			if err != nil {
				return fail(err)
			}
			ob.lastKey = append(ob.lastKey[:0], mi.key...)
		}
	}
	if mi.e != nil {
		return fail(mi.e)
	}
	for len(buckets) > 0 {
		if err := seal(buckets[0], nil); err != nil {
			return fail(err)
		}
	}
	return nil
}

// RowFilter rewrites or drops rows during a compaction (TableOptions).
type RowFilter interface {
	// Filter returns the row to write instead of value (nil to keep
	// value), or drop true to write nothing.
	Filter(key, value []byte, env *FilterEnv) (out []byte, drop bool, err error)
}

// FilterEnv is what a RowFilter may consult.
type FilterEnv struct {
	NowSec, GCBefore int64
	// MaxPurgeable is the smallest timestamp of the data in [lo, hi)
	// outside the compaction: a deletion of that range older than it
	// shadows nothing that survives the compaction.
	MaxPurgeable func(lo, hi []byte, minTS, maxTS int64) (int64, error)
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
	p := purgeLookup{e: e, t: t, inputs: inputs}
	defer p.close()
	return p.maxPurgeable(key, minTS, maxTS)
}

// purgeLookup answers maxPurgeable for the rows of one compaction shard,
// which come in key order. It keeps one point iterator per sstable it has
// looked in (holding a reference on the file), instead of creating one per
// row, so repeated lookups reuse the iterator and its loaded blocks. Each
// lookup still reads the table's current state, so sstables and memtables
// that appeared since earlier rows are seen.
type purgeLookup struct {
	e      *Engine
	t      *Table
	inputs map[uint64]bool
	iters  map[uint64]*lookupIter
}

type lookupIter struct {
	f  *fileMeta
	it sstable.Iterator
}

func (p *purgeLookup) close() {
	for _, li := range p.iters {
		_ = li.it.Close()
		li.f.unref()
	}
	p.iters = nil
}

// iter returns the cached iterator for f, creating it.
func (p *purgeLookup) iter(f *fileMeta) (sstable.Iterator, error) {
	if li := p.iters[f.num]; li != nil {
		return li.it, nil
	}
	it, err := f.reader.r.NewPointIter(context.Background(), sstable.IterOptions{
		FilterBlockSizeLimit: sstable.AlwaysUseFilterBlock,
		Env:                  sstable.NoReadEnv,
		ReaderProvider:       sstable.MakeTrivialReaderProvider(f.reader.r),
	})
	if err != nil {
		return nil, err
	}
	f.refs.Add(1) // keeps the file open while the iterator lives
	if p.iters == nil {
		p.iters = map[uint64]*lookupIter{}
	}
	p.iters[f.num] = &lookupIter{f: f, it: it}
	return it, nil
}

// maxPurgeableRange is maxPurgeable for every key in [lo, hi) (a whole
// partition): the smallest timestamp of the range's data outside the
// compaction. In an sstable it reads up to rangeScanLimit rows of the
// range, then falls back to the file's smallest timestamp.
func (p *purgeLookup) maxPurgeableRange(lo, hi []byte, minTS, maxTS int64) (int64, error) {
	e, t, inputs := p.e, p.t, p.inputs
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
		for n := m.seekGE(lo, math.MaxUint64); n != nil && bytes.Compare(n.key, hi) < 0; n = n.next() {
			if err := note(n.value); err != nil {
				return 0, err
			}
		}
		if limit <= minTS {
			return math.MinInt64, nil
		}
	}
	for _, f := range s.files {
		if inputs[f.num] || f.minTS >= limit || f.minTS > maxTS ||
			bytes.Compare(hi, f.smallest.UserKey) <= 0 || bytes.Compare(lo, f.largest.UserKey) > 0 {
			continue
		}
		var src tableSource
		if err := src.init(f.reader, lo, hi, false); err != nil {
			return 0, err
		}
		n := 0
		kv := src.it.SeekGE(lo, 0)
		for ; kv != nil; kv = src.it.Next() {
			if n++; n > rangeScanLimit {
				limit = min(limit, f.minTS)
				break
			}
			v, _, err := kv.Value(nil)
			if err == nil {
				err = note(v)
			}
			if err != nil {
				_ = src.it.Close()
				return 0, err
			}
		}
		err := src.it.Error()
		if cerr := src.it.Close(); err == nil {
			err = cerr
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

// rangeScanLimit bounds the rows maxPurgeableRange reads per sstable.
const rangeScanLimit = 256

func (p *purgeLookup) maxPurgeable(key []byte, minTS, maxTS int64) (int64, error) {
	e, t, inputs := p.e, p.t, p.inputs
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
		// Every fragment of the key, unmerged: only their smallest
		// timestamp matters.
		for n := m.get(key, math.MaxUint64>>8); n != nil && bytes.Equal(n.key, key); n = n.next() {
			if err := note(n.value); err != nil {
				return 0, err
			}
		}
		if limit <= minTS {
			return math.MinInt64, nil
		}
	}
	for _, f := range s.files {
		if inputs[f.num] || f.minTS >= limit || f.minTS > maxTS || !f.mayContain(key) {
			continue
		}
		it, err := p.iter(f)
		if err != nil {
			return 0, err
		}
		if kv := it.SeekPrefixGE(key, key, 0); kv != nil && bytes.Equal(kv.K.UserKey, key) {
			v, _, verr := kv.Value(nil)
			if verr == nil {
				verr = note(v)
			}
			if verr != nil {
				return 0, verr
			}
		} else if err := it.Error(); err != nil {
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
