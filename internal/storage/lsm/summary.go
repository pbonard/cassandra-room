package lsm

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
)

// Purger is implemented by Mergers that support purging and per-file
// summaries (row.Merger does). With a Merger that does not, nothing is
// purged and files carry no summary.
type Purger interface {
	// Summarize returns a row's statistics and reports the local deletion
	// time of each droppable part to ldt (if not nil).
	Summarize(v []byte, ldt func(int64)) (row.Stats, error)
	// PurgeAndSummarize drops the parts of v whose deletion is older than
	// gcBefore and whose timestamp is below maxPurgeable (see row.Purge),
	// and returns the result's statistics, as Summarize would, from the
	// same decoding.
	PurgeAndSummarize(dst, v []byte, nowSec, gcBefore int64, maxPurgeable func(minTS, maxTS int64) (int64, error), ldt func(int64)) (out []byte, changed, empty bool, st row.Stats, err error)
}

// fileStats is an sstable's summary (Cassandra's Statistics.db: maximum
// local deletion time, tombstone drop-time histogram), recorded in the
// manifest. Files written before summaries existed, or with a Merger that
// is not a Purger, have valid=false and are never purged on their own.
type fileStats struct {
	valid bool
	// created is when the file was written, in Unix seconds.
	created int64
	entries uint64
	// cells counts cells, live or not.
	cells uint64
	// tombstones counts tombstones, expiring cells and row deletions.
	tombstones uint64
	// maxLDT is the largest local deletion time (row.NoDeletionTime if any
	// cell is live and not expiring): once it is before gcBefore, all of the
	// file's data is deleted or expired and purgeable by time.
	maxLDT int64
	// minLDT is the earliest local deletion time of a droppable part
	// (row.NoDeletionTime if none): nothing in the file is purgeable before.
	minLDT int64
	// hist estimates the drop times (local deletion times) of the droppable
	// parts.
	hist ldtHistogram
}

func newFileStats(created int64) fileStats {
	return fileStats{valid: true, created: created, maxLDT: math.MinInt64, minLDT: row.NoDeletionTime}
}

// add accounts for one row.
func (s *fileStats) add(r *row.Stats) {
	s.entries++
	s.cells += uint64(r.Cells)
	s.tombstones += uint64(r.Tombstones)
	s.maxLDT = max(s.maxLDT, r.MaxLDT)
	s.minLDT = min(s.minLDT, r.MinLDT)
}

// union combines the summaries of compaction inputs into a bound for the
// output, used when rows are merged without being decoded: merging keeps a
// subset of the inputs' cells and deletions, so counts are upper bounds,
// maxLDT is at least the output's and minLDT at most.
func unionStats(created int64, files []*fileMeta) fileStats {
	s := newFileStats(created)
	for _, f := range files {
		if !f.stats.valid {
			return fileStats{}
		}
		s.entries += f.stats.entries
		s.cells += f.stats.cells
		s.tombstones += f.stats.tombstones
		s.maxLDT = max(s.maxLDT, f.stats.maxLDT)
		s.minLDT = min(s.minLDT, f.stats.minLDT)
		s.hist.merge(&f.stats.hist)
	}
	return s
}

// droppableRatio is StatsMetadata.getEstimatedDroppableTombstoneRatio: the
// tombstones droppable by time at gcBefore over the number of cells (0 if
// there are no cells).
func (s *fileStats) droppableRatio(gcBefore int64) float64 {
	if !s.valid || s.cells == 0 {
		return 0
	}
	return s.hist.countBefore(gcBefore) / float64(s.cells)
}

// ldtHistogram is a streaming histogram (Ben-Haim and Tom-Tov, as
// Cassandra's TombstoneHistogram) of local deletion times, with at most
// histBins bins.
type ldtHistogram struct {
	points []int64
	counts []uint64
}

const histBins = 32

func (h *ldtHistogram) add(v int64, n uint64) {
	i, found := slices.BinarySearch(h.points, v)
	if found {
		h.counts[i] += n
		return
	}
	h.points = slices.Insert(h.points, i, v)
	h.counts = slices.Insert(h.counts, i, n)
	if len(h.points) <= histBins {
		return
	}
	// Merge the two closest bins into their weighted mean.
	best := 0
	for j := 1; j < len(h.points)-1; j++ {
		if h.points[j+1]-h.points[j] < h.points[best+1]-h.points[best] {
			best = j
		}
	}
	c := h.counts[best] + h.counts[best+1]
	p := h.points[best] + int64(float64(h.points[best+1]-h.points[best])*float64(h.counts[best+1])/float64(c))
	h.points[best], h.counts[best] = p, c
	h.points = slices.Delete(h.points, best+1, best+2)
	h.counts = slices.Delete(h.counts, best+1, best+2)
}

func (h *ldtHistogram) merge(o *ldtHistogram) {
	for i := range o.points {
		h.add(o.points[i], o.counts[i])
	}
}

// countBefore estimates how many values are below v. Bins merge values
// into their mean, so the estimate counts a bin whose mean is below v.
func (h *ldtHistogram) countBefore(v int64) float64 {
	var n uint64
	for i, p := range h.points {
		if p >= v {
			break
		}
		n += h.counts[i]
	}
	return float64(n)
}

func appendFileStats(b []byte, s *fileStats) []byte {
	b = binary.AppendVarint(b, s.created)
	b = binary.AppendUvarint(b, s.entries)
	b = binary.AppendUvarint(b, s.cells)
	b = binary.AppendUvarint(b, s.tombstones)
	b = binary.AppendVarint(b, s.maxLDT)
	b = binary.AppendVarint(b, s.minLDT)
	b = binary.AppendUvarint(b, uint64(len(s.hist.points)))
	for i := range s.hist.points {
		b = binary.AppendVarint(b, s.hist.points[i])
		b = binary.AppendUvarint(b, s.hist.counts[i])
	}
	return b
}
