package row

import (
	"fmt"
	"testing"
)

// benchRow is a typical row of an INSERT: ten live cells of one write
// (a microsecond timestamp), small values.
func benchRow(ts int64) *Row {
	r := &Row{Deletion: Deletion{MarkedForDeleteAt: NoTimestamp}}
	for c := range 10 {
		r.Cells = append(r.Cells, Cell{Column: uint32(c), Timestamp: ts, LocalDeletionTime: NoDeletionTime,
			Value: []byte(fmt.Sprintf("v%07d", c*1000+int(ts%1000)))})
	}
	return r
}

const benchTS = 1_764_000_000_000_000 // late 2025, in microseconds

func BenchmarkRowFormat(b *testing.B) {
	r := benchRow(benchTS)
	enc := Append(nil, r)
	b.Run("Append", func(b *testing.B) {
		buf := make([]byte, 0, 512)
		for b.Loop() {
			buf = Append(buf[:0], r)
		}
		b.ReportMetric(float64(len(enc)), "bytes/row")
	})
	b.Run("Decode", func(b *testing.B) {
		var out Row
		for b.Loop() {
			if err := decodeInto(&out, enc); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Merge of two writes of the same row: one updating half the columns.
	newer := benchRow(benchTS + 1)
	newer.Cells = newer.Cells[:5]
	encNewer := Append(nil, newer)
	b.Run("Merge", func(b *testing.B) {
		buf := make([]byte, 0, 512)
		for b.Loop() {
			var err error
			if buf, err = Merge(buf[:0], enc, encNewer); err != nil {
				b.Fatal(err)
			}
		}
	})
}
