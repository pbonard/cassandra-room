package lsm

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// BenchmarkRowSpace writes rows of ten live cells (one INSERT each, a
// microsecond timestamp) and reports the bytes per row in the WAL and in
// the sstables for each block compression.
func BenchmarkRowSpace(b *testing.B) {
	const rows = 100000
	for _, c := range []struct {
		name string
		c    Compression
	}{{"none", CompressionNone}, {"snappy", CompressionSnappy}, {"minlz", CompressionMinLZ}, {"zstd", CompressionZstd}} {
		b.Run(c.name, func(b *testing.B) {
			var wal, sst int64
			for b.Loop() {
				fs := vfs.NewMem()
				e, err := Open("db", &Options{FS: fs, DisableCompactions: true,
					TableOptions: func(TableID) TableOptions { return TableOptions{Compression: c.c} }})
				if err != nil {
					b.Fatal(err)
				}
				tbl, err := e.CreateTable(1)
				if err != nil {
					b.Fatal(err)
				}
				for i := range rows {
					r := &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}}
					ts := int64(1_764_000_000_000_000 + i*37)
					for col := range 10 {
						r.Cells = append(r.Cells, row.Cell{Column: uint32(col), Timestamp: ts, LocalDeletionTime: row.NoDeletionTime,
							Value: []byte(fmt.Sprintf("user-%d-field-%d", i%5000, col))})
					}
					if err := e.Put(1, tokenBenchKey(i), row.Append(nil, r), NoSync); err != nil {
						b.Fatal(err)
					}
				}
				wal = suffixBytes(b, fs, "db", ".wal")
				if err := tbl.Flush(); err != nil {
					b.Fatal(err)
				}
				sst = suffixBytes(b, fs, "db", ".sst")
				if err := e.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(wal)/rows, "wal-B/row")
			b.ReportMetric(float64(sst)/rows, "sst-B/row")
		})
	}
}

func suffixBytes(b *testing.B, fs vfs.FS, dir, suffix string) int64 {
	names, err := fs.List(dir)
	if err != nil {
		b.Fatal(err)
	}
	var n int64
	for _, name := range names {
		p := fs.PathJoin(dir, name)
		st, err := fs.Stat(p)
		if err != nil {
			b.Fatal(err)
		}
		if st.IsDir() {
			n += suffixBytes(b, fs, p, suffix)
		} else if strings.HasSuffix(name, suffix) {
			n += st.Size()
		}
	}
	return n
}
