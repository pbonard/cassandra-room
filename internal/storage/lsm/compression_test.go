package lsm

import (
	"slices"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// TestTableCompression checks that a table's sstables use its compression,
// and that a change applies to the files written afterwards.
func TestTableCompression(t *testing.T) {
	cases := []struct {
		c    Compression
		name string
	}{
		{CompressionDefault, "MinLZ"}, {CompressionNone, "NoCompression"},
		{CompressionSnappy, "Snappy"}, {CompressionMinLZ, "MinLZ"}, {CompressionZstd, "ZSTD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := Open("db", &Options{FS: vfs.NewMem(), DisableCompactions: true,
				TableOptions: func(TableID) TableOptions { return TableOptions{Compression: tc.c} }})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			tbl, err := e.CreateTable(1)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 1000 {
				v := row.Append(nil, &row.Row{
					Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
					Cells: []row.Cell{{Column: 1, Timestamp: int64(i), LocalDeletionTime: row.NoDeletionTime,
						Value: []byte(fmt.Sprintf("value number %d of the table", i))}},
				})
				if err := e.Put(1, []byte(fmt.Sprintf("key%06d", i)), v, NoSync); err != nil {
					t.Fatal(err)
				}
			}
			if err := tbl.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := compressionNames(tbl); len(got) != 1 || got[0] != tc.name {
				t.Fatalf("compression %v, want %s", got, tc.name)
			}
		})
	}

	// A change of option: the next flush uses the new compression, the old
	// file keeps its own until rewritten.
	e, err := Open("db", &Options{FS: vfs.NewMem(), DisableCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tbl, err := e.CreateTable(1)
	if err != nil {
		t.Fatal(err)
	}
	put := func(k string) {
		v := row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
			Cells: []row.Cell{{Column: 1, Timestamp: 1, LocalDeletionTime: row.NoDeletionTime, Value: []byte("v")}}})
		if err := e.Put(1, []byte(k), v, NoSync); err != nil {
			t.Fatal(err)
		}
		if err := tbl.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	put("a")
	tbl.SetOptions(TableOptions{Compression: CompressionZstd})
	put("b")
	if got := compressionNames(tbl); len(got) != 2 || got[0] == got[1] {
		t.Fatalf("compressions after change %v, want MinLZ and ZSTD", got)
	}
	if err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := compressionNames(tbl); len(got) != 1 || got[0] != "ZSTD" {
		t.Fatalf("compression after compaction %v, want ZSTD", got)
	}
}

func compressionNames(tbl *Table) []string {
	out, err := tbl.SSTableCompressions()
	if err != nil {
		panic(err)
	}
	slices.Sort(out)
	return out
}
