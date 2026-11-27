package db

import (
	"testing"

	"github.com/pbonard/cassandra-room/internal/storage/lsm"
)

// TestCompressionOption checks the table option reaches the storage engine
// and bad maps are rejected as Cassandra does.
func TestCompressionOption(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	compression := func(table string) lsm.Compression {
		t.Helper()
		tb := s.d.schema.Load().Keyspaces["ks"].Tables[table]
		if tb == nil {
			t.Fatalf("no table %s", table)
		}
		return s.d.engineTableOptions(tb.Storage).Compression
	}

	s.ok("CREATE TABLE t (k int PRIMARY KEY, v text)")
	if c := compression("t"); c != lsm.CompressionMinLZ {
		t.Fatalf("default compression %v, want MinLZ (LZ4)", c)
	}
	for q, want := range map[string]lsm.Compression{
		"{'class': 'ZstdCompressor', 'compression_level': 3}":            lsm.CompressionZstd,
		"{'class': 'org.apache.cassandra.io.compress.SnappyCompressor'}": lsm.CompressionSnappy,
		"{'class': 'DeflateCompressor'}":                                 lsm.CompressionZstd,
		"{'enabled': 'false'}":                                           lsm.CompressionNone,
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 64}":           lsm.CompressionMinLZ,
		"{'sstable_compression': ''}":                                    lsm.CompressionNone,
		"{'class': 'org.apache.cassandra.io.compress.NoopCompressor'}":   lsm.CompressionNone,
	} {
		s.ok("ALTER TABLE t WITH compression = " + q)
		if c := compression("t"); c != want {
			t.Fatalf("%s: compression %v, want %v", q, c, want)
		}
	}
	s.fails("ALTER TABLE t WITH compression = {'class': 'BrotliCompressor'}", "Could not create Compression for type BrotliCompressor")
	s.fails("ALTER TABLE t WITH compression = {'class': 'LZ4Compressor', 'level': 1}", "Unknown compression options [level]")
	s.fails("ALTER TABLE t WITH compression = {'chunk_length_in_kb': 64}", "Missing sub-option 'class'")
	s.fails("CREATE TABLE u (k int PRIMARY KEY) WITH compression = {'enabled': 'maybe'}", "Invalid value maybe")
}
