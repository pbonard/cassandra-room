package db

import (
	"slices"
	"testing"

	"github.com/pbonard/cassandra-room/internal/storage/lsm"
	"github.com/stretchr/testify/require"
)

// TestCompressionOption checks the table option reaches the storage engine
// and bad maps are rejected as Cassandra's CompressionParams does.
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
		"{'class': 'ZstdCompressor', 'compression_level': 3}":                                       lsm.CompressionZstd,
		"{'class': 'ZstdCompressor', 'compression_level': -5}":                                      lsm.CompressionZstd,
		"{'class': 'org.apache.cassandra.io.compress.SnappyCompressor'}":                            lsm.CompressionSnappy,
		"{'class': 'DeflateCompressor'}":                                                            lsm.CompressionZstd,
		"{'enabled': 'false'}":                                                                      lsm.CompressionNone,
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 64, 'crc_check_chance': 0.5}":             lsm.CompressionMinLZ,
		"{'class': 'LZ4Compressor', 'lz4_compressor_type': 'high', 'lz4_high_compressor_level': 9}": lsm.CompressionMinLZ,
		"{'class': 'LZ4Compressor', 'chunk_length_kb': 16, 'min_compress_ratio': 1.1}":              lsm.CompressionMinLZ,
		"{'class': 'SnappyCompressor', 'min_compress_ratio': 0}":                                    lsm.CompressionSnappy,
		"{'class': 'LZ4Compressor', 'enabled': 'true'}":                                             lsm.CompressionMinLZ,
		"{'sstable_compression': ''}":                                                               lsm.CompressionNone,
		"{'sstable_compression': 'SnappyCompressor'}":                                               lsm.CompressionSnappy,
		"{'class': 'org.apache.cassandra.io.compress.NoopCompressor'}":                              lsm.CompressionNone,
	} {
		s.ok("ALTER TABLE t WITH compression = " + q)
		if c := compression("t"); c != want {
			t.Fatalf("%s: compression %v, want %v", q, c, want)
		}
	}
	for q, msg := range map[string]string{
		"{'class': 'BrotliCompressor'}":                              "Could not create Compression for type BrotliCompressor",
		"{'class': 'LZ4Compressor', 'level': 1}":                     "Unknown compression options [level]",
		"{'class': 'LZ4Compressor', 'compression_level': 3}":         "Unknown compression options [compression_level]",
		"{'class': 'ZstdCompressor', 'lz4_compressor_type': 'fast'}": "Unknown compression options [lz4_compressor_type]",
		"{'chunk_length_in_kb': 64}":                                 "Missing sub-option 'class'",
		"{'enabled': 'maybe', 'class': 'LZ4Compressor'}":             "Invalid value maybe",
		"{'enabled': 'false', 'class': 'BogusCompressor'}":           "If the 'enabled' option is set to false no other options must be specified",
		"{'class': 'LZ4Compressor', 'sstable_compression': 'Zstd'}":  "The 'sstable_compression' option must not be used",
		"{'class': ''}": "The 'class' option must not be empty",
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 'abc'}":                          "Invalid value abc for chunk_length_in_kb",
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 0}":                              "Invalid negative or null chunk_length_in_kb",
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 48}":                             "chunk_length_in_kb must be a power of 2",
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 4194304}":                        "chunk_length_in_kb is too large",
		"{'class': 'LZ4Compressor', 'chunk_length_in_kb': 16, 'chunk_length_kb': 16}":      "The 'chunk_length_kb' option must not be used",
		"{'class': 'LZ4Compressor', 'crc_check_chance': 9.9}":                              "crc_check_chance should be between 0.0 and 1.0",
		"{'class': 'LZ4Compressor', 'min_compress_ratio': 0.5}":                            "Can either be 0 or greater than or equal to 1",
		"{'class': 'LZ4Compressor', 'min_compress_ratio': 2, 'max_compressed_length': 16}": "Can not specify both",
		"{'class': 'ZstdCompressor', 'compression_level': 'nope'}":                         "compression_level must be between -131072 and 22",
		"{'class': 'ZstdCompressor', 'compression_level': 23}":                             "compression_level must be between -131072 and 22",
		"{'class': 'LZ4Compressor', 'lz4_compressor_type': 'medium'}":                      "Invalid compressor type 'medium'",
		"{'class': 'LZ4Compressor', 'lz4_high_compressor_level': 18}":                      "Value must be between 1 and 17",
	} {
		s.fails("ALTER TABLE t WITH compression = "+q, msg)
	}
	s.fails("CREATE TABLE u (k int PRIMARY KEY) WITH compression = {'enabled': 'false', 'chunk_length_in_kb': 16}", "no other options")
}

// TestCompressionFlush checks, through CQL, that an ALTER TABLE changes
// the compression of the sstables flushed afterwards, and that compaction
// rewrites the older ones with it.
func TestCompressionFlush(t *testing.T) {
	s := openTest(t, nil)
	s.ok("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	s.ok("USE ks")
	s.ok("CREATE TABLE t (k int PRIMARY KEY, v text)")
	tb := s.d.schema.Load().Keyspaces["ks"].Tables["t"]
	lt, err := s.d.engine.Table(tb.Storage)
	require.NoError(t, err)
	files := func() []string {
		t.Helper()
		out, err := lt.SSTableCompressions()
		require.NoError(t, err)
		slices.Sort(out)
		return out
	}

	s.ok("INSERT INTO t (k, v) VALUES (1, 'one')")
	require.NoError(t, lt.Flush())
	require.Equal(t, []string{"MinLZ"}, files())

	s.ok("ALTER TABLE t WITH compression = {'class': 'ZstdCompressor'}")
	s.ok("INSERT INTO t (k, v) VALUES (2, 'two')")
	require.NoError(t, lt.Flush())
	require.Equal(t, []string{"MinLZ", "ZSTD"}, files())

	require.NoError(t, lt.Compact())
	require.Equal(t, []string{"ZSTD"}, files())
	require.ElementsMatch(t, []string{"1 one", "2 two"}, s.rows("SELECT k, v FROM t"))

	s.ok("ALTER TABLE t WITH compression = {'enabled': false}")
	require.NoError(t, lt.Compact())
	require.Equal(t, []string{"NoCompression"}, files())
}
