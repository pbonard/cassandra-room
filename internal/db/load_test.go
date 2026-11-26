package db

import (
	"io"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestLoadIntoOtherTopology: the sstables of a three-node cluster (each
// node's share, replication factor one) load through the coordinator of a
// two-node cluster (replication factor two), which places every partition
// on its own replicas. Timestamps and deletions carry over.
func TestLoadIntoOtherTopology(t *testing.T) {
	src := newTestCluster(t, 3)
	src.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	src.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	for i := 0; i < 50; i++ {
		src.ok(i%3, one, "INSERT INTO ks.kv (k, v) VALUES (?, ?)", i, "v"+strconv.Itoa(i))
	}
	src.ok(0, one, "DELETE FROM ks.kv WHERE k = 7")

	dst := newTestCluster(t, 2)
	dst.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	dst.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	// Older than the source's delete: the deletion wins.
	dst.ok(0, quorum, "INSERT INTO ks.kv (k, v) VALUES (7, 'old') USING TIMESTAMP 1")

	// Every source node's snapshot, copied to the first target node.
	var files []string
	for i, d := range src.nodes {
		tables, err := d.Snapshot("b", []string{"ks"})
		require.NoError(t, err)
		require.Len(t, tables, 1)
		for j, f := range tables[0].Files {
			to := filepath.Join("load", strconv.Itoa(i)+"-"+strconv.Itoa(j)+".sst")
			copyFile(t, src.fs[i], f, dst.fs[0], to)
			files = append(files, to)
		}
	}
	require.NotEmpty(t, files)
	res, err := dst.nodes[0].Load("ks", "kv", files, all)
	require.NoError(t, err)
	require.EqualValues(t, 50, res.Entries)

	// Every row on both target nodes, read locally (ONE at each).
	for n := range dst.nodes {
		got := dst.values(n, one, "SELECT k FROM ks.kv")
		require.Len(t, got, 49, "node %d", n)
		require.Equal(t, []string{"v3"}, dst.values(n, one, "SELECT v FROM ks.kv WHERE k = 3"))
		require.Empty(t, dst.values(n, one, "SELECT v FROM ks.kv WHERE k = 7"))
	}
	_, err = dst.nodes[0].Load("ks", "nope", files, all)
	require.Error(t, err)
}

func copyFile(t *testing.T, from vfs.FS, path string, to vfs.FS, dst string) {
	t.Helper()
	require.NoError(t, to.MkdirAll(filepath.Dir(dst), 0o755))
	r, err := from.Open(path)
	require.NoError(t, err)
	defer r.Close()
	w, err := to.Create(dst, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = io.Copy(w, r)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}
