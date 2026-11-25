package manage

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/pbonard/cassandra-room/internal/db"
	"github.com/stretchr/testify/require"
)

func call(t *testing.T, srv *httptest.Server, method, path string, body any, out any) int {
	var r *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		r = bytes.NewReader(b)
	} else {
		r = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, srv.URL+path, r)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
	return resp.StatusCode
}

func exec(t *testing.T, d *db.DB, q string) message.Message {
	p, err := d.Prepare("", q)
	require.NoError(t, err)
	res := d.Execute(p, &db.RequestOptions{})
	if e, ok := res.(message.Error); ok {
		t.Fatalf("%s: %s", q, e.GetErrorMessage())
	}
	return res
}

func TestManagementAPI(t *testing.T) {
	d, err := db.Open("db", db.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer d.Close()
	serving := false
	srv := httptest.NewServer(Handler(d, func() bool { return serving }))
	defer srv.Close()

	require.Equal(t, http.StatusOK, call(t, srv, "GET", "/api/v1/health", nil, nil))
	require.Equal(t, http.StatusServiceUnavailable, call(t, srv, "GET", "/api/v1/ready", nil, nil), "not serving clients yet")
	serving = true
	require.Equal(t, http.StatusOK, call(t, srv, "GET", "/api/v1/ready", nil, nil))
	var node db.NodeInfo
	require.Equal(t, http.StatusOK, call(t, srv, "GET", "/api/v1/node", nil, &node))
	require.Equal(t, "NORMAL", node.Status)
	require.True(t, node.Self)

	exec(t, d, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	exec(t, d, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	exec(t, d, "INSERT INTO ks.kv (k, v) VALUES (1, 'a')")
	var keyspaces []string
	require.Equal(t, http.StatusOK, call(t, srv, "GET", "/api/v1/keyspaces", nil, &keyspaces))
	require.Equal(t, []string{"ks"}, keyspaces)
	var ring []db.RangeInfo
	require.Equal(t, http.StatusOK, call(t, srv, "GET", "/api/v1/ring?keyspace=ks", nil, &ring))
	require.Len(t, ring, 1)
	var e map[string]string
	require.Equal(t, http.StatusBadRequest, call(t, srv, "GET", "/api/v1/ring?keyspace=nope", nil, &e))
	require.Contains(t, e["error"], "does not exist")
	require.Equal(t, http.StatusOK, call(t, srv, "POST", "/api/v1/flush", nil, nil))

	// Snapshot, truncate, import.
	var tables []db.SnapshotTable
	require.Equal(t, http.StatusOK, call(t, srv, "POST", "/api/v1/snapshots", SnapshotRequest{Name: "s1", Keyspaces: []string{"ks"}}, &tables))
	require.Len(t, tables, 1)
	require.NotEmpty(t, tables[0].Files)
	exec(t, d, "TRUNCATE ks.kv")
	rows := exec(t, d, "SELECT k FROM ks.kv").(*message.RowsResult)
	require.Empty(t, rows.Data)
	require.Equal(t, http.StatusOK, call(t, srv, "POST", "/api/v1/import", ImportRequest{Keyspace: "ks", Table: "kv", Files: tables[0].Files}, nil))
	rows = exec(t, d, "SELECT v FROM ks.kv WHERE k = 1").(*message.RowsResult)
	require.Len(t, rows.Data, 1)
	require.Equal(t, http.StatusOK, call(t, srv, "DELETE", "/api/v1/snapshots/s1", nil, nil))
	require.Equal(t, http.StatusInternalServerError, call(t, srv, "POST", "/api/v1/import", ImportRequest{Keyspace: "ks", Table: "kv", Files: tables[0].Files}, &e), "the snapshot is gone")

	// Prometheus metrics.
	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	m := string(body)
	for _, want := range []string{
		`cassandra_room_client_request_duration_seconds_count{consistency="any",request="write"}`,
		`cassandra_room_client_request_duration_seconds_count{consistency="any",request="read"}`,
		`cassandra_room_client_request_duration_seconds_count{consistency="any",request="schema"}`,
		`cassandra_room_table_live_bytes{keyspace="ks",table="kv"}`,
		`cassandra_room_table_sstables{keyspace="ks",table="kv"}`,
		`cassandra_room_flushes_total`,
		`go_goroutines`,
	} {
		require.Contains(t, m, want)
	}

	// A repair needs a cluster.
	require.Equal(t, http.StatusInternalServerError, call(t, srv, "POST", "/api/v1/repair", RepairRequest{Keyspace: "ks", Start: "0", End: "10"}, &e))
	require.Contains(t, e["error"], "not a cluster node")
	require.Equal(t, http.StatusBadRequest, call(t, srv, "POST", "/api/v1/repair", RepairRequest{Keyspace: "ks", Start: "x", End: "1"}, &e))
}
