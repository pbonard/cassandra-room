package server

import (
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

// TestSchemaChangeEvents: a client registered for SCHEMA_CHANGE events is
// pushed one, on stream -1, for each keyspace, table and type created,
// changed or dropped (drivers that cache the schema, as gocql v2 and the
// Java driver do, refresh it on them); a client not registered is not.
func TestSchemaChangeEvents(t *testing.T) {
	srv := newTestServer(t)
	codec := frame.NewCodec()
	conn, _ := startPipeConn(srv)
	defer conn.Close()
	other, _ := startPipeConn(srv)
	defer other.Close()
	startup := &message.Startup{Options: map[string]string{"CQL_VERSION": "3.0.0"}}
	require.IsType(t, &message.Ready{}, roundTrip(t, codec, conn, 1, startup).Body.Message)
	require.IsType(t, &message.Ready{}, roundTrip(t, codec, other, 1, startup).Body.Message)
	require.IsType(t, &message.Ready{}, roundTrip(t, codec, conn, 2,
		&message.Register{EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange}}).Body.Message)

	next := func() *message.SchemaChangeEvent {
		t.Helper()
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		f, err := codec.DecodeFrame(conn)
		require.NoError(t, err)
		require.Equal(t, int16(-1), f.Header.StreamId)
		ev, ok := f.Body.Message.(*message.SchemaChangeEvent)
		require.True(t, ok, "%T", f.Body.Message)
		return ev
	}
	want := func(ct primitive.SchemaChangeType, target primitive.SchemaChangeTarget, ks, obj string) {
		t.Helper()
		require.Equal(t, &message.SchemaChangeEvent{ChangeType: ct, Target: target, Keyspace: ks, Object: obj}, next())
	}
	mustExec(t, srv, "", "CREATE KEYSPACE ev WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	want(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetKeyspace, "ev", "")
	mustExec(t, srv, "", "CREATE TABLE ev.t (k int PRIMARY KEY, v int)")
	want(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetTable, "ev", "t")
	mustExec(t, srv, "", "ALTER TABLE ev.t ADD w text")
	want(primitive.SchemaChangeTypeUpdated, primitive.SchemaChangeTargetTable, "ev", "t")
	mustExec(t, srv, "", "CREATE TYPE ev.addr (street text)")
	want(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetType, "ev", "addr")
	mustExec(t, srv, "", "ALTER KEYSPACE ev WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
	want(primitive.SchemaChangeTypeUpdated, primitive.SchemaChangeTargetKeyspace, "ev", "")
	mustExec(t, srv, "", "DROP TABLE ev.t")
	want(primitive.SchemaChangeTypeDropped, primitive.SchemaChangeTargetTable, "ev", "t")
	mustExec(t, srv, "", "DROP KEYSPACE ev")
	want(primitive.SchemaChangeTypeDropped, primitive.SchemaChangeTargetKeyspace, "ev", "")

	// The unregistered client got nothing: its next frame answers its
	// next request.
	require.IsType(t, &message.Supported{}, roundTrip(t, codec, other, 3, &message.Options{}).Body.Message)
}
