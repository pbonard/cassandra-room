package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/db"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/datastax/go-cassandra-native-protocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startPipeConn(srv *Server) (net.Conn, <-chan error) {
	clientConn, serverConn := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- srv.serveConn(serverConn)
	}()
	return clientConn, done
}

func roundTrip(t *testing.T, codec frame.Codec, conn net.Conn, streamID int16, reqMsg message.Message) *frame.Frame {
	t.Helper()
	reqFrame := frame.NewFrame(primitive.ProtocolVersion4, streamID, reqMsg)
	require.NoError(t, codec.EncodeFrame(reqFrame, conn), "encode request frame (%T)", reqMsg)
	respFrame, err := codec.DecodeFrame(conn)
	require.NoError(t, err, "decode response frame (%T)", reqMsg)
	require.Equal(t, streamID, respFrame.Header.StreamId)
	return respFrame
}

func roundTripCompressed(t *testing.T, compCodec frame.Codec, conn net.Conn, streamID int16, reqMsg message.Message) *frame.Frame {
	t.Helper()
	reqFrame := frame.NewFrame(primitive.ProtocolVersion4, streamID, reqMsg)
	reqFrame.Header.Flags = reqFrame.Header.Flags.Add(primitive.HeaderFlagCompressed)
	require.NoError(t, compCodec.EncodeFrame(reqFrame, conn), "encode compressed request frame (%T)", reqMsg)
	respFrame, err := compCodec.DecodeFrame(conn)
	require.NoError(t, err, "decode compressed response frame (%T)", reqMsg)
	require.Equal(t, streamID, respFrame.Header.StreamId)
	return respFrame
}

func newTestServer(t testing.TB) *Server {
	t.Helper()
	d, err := db.Open("db", db.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return New(d)
}

func mustExec(t testing.TB, srv *Server, ks, q string, values ...*primitive.Value) message.Message {
	t.Helper()
	p, err := srv.db.Prepare(ks, q)
	require.NoError(t, err, q)
	res := srv.db.Execute(p, &db.RequestOptions{Values: values})
	if e, ok := res.(message.Error); ok {
		t.Fatalf("%s: %s", q, e.GetErrorMessage())
	}
	return res
}

var largePayload = bytes.Repeat([]byte("X"), 200*1024) // 200 KiB > segment.MaxPayloadLength (128 KiB - 1)

// newFixtureServer returns a server with the keyspaces and tables the
// protocol tests query.
func newFixtureServer(t testing.TB) *Server {
	srv := newTestServer(t)
	keyspaces := []string{"cycling", "other_ks", "ks_comment", "ks_prepared", "explicit_v5_ks"}
	for i := 0; i < 8; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("ks_%d", i))
	}
	for _, ks := range keyspaces {
		mustExec(t, srv, "", "CREATE KEYSPACE "+ks+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	}
	for _, q := range []string{
		"CREATE TABLE cycling.riders (id int PRIMARY KEY, name text)",
		"CREATE TABLE cycling.slow_table (id int PRIMARY KEY)",
		"CREATE TABLE cycling.fast_table (id int PRIMARY KEY)",
		"CREATE TABLE cycling.hold (id int PRIMARY KEY)",
		"CREATE TABLE cycling.fast (id int PRIMARY KEY)",
		"CREATE TABLE cycling.large_rows (id int PRIMARY KEY, blob_col blob)",
		"CREATE TABLE ks_prepared.tbl (id int PRIMARY KEY)",
		"CREATE TABLE explicit_v5_ks.riders (id int PRIMARY KEY, name text)",
	} {
		mustExec(t, srv, "", q)
	}
	for i := 0; i < 8; i++ {
		mustExec(t, srv, "", fmt.Sprintf("CREATE TABLE ks_%d.riders (id int PRIMARY KEY)", i))
	}
	mustExec(t, srv, "cycling", "INSERT INTO large_rows (id, blob_col) VALUES (1, ?)", primitive.NewValue(largePayload))
	return srv
}

func TestNativeProtocolWithCQLParser(t *testing.T) {
	srv := newTestServer(t)
	for _, ks := range []string{"temp_ks", "other_ks"} {
		mustExec(t, srv, "", "CREATE KEYSPACE "+ks+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	}
	mustExec(t, srv, "other_ks", "CREATE TABLE riders (id uuid PRIMARY KEY, name text)")

	clientConn, done := startPipeConn(srv)
	codec := frame.NewCodec()

	// 1. OPTIONS -> SUPPORTED (v3, v4, and v5 advertised)
	resp := roundTrip(t, codec, clientConn, 1, &message.Options{})
	sup, ok := resp.Body.Message.(*message.Supported)
	require.True(t, ok, "expected *message.Supported, got %T", resp.Body.Message)
	require.Equal(t, []string{"3.4.7"}, sup.Options[message.StartupOptionCqlVersion])
	require.Equal(t, []string{"3/v3", "4/v4", "5/v5"}, sup.Options[message.SupportedProtocolVersions])

	// Verify that protocol v2 and DSE v1 are rejected with ProtocolError
	for _, ver := range []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersionDse1} {
		verResp := srv.handleFrameWithContext(context.Background(), &Session{}, frame.NewFrame(ver, 99, &message.Options{}))
		require.IsType(t, &message.ProtocolError{}, verResp.Body.Message, "version %v", ver)
	}

	// 2. STARTUP -> READY
	resp = roundTrip(t, codec, clientConn, 2, message.NewStartup())
	require.IsType(t, &message.Ready{}, resp.Body.Message)

	// 3. QUERY: CREATE KEYSPACE -> RESULT SCHEMA_CHANGE (CREATED KEYSPACE cycling)
	resp = roundTrip(t, codec, clientConn, 3, &message.Query{
		Query: "CREATE KEYSPACE IF NOT EXISTS cycling WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
	})
	sc, ok := resp.Body.Message.(*message.SchemaChangeResult)
	require.True(t, ok, "expected *message.SchemaChangeResult, got %T", resp.Body.Message)
	require.Equal(t, primitive.SchemaChangeTypeCreated, sc.ChangeType)
	require.Equal(t, primitive.SchemaChangeTargetKeyspace, sc.Target)
	require.Equal(t, "cycling", sc.Keyspace)

	// 4. Per-query Keyspace option applies to the query and does NOT permanently mutate session keyspace
	sess := &Session{}
	sess.setInitializedWithVersion(primitive.ProtocolVersion4)
	resp = srv.handleFrameWithContext(context.Background(), sess, frame.NewFrame(primitive.ProtocolVersion4, 4, &message.Query{
		Query:   "CREATE TABLE temp_riders (id uuid PRIMARY KEY);",
		Options: &message.QueryOptions{Keyspace: "temp_ks"},
	}))
	sc, ok = resp.Body.Message.(*message.SchemaChangeResult)
	require.True(t, ok)
	require.Equal(t, "temp_ks", sc.Keyspace)
	require.Empty(t, sess.Keyspace())

	// Subsequent unqualified query and PREPARE without USE must fail with Invalid because session keyspace is still empty
	resp = roundTrip(t, codec, clientConn, 5, &message.Query{
		Query: "SELECT * FROM temp_riders;",
	})
	require.IsType(t, &message.Invalid{}, resp.Body.Message)

	respPrepNoKS := roundTrip(t, codec, clientConn, 50, &message.Prepare{
		Query: "INSERT INTO temp_riders (id) VALUES (?);",
	})
	require.IsType(t, &message.Invalid{}, respPrepNoKS.Body.Message)

	// 5. QUERY: USE cycling -> RESULT SET_KEYSPACE
	resp = roundTrip(t, codec, clientConn, 6, &message.Query{
		Query: "USE cycling;",
	})
	sk, ok := resp.Body.Message.(*message.SetKeyspaceResult)
	require.True(t, ok)
	require.Equal(t, "cycling", sk.Keyspace)

	// 6. QUERY: CREATE TABLE (using session's active keyspace "cycling") -> RESULT SCHEMA_CHANGE (CREATED TABLE)
	resp = roundTrip(t, codec, clientConn, 7, &message.Query{
		Query: "CREATE TABLE riders (id uuid PRIMARY KEY, name text);",
	})
	sc, ok = resp.Body.Message.(*message.SchemaChangeResult)
	require.True(t, ok)
	require.Equal(t, primitive.SchemaChangeTargetTable, sc.Target)
	require.Equal(t, "cycling", sc.Keyspace)
	require.Equal(t, "riders", sc.Object)

	// 7. PREPARE: INSERT INTO riders (id, name) VALUES (:id, :name) -> RESULT PREPARED
	resp = roundTrip(t, codec, clientConn, 8, &message.Prepare{
		Query: "INSERT INTO riders (id, name) VALUES (:id, :name);",
	})
	prepRes, ok := resp.Body.Message.(*message.PreparedResult)
	require.True(t, ok)
	require.Len(t, prepRes.VariablesMetadata.Columns, 2)
	require.Equal(t, "id", prepRes.VariablesMetadata.Columns[0].Name)
	require.Equal(t, "name", prepRes.VariablesMetadata.Columns[1].Name)

	// Verify that preparing the same unqualified query in a different keyspace produces a distinct ID
	respUseOther := roundTrip(t, codec, clientConn, 9, &message.Query{
		Query: "USE other_ks;",
	})
	require.IsType(t, &message.SetKeyspaceResult{}, respUseOther.Body.Message)

	respOtherKS := roundTrip(t, codec, clientConn, 10, &message.Prepare{
		Query: "INSERT INTO riders (id, name) VALUES (:id, :name);",
	})
	prepOther, ok := respOtherKS.Body.Message.(*message.PreparedResult)
	require.True(t, ok)
	require.NotEqual(t, prepRes.PreparedQueryId, prepOther.PreparedQueryId)

	riderID := []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}
	// 8. EXECUTE the first prepared statement (prepared in "cycling") even though session is now in "other_ks"
	resp = roundTrip(t, codec, clientConn, 11, &message.Execute{
		QueryId:          prepRes.PreparedQueryId,
		ResultMetadataId: prepRes.ResultMetadataId,
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelOne,
			NamedValues: map[string]*primitive.Value{
				"name": primitive.NewValue([]byte("Alice")),
				"id":   primitive.NewValue(riderID),
			},
		},
	})
	require.IsType(t, &message.VoidResult{}, resp.Body.Message)
	check := roundTrip(t, codec, clientConn, 111, &message.Query{Query: "SELECT name FROM cycling.riders WHERE id = 123e4567-e89b-12d3-a456-426614174000"})
	require.Equal(t, message.RowSet{{[]byte("Alice")}}, check.Body.Message.(*message.RowsResult).Data)
	// A value of the wrong size for its type is rejected.
	bad := roundTrip(t, codec, clientConn, 112, &message.Execute{
		QueryId: prepRes.PreparedQueryId,
		Options: &message.QueryOptions{NamedValues: map[string]*primitive.Value{
			"name": primitive.NewValue([]byte("Alice")), "id": primitive.NewValue([]byte("uuid-bytes")),
		}},
	})
	require.IsType(t, &message.Invalid{}, bad.Body.Message)

	// Verify missing or mismatched bind values return Invalid instead of nil values
	respMissingBind := roundTrip(t, codec, clientConn, 12, &message.Execute{
		QueryId:          prepRes.PreparedQueryId,
		ResultMetadataId: prepRes.ResultMetadataId,
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelOne,
			NamedValues: map[string]*primitive.Value{
				"id": primitive.NewValue(riderID),
			},
		},
	})
	require.IsType(t, &message.Invalid{}, respMissingBind.Body.Message)

	// Switch back to cycling for remaining queries
	_ = roundTrip(t, codec, clientConn, 13, &message.Query{Query: "USE cycling;"})

	// 9. QUERY: SELECT COUNT(*), id AS rider_id, Name FROM riders -> RESULT ROWS with proper column names
	resp = roundTrip(t, codec, clientConn, 14, &message.Query{
		Query: `SELECT COUNT(*), id AS rider_id, Name FROM riders WHERE id = 123e4567-e89b-12d3-a456-426614174000;`,
	})
	rowsRes, ok := resp.Body.Message.(*message.RowsResult)
	require.True(t, ok)
	require.Equal(t, int32(3), rowsRes.Metadata.ColumnCount)
	require.Equal(t, "count", rowsRes.Metadata.Columns[0].Name)
	require.Equal(t, "rider_id", rowsRes.Metadata.Columns[1].Name)
	require.Equal(t, "name", rowsRes.Metadata.Columns[2].Name)
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte(rowsRes.Data[0][0]))
	resp = roundTrip(t, codec, clientConn, 140, &message.Query{Query: `SELECT "ExactCol" FROM riders`})
	require.IsType(t, &message.Invalid{}, resp.Body.Message)

	// 10. BATCH with both raw CQL and prepared ID -> RESULT VOID
	resp = roundTrip(t, codec, clientConn, 15, &message.Batch{
		Type:        primitive.BatchTypeLogged,
		Consistency: primitive.ConsistencyLevelOne,
		Children: []*message.BatchChild{
			{
				Id: prepRes.PreparedQueryId,
				Values: []*primitive.Value{
					primitive.NewValue(append([]byte{0x22}, riderID[1:]...)),
					primitive.NewValue([]byte("Bob")),
				},
			},
			{
				Query: "DELETE FROM riders WHERE id = 123e4567-e89b-12d3-a456-426614174000;",
			},
		},
	})
	require.IsType(t, &message.VoidResult{}, resp.Body.Message)

	// 11. QUERY with invalid CQL -> ERROR SYNTAX_ERROR (0x2000)
	resp = roundTrip(t, codec, clientConn, 16, &message.Query{
		Query: "SELEC BROKEN SYNTAX FROM;",
	})
	synErr, ok := resp.Body.Message.(*message.SyntaxError)
	require.True(t, ok)
	require.Equal(t, primitive.ErrorCodeSyntaxError, synErr.GetErrorCode())

	require.NoError(t, clientConn.Close())
	require.NoError(t, <-done)
}

func TestMultiConnectionAndStreamMultiplexing(t *testing.T) {
	slowDone := make(chan struct{})
	slowStarted := make(chan struct{})
	var once sync.Once

	srv := newFixtureServer(t)
	srv.testHook = func(_ context.Context, q string) error {
		if strings.Contains(q, "slow_table") {
			once.Do(func() { close(slowStarted) })
			<-slowDone
		}
		return nil
	}

	// Test 1: In-connection stream multiplexing (fast stream finishes while slow stream is blocked on the SAME connection)
	clientConn, done := startPipeConn(srv)
	codec := frame.NewCodec()
	_ = roundTrip(t, codec, clientConn, 1, message.NewStartup())

	// Send slow query on stream 10
	require.NoError(t, codec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 10, &message.Query{
		Query: "SELECT id FROM cycling.slow_table WHERE id = 1;",
	}), clientConn))
	<-slowStarted

	// Send fast query on stream 20 over the SAME connection while stream 10 is still blocked
	require.NoError(t, codec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 20, &message.Query{
		Query: "SELECT id FROM cycling.fast_table WHERE id = 2;",
	}), clientConn))

	// Stream 20 should arrive FIRST (out-of-order completion!)
	firstResp, err := codec.DecodeFrame(clientConn)
	require.NoError(t, err)
	require.Equal(t, int16(20), firstResp.Header.StreamId)

	// Unblock stream 10 and read its response
	close(slowDone)
	secondResp, err := codec.DecodeFrame(clientConn)
	require.NoError(t, err)
	require.Equal(t, int16(10), secondResp.Header.StreamId)

	require.NoError(t, clientConn.Close())
	require.NoError(t, <-done)

	// Test 2: Multiple concurrent connections
	const numConns = 8
	const streamsPerConn = 16
	var connWG sync.WaitGroup
	errCh := make(chan error, numConns*streamsPerConn)

	for c := 0; c < numConns; c++ {
		cConn, _ := startPipeConn(srv)

		connWG.Add(1)
		go func(connIdx int, conn net.Conn) {
			defer connWG.Done()
			defer conn.Close()

			cCodec := frame.NewCodec()
			if err := cCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 0, message.NewStartup()), conn); err != nil {
				errCh <- err
				return
			}
			if _, err := cCodec.DecodeFrame(conn); err != nil {
				errCh <- err
				return
			}
			// Pipeline streamsPerConn requests before reading responses
			go func() {
				for s := 1; s <= streamsPerConn; s++ {
					f := frame.NewFrame(primitive.ProtocolVersion4, int16(s), &message.Query{
						Query: fmt.Sprintf("SELECT id FROM ks_%d.riders WHERE id = %d;", connIdx, s),
					})
					if err := cCodec.EncodeFrame(f, conn); err != nil {
						errCh <- err
						return
					}
				}
			}()

			seen := make(map[int16]bool, streamsPerConn)
			for s := 1; s <= streamsPerConn; s++ {
				rf, err := cCodec.DecodeFrame(conn)
				if err != nil {
					errCh <- err
					return
				}
				seen[rf.Header.StreamId] = true
			}
			if len(seen) != streamsPerConn {
				errCh <- fmt.Errorf("conn %d: expected %d responses, got %d", connIdx, streamsPerConn, len(seen))
			}
		}(c, cConn)
	}

	connWG.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.NoError(t, srv.Shutdown(context.Background()))
}

func TestAdmissionControlThrowOnOverload(t *testing.T) {
	blockCh := make(chan struct{})
	startedCh := make(chan struct{})

	srv := newFixtureServer(t)
	srv.testHook = func(_ context.Context, q string) error {
		if strings.Contains(q, "cycling.hold") {
			close(startedCh)
			<-blockCh
		}
		return nil
	}
	srv.SetMaxConcurrentRequests(1)

	// Session 1 occupies the single global admission slot
	s1 := &Session{}
	s1.setInitializedWithVersion(primitive.ProtocolVersion4)
	done1 := make(chan *frame.Frame, 1)
	go func() {
		done1 <- srv.handleFrameWithContext(context.Background(), s1, frame.NewFrame(primitive.ProtocolVersion4, 1, &message.Query{
			Query: "SELECT id FROM cycling.hold;",
		}))
	}()
	<-startedCh

	// Session 2 has THROW_ON_OVERLOAD=1 set via STARTUP and should immediately receive Overloaded (0x1001)
	s2 := &Session{}
	_ = srv.handleFrameWithContext(context.Background(), s2, frame.NewFrame(primitive.ProtocolVersion4, 1, message.NewStartup(
		message.StartupOptionThrowOnOverload, "1",
	)))

	overloadedResp := srv.handleFrameWithContext(context.Background(), s2, frame.NewFrame(primitive.ProtocolVersion4, 2, &message.Query{
		Query: "SELECT id FROM cycling.fast;",
	}))
	require.IsType(t, &message.Overloaded{}, overloadedResp.Body.Message)

	close(blockCh)
	<-done1
}

func TestFrameBoundariesAndMaliciousLengths(t *testing.T) {
	srv := newFixtureServer(t)
	clientConn, done := startPipeConn(srv)
	rawCodec := frame.NewRawCodec()
	_ = roundTrip(t, rawCodec, clientConn, 1, message.NewStartup())

	// 1. Send a tiny 13-byte QUERY frame on stream 42 whose inner [long string] claims 0x7FFFFFFF (~2 GiB).
	// The server must reject it with ProtocolError on stream 42 without allocating 2 GiB or closing the connection.
	maliciousBody := []byte{0x7F, 0xFF, 0xFF, 0xFF}
	maliciousRaw := &frame.RawFrame{
		Header: &frame.Header{
			Version:    primitive.ProtocolVersion4,
			StreamId:   42,
			OpCode:     primitive.OpCodeQuery,
			BodyLength: int32(len(maliciousBody)),
		},
		Body: maliciousBody,
	}
	require.NoError(t, rawCodec.EncodeRawFrame(maliciousRaw, clientConn))
	resp, err := rawCodec.DecodeFrame(clientConn)
	require.NoError(t, err)
	require.Equal(t, int16(42), resp.Header.StreamId)
	require.IsType(t, &message.ProtocolError{}, resp.Body.Message)

	// 1b. Send a tiny 13-byte AUTH_RESPONSE frame on stream 45 claiming a 1 GiB token.
	// It must be rejected with ProtocolError before ConvertFromRawFrame can allocate 1 GiB.
	var msBefore, msAfter runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	maliciousAuth := &frame.RawFrame{
		Header: &frame.Header{
			Version:    primitive.ProtocolVersion4,
			StreamId:   45,
			OpCode:     primitive.OpCodeAuthResponse,
			BodyLength: 4,
		},
		Body: []byte{0x40, 0x00, 0x00, 0x00}, // 1 GiB
	}
	require.NoError(t, rawCodec.EncodeRawFrame(maliciousAuth, clientConn))
	respAuth, err := rawCodec.DecodeFrame(clientConn)
	require.NoError(t, err)
	runtime.ReadMemStats(&msAfter)
	require.Equal(t, int16(45), respAuth.Header.StreamId)
	require.IsType(t, &message.ProtocolError{}, respAuth.Body.Message)
	require.LessOrEqual(t, msAfter.TotalAlloc-msBefore.TotalAlloc, uint64(10*1024*1024))

	// 2. Send a valid frame with extra trailing garbage bytes inside BodyLength on stream 43.
	// The stream must stay aligned so the next frame (stream 44) succeeds cleanly.
	var validQueryBuf bytes.Buffer
	validFrame := frame.NewFrame(primitive.ProtocolVersion4, 43, &message.Query{
		Query: "USE cycling;",
	})
	require.NoError(t, rawCodec.EncodeFrame(validFrame, &validQueryBuf))
	encodedBytes := validQueryBuf.Bytes()
	trailingGarbage := []byte("GARBAGE!")
	origLen := binary.BigEndian.Uint32(encodedBytes[5:9])
	binary.BigEndian.PutUint32(encodedBytes[5:9], origLen+uint32(len(trailingGarbage)))
	encodedBytes = append(encodedBytes, trailingGarbage...)

	_, err = clientConn.Write(encodedBytes)
	require.NoError(t, err)
	resp43, err := rawCodec.DecodeFrame(clientConn)
	require.NoError(t, err)
	require.Equal(t, int16(43), resp43.Header.StreamId)

	// Subsequent frame on stream 44 must still be aligned and succeed!
	resp44 := roundTrip(t, rawCodec, clientConn, 44, &message.Query{
		Query: "SELECT id FROM riders;",
	})
	require.IsType(t, &message.RowsResult{}, resp44.Body.Message)

	require.NoError(t, clientConn.Close())
	<-done
}

func TestUseBarrierWithCommentsAndPreparedExecute(t *testing.T) {
	srv := newFixtureServer(t)
	clientConn, done := startPipeConn(srv)
	codec := frame.NewCodec()
	_ = roundTrip(t, codec, clientConn, 0, message.NewStartup())

	// 1. USE with leading comment must act as a barrier and set keyspace
	resp := roundTrip(t, codec, clientConn, 1, &message.Query{
		Query: "/* leading comment */ USE ks_comment;",
	})
	sk, ok := resp.Body.Message.(*message.SetKeyspaceResult)
	require.True(t, ok)
	require.Equal(t, "ks_comment", sk.Keyspace)

	// 2. PREPARE a USE statement and EXECUTE it; it must act as a barrier and switch keyspace
	prepResp := roundTrip(t, codec, clientConn, 2, &message.Prepare{
		Query: "USE ks_prepared;",
	})
	prepRes, ok := prepResp.Body.Message.(*message.PreparedResult)
	require.True(t, ok)

	execResp := roundTrip(t, codec, clientConn, 3, &message.Execute{
		QueryId: prepRes.PreparedQueryId,
	})
	sk, ok = execResp.Body.Message.(*message.SetKeyspaceResult)
	require.True(t, ok)
	require.Equal(t, "ks_prepared", sk.Keyspace)

	// Verify unqualified SELECT now resolves to ks_prepared
	selResp := roundTrip(t, codec, clientConn, 4, &message.Query{
		Query: "SELECT id FROM tbl;",
	})
	rows, ok := selResp.Body.Message.(*message.RowsResult)
	require.True(t, ok)
	require.Len(t, rows.Metadata.Columns, 1)
	require.Equal(t, "ks_prepared", rows.Metadata.Columns[0].Keyspace)

	require.NoError(t, clientConn.Close())
	<-done
}

func TestCloseAndShutdownCancelBlockedHandler(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan error, 1)

	srv := newFixtureServer(t)
	srv.testHook = func(ctx context.Context, _ string) error {
		close(handlerStarted)
		<-ctx.Done()
		handlerCanceled <- ctx.Err()
		return ctx.Err()
	}

	clientConn, _ := startPipeConn(srv)
	codec := frame.NewCodec()
	_ = roundTrip(t, codec, clientConn, 1, message.NewStartup())
	require.NoError(t, codec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 7, &message.Query{
		Query: "SELECT id FROM cycling.riders;",
	}), clientConn))

	<-handlerStarted

	// Calling Shutdown with a short deadline should cancel req.Ctx when the deadline expires
	// and wait for the direct serveConn call to finish.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, srv.Shutdown(ctx))

	select {
	case cErr := <-handlerCanceled:
		require.Error(t, cErr)
	case <-time.After(time.Second):
		t.Fatal("blocked handler was not canceled by Shutdown")
	}
	_ = clientConn.Close()
}

func TestGracefulShutdownDeliversInFlightResponses(t *testing.T) {
	handlerStarted := make(chan struct{})

	srv := newFixtureServer(t)
	srv.testHook = func(context.Context, string) error {
		close(handlerStarted)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	clientConn, _ := startPipeConn(srv)
	codec := frame.NewCodec()
	_ = roundTrip(t, codec, clientConn, 1, message.NewStartup())
	require.NoError(t, codec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 19, &message.Query{
		Query: "SELECT id FROM cycling.riders;",
	}), clientConn))

	<-handlerStarted

	// Start reading the response on the client while Shutdown is called concurrently
	respCh := make(chan *frame.Frame, 1)
	errCh := make(chan error, 1)
	go func() {
		rf, err := codec.DecodeFrame(clientConn)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- rf
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	select {
	case err := <-errCh:
		require.NoError(t, err, "expected client to receive in-flight response before EOF")
	case rf := <-respCh:
		require.Equal(t, int16(19), rf.Header.StreamId)
		require.IsType(t, &message.RowsResult{}, rf.Body.Message)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight response")
	}

	// After Shutdown completes, subsequent reads from clientConn should return EOF
	_, err := codec.DecodeFrame(clientConn)
	require.ErrorIs(t, err, io.EOF)
	_ = clientConn.Close()
}

func FuzzRawFrameBounds(f *testing.F) {
	f.Add(uint8(primitive.OpCodeQuery), uint8(0), []byte{0x7F, 0xFF, 0xFF, 0xFF})
	f.Add(uint8(primitive.OpCodeAuthResponse), uint8(0), []byte{0x40, 0x00, 0x00, 0x00})
	f.Add(uint8(primitive.OpCodeExecute), uint8(0), []byte{0xFF, 0xFF})
	f.Add(uint8(primitive.OpCodeBatch), uint8(0), []byte{0x00, 0x00, 0x01, 0x00, 0x7F, 0xFF, 0xFF, 0xFF})
	f.Add(uint8(primitive.OpCodeStartup), uint8(primitive.HeaderFlagCustomPayload), []byte{0x00, 0x01, 0x00, 0x01, 'a', 0x7F, 0xFF, 0xFF, 0xFF})

	srv := newTestServer(f)
	rawCodec := frame.NewRawCodec()

	f.Fuzz(func(t *testing.T, opCode uint8, flags uint8, body []byte) {
		if len(body) > 4096 {
			return
		}
		cConn, done := startPipeConn(srv)

		var msBefore, msAfter runtime.MemStats
		runtime.ReadMemStats(&msBefore)

		rf := &frame.RawFrame{
			Header: &frame.Header{
				Version:    primitive.ProtocolVersion4,
				Flags:      primitive.HeaderFlag(flags),
				StreamId:   1,
				OpCode:     primitive.OpCode(opCode),
				BodyLength: int32(len(body)),
			},
			Body: body,
		}
		if err := rawCodec.EncodeRawFrame(rf, cConn); err == nil {
			_, _ = rawCodec.DecodeFrame(cConn)
		}
		_ = cConn.Close()
		<-done

		runtime.ReadMemStats(&msAfter)
		assert.LessOrEqual(t, msAfter.TotalAlloc-msBefore.TotalAlloc, uint64(16*1024*1024),
			"excessive allocation for %d-byte frame (opcode=%#x, flags=%#x)", len(body), opCode, flags)
	})
}

func TestCompressionLZ4AndSnappy(t *testing.T) {
	srv := newFixtureServer(t)

	cases := []struct {
		name string
		comp compression.Compressor
	}{
		{name: "lz4", comp: lz4.Compressor{}},
		{name: "snappy", comp: snappy.Compressor{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clientConn, done := startPipeConn(srv)

			plainCodec := frame.NewRawCodec()
			compCodec := frame.NewRawCodecWithCompression(tc.comp)

			// 1. OPTIONS must advertise both lz4 and snappy
			optResp := roundTrip(t, plainCodec, clientConn, 1, &message.Options{})
			sup, ok := optResp.Body.Message.(*message.Supported)
			require.True(t, ok)
			require.Equal(t, []string{"lz4", "snappy"}, sup.Options[message.StartupOptionCompression])

			// 1b. Sending a compressed frame BEFORE STARTUP negotiation must return ProtocolError
			preNegReq := frame.NewFrame(primitive.ProtocolVersion4, 10, &message.Query{
				Query: "USE cycling;",
			})
			preNegReq.Header.Flags = preNegReq.Header.Flags.Add(primitive.HeaderFlagCompressed)
			require.NoError(t, compCodec.EncodeFrame(preNegReq, clientConn))
			preNegResp, err := plainCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.ProtocolError{}, preNegResp.Body.Message)

			// 2. STARTUP with unsupported compression algorithm must return ProtocolError
			badStartup := roundTrip(t, plainCodec, clientConn, 2, message.NewStartup(
				message.StartupOptionCompression, "zstd",
			))
			require.IsType(t, &message.ProtocolError{}, badStartup.Body.Message)

			// 3. STARTUP with valid compression algorithm -> READY (uncompressed empty body)
			readyResp := roundTrip(t, plainCodec, clientConn, 3, message.NewStartup(
				message.StartupOptionCompression, tc.name,
				message.StartupOptionThrowOnOverload, "1",
			))
			require.IsType(t, &message.Ready{}, readyResp.Body.Message)

			// 3b. A second STARTUP on an already-initialized connection must be rejected with ProtocolError
			dupStartup := frame.NewFrame(primitive.ProtocolVersion4, 30, message.NewStartup(
				message.StartupOptionCompression, "snappy",
			))
			require.NoError(t, plainCodec.EncodeFrame(dupStartup, clientConn))
			dupResp, err := compCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.ProtocolError{}, dupResp.Body.Message)

			// 4. Send compressed USE query (must be recognized as a barrier and return compressed SetKeyspaceResult)
			useResp := roundTripCompressed(t, compCodec, clientConn, 4, &message.Query{
				Query: "/* comment */ USE cycling;",
			})
			require.True(t, useResp.Header.Flags.Contains(primitive.HeaderFlagCompressed))
			sk, ok := useResp.Body.Message.(*message.SetKeyspaceResult)
			require.True(t, ok)
			require.Equal(t, "cycling", sk.Keyspace)

			// 5. Send compressed SELECT query -> compressed RowsResult
			selResp := roundTripCompressed(t, compCodec, clientConn, 5, &message.Query{
				Query: "SELECT id, name FROM riders WHERE id = 1;",
			})
			require.True(t, selResp.Header.Flags.Contains(primitive.HeaderFlagCompressed))
			rows, ok := selResp.Body.Message.(*message.RowsResult)
			require.True(t, ok)
			require.Equal(t, int32(2), rows.Metadata.ColumnCount)

			// 6. Zip-bomb protection: send a tiny compressed frame claiming ~2 GiB uncompressed size
			var bombBody []byte
			if tc.name == "lz4" {
				bombBody = []byte{0x7F, 0xFF, 0xFF, 0xFF, 0x00}
			} else {
				// Snappy uvarint for 2 GiB (0x80000000)
				bombBody = []byte{0x80, 0x80, 0x80, 0x80, 0x08}
			}
			bombRaw := &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					Flags:      primitive.HeaderFlagCompressed,
					StreamId:   6,
					OpCode:     primitive.OpCodeQuery,
					BodyLength: int32(len(bombBody)),
				},
				Body: bombBody,
			}
			require.NoError(t, plainCodec.EncodeRawFrame(bombRaw, clientConn))
			bombResp, err := compCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.ProtocolError{}, bombResp.Body.Message)

			// 7. Corrupt compressed payload -> ProtocolError
			var corruptBody []byte
			if tc.name == "lz4" {
				// Valid uncompressed length (32 bytes), followed by invalid LZ4 offset/token
				corruptBody = []byte{0x00, 0x00, 0x00, 0x20, 0x0F, 0x00, 0x00}
			} else {
				// Valid Snappy uvarint length (32 bytes), followed by invalid Snappy tag
				corruptBody = []byte{0x20, 0xFF, 0xFF, 0xFF}
			}
			corruptRaw := &frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion4,
					Flags:      primitive.HeaderFlagCompressed,
					StreamId:   7,
					OpCode:     primitive.OpCodeQuery,
					BodyLength: int32(len(corruptBody)),
				},
				Body: corruptBody,
			}
			require.NoError(t, plainCodec.EncodeRawFrame(corruptRaw, clientConn))
			corruptResp, err := compCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.ProtocolError{}, corruptResp.Body.Message)

			// 8. Mismatched LZ4 uncompressed length header -> ProtocolError
			if tc.name == "lz4" {
				validCompressed, err := tc.comp.CompressFrame([]byte("short payload"))
				require.NoError(t, err)
				// Tamper with the 4-byte uncompressed length header so it claims 200 bytes instead of 13
				binary.BigEndian.PutUint32(validCompressed[:4], 200)
				mismatchRaw := &frame.RawFrame{
					Header: &frame.Header{
						Version:    primitive.ProtocolVersion4,
						Flags:      primitive.HeaderFlagCompressed,
						StreamId:   8,
						OpCode:     primitive.OpCodeQuery,
						BodyLength: int32(len(validCompressed)),
					},
					Body: validCompressed,
				}
				require.NoError(t, plainCodec.EncodeRawFrame(mismatchRaw, clientConn))
				mismatchResp, err := compCodec.DecodeFrame(clientConn)
				require.NoError(t, err)
				require.IsType(t, &message.ProtocolError{}, mismatchResp.Body.Message)
			}

			// 9. In-flight byte limit must count the DECOMPRESSED size, not just the compressed wire size.
			// Set byte budget to 512 bytes and send a repetitive ~2.5 KB query that compresses to < 100 bytes.
			srv.SetMaxInFlightBytes(512)
			defer srv.SetMaxInFlightBytes(DefaultMaxInFlightBytes)

			repetitiveQuery := "SELECT id FROM riders WHERE name = '" + strings.Repeat("A", 2500) + "' ALLOW FILTERING;"
			overloadResp := roundTripCompressed(t, compCodec, clientConn, 9, &message.Query{
				Query: repetitiveQuery,
			})
			require.IsType(t, &message.Overloaded{}, overloadResp.Body.Message)

			require.NoError(t, clientConn.Close())
			<-done

			// 10. Without THROW_ON_OVERLOAD=1, a single compressed frame whose decompressed size
			// exceeds MaxInFlightBytes must succeed via the activeBytes == 0 exception without
			// deadlocking against its own initial compressed-size reservation.
			cConn2, done2 := startPipeConn(srv)
			require.NoError(t, cConn2.SetDeadline(time.Now().Add(2*time.Second)))

			readyResp2 := roundTrip(t, plainCodec, cConn2, 1, message.NewStartup(
				message.StartupOptionCompression, tc.name,
			))
			require.IsType(t, &message.Ready{}, readyResp2.Body.Message)

			bigQualResp := roundTripCompressed(t, compCodec, cConn2, 2, &message.Query{
				Query: "SELECT id FROM cycling.riders WHERE name = '" + strings.Repeat("B", 2500) + "' ALLOW FILTERING;",
			})
			require.IsType(t, &message.RowsResult{}, bigQualResp.Body.Message)

			require.NoError(t, cConn2.Close())
			<-done2
		})
	}
}

func TestProtocolV5SegmentFraming(t *testing.T) {
	srv := newFixtureServer(t)

	for _, compName := range []string{"", "lz4"} {
		name := compName
		if name == "" {
			name = "uncompressed"
		}
		t.Run(name, func(t *testing.T) {
			clientConn, done := startPipeConn(srv)
			rawCodec := frame.NewRawCodec()

			var segCodec segment.Codec
			if compName == "lz4" {
				segCodec = segment.NewCodecWithCompression(lz4.Compressor{})
			} else {
				segCodec = segment.NewCodec()
			}

			// 1. v5 OPTIONS frame (before STARTUP, still standard frame format)
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 1, &message.Options{}), clientConn))
			optResp, err := rawCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			sup, ok := optResp.Body.Message.(*message.Supported)
			require.True(t, ok)
			require.Equal(t, []string{"lz4"}, sup.Options[message.StartupOptionCompression])
			require.Equal(t, []string{"3/v3", "4/v4", "5/v5"}, sup.Options[message.SupportedProtocolVersions])

			// 1b. v5 REGISTER before STARTUP must be rejected with ProtocolError and must NOT switch to segment framing
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 11, &message.Register{
				EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange},
			}), clientConn))
			regResp, err := rawCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.ProtocolError{}, regResp.Body.Message)

			// 2. v5 STARTUP with snappy must be rejected with ProtocolError (and stay in pre-READY frame mode)
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 2, message.NewStartup(
				message.StartupOptionCompression, "snappy",
			)), clientConn))
			snappyResp, err := rawCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.ProtocolError{}, snappyResp.Body.Message)

			// 3. v5 STARTUP -> READY (returned as a standard frame, then connection switches to v5 segments)
			startup := message.NewStartup()
			if compName != "" {
				startup = message.NewStartup(message.StartupOptionCompression, compName)
			}
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 3, startup), clientConn))
			readyResp, err := rawCodec.DecodeFrame(clientConn)
			require.NoError(t, err)
			require.IsType(t, &message.Ready{}, readyResp.Body.Message)

			// Helper to decode a single frame from a response v5 segment
			decodeSegmentFrame := func() *frame.Frame {
				seg, err := segCodec.DecodeSegment(clientConn)
				require.NoError(t, err)
				require.True(t, seg.Header.IsSelfContained)
				rf, err := rawCodec.DecodeFrame(bytes.NewReader(seg.Payload.UncompressedData))
				require.NoError(t, err)
				require.Equal(t, primitive.ProtocolVersion5, rf.Header.Version)
				return rf
			}

			// 4. Send a self-contained v5 segment packing TWO frames back-to-back (USE cycling; + PREPARE with v5 Keyspace)
			var packedBuf bytes.Buffer
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 4, &message.Query{
				Query: "USE cycling;",
			}), &packedBuf))
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 5, &message.Prepare{
				Query:    "INSERT INTO riders (id, name) VALUES (:id, :name);",
				Keyspace: "explicit_v5_ks",
			}), &packedBuf))

			require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
				Header:  &segment.Header{IsSelfContained: true},
				Payload: &segment.Payload{UncompressedData: packedBuf.Bytes()},
			}, clientConn))

			resp4 := decodeSegmentFrame()
			require.Equal(t, int16(4), resp4.Header.StreamId)
			sk, ok := resp4.Body.Message.(*message.SetKeyspaceResult)
			require.True(t, ok)
			require.Equal(t, "cycling", sk.Keyspace)

			resp5 := decodeSegmentFrame()
			require.Equal(t, int16(5), resp5.Header.StreamId)
			prepRes, ok := resp5.Body.Message.(*message.PreparedResult)
			require.True(t, ok)
			require.NotEmpty(t, prepRes.ResultMetadataId)

			// 5. Send v5 EXECUTE with ResultMetadataId in a self-contained segment
			var execBuf bytes.Buffer
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 6, &message.Execute{
				QueryId:          prepRes.PreparedQueryId,
				ResultMetadataId: prepRes.ResultMetadataId,
				Options: &message.QueryOptions{
					Consistency: primitive.ConsistencyLevelOne,
					NamedValues: map[string]*primitive.Value{
						"id":   primitive.NewValue([]byte{0, 0, 0, 5}),
						"name": primitive.NewValue([]byte("v5-rider")),
					},
				},
			}), &execBuf))
			require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
				Header:  &segment.Header{IsSelfContained: true},
				Payload: &segment.Payload{UncompressedData: execBuf.Bytes()},
			}, clientConn))

			resp6 := decodeSegmentFrame()
			require.Equal(t, int16(6), resp6.Header.StreamId)
			require.IsType(t, &message.VoidResult{}, resp6.Body.Message)

			// 6. Send a multi-segment frame (IsSelfContained: false) split across 3 segments
			var multiFrameBuf bytes.Buffer
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 7, &message.Query{
				Query: "SELECT id, name FROM riders WHERE name = '" + strings.Repeat("V", 512) + "' ALLOW FILTERING;",
			}), &multiFrameBuf))
			fullBytes := multiFrameBuf.Bytes()
			part1 := fullBytes[:15]
			part2 := fullBytes[15:100]
			part3 := fullBytes[100:]
			for _, part := range [][]byte{part1, part2, part3} {
				require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
					Header:  &segment.Header{IsSelfContained: false},
					Payload: &segment.Payload{UncompressedData: part},
				}, clientConn))
			}

			resp7 := decodeSegmentFrame()
			require.Equal(t, int16(7), resp7.Header.StreamId)
			rows7, ok := resp7.Body.Message.(*message.RowsResult)
			require.True(t, ok)
			require.Equal(t, int32(2), rows7.Metadata.ColumnCount)

			// 7. Server sending a response frame > 128 KiB (segment.MaxPayloadLength = 131071) splits across multiple segments
			var largeQueryBuf bytes.Buffer
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 8, &message.Query{
				Query: "SELECT blob_col FROM cycling.large_rows;",
			}), &largeQueryBuf))
			require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
				Header:  &segment.Header{IsSelfContained: true},
				Payload: &segment.Payload{UncompressedData: largeQueryBuf.Bytes()},
			}, clientConn))

			var reassembledResp bytes.Buffer
			segCount := 0
			for {
				seg, err := segCodec.DecodeSegment(clientConn)
				require.NoError(t, err)
				require.False(t, seg.Header.IsSelfContained, "expected multi-segment split response")
				reassembledResp.Write(seg.Payload.UncompressedData)
				segCount++
				if reassembledResp.Len() >= 9 {
					bodyLen := int(binary.BigEndian.Uint32(reassembledResp.Bytes()[5:9]))
					if reassembledResp.Len() == 9+bodyLen {
						break
					}
				}
			}
			require.GreaterOrEqual(t, segCount, 2)
			largeRespFrame, err := rawCodec.DecodeFrame(&reassembledResp)
			require.NoError(t, err)
			require.Equal(t, int16(8), largeRespFrame.Header.StreamId)
			largeRows, ok := largeRespFrame.Body.Message.(*message.RowsResult)
			require.True(t, ok)
			require.Equal(t, largePayload, []byte(largeRows.Data[0][0]))

			// 8. Compressed-flag frame inside a v5 segment must be rejected with ProtocolError
			var compFlagBuf bytes.Buffer
			require.NoError(t, rawCodec.EncodeRawFrame(&frame.RawFrame{
				Header: &frame.Header{
					Version:    primitive.ProtocolVersion5,
					Flags:      primitive.HeaderFlagCompressed,
					StreamId:   9,
					OpCode:     primitive.OpCodeQuery,
					BodyLength: 4,
				},
				Body: []byte{0, 0, 0, 0},
			}, &compFlagBuf))
			require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
				Header:  &segment.Header{IsSelfContained: true},
				Payload: &segment.Payload{UncompressedData: compFlagBuf.Bytes()},
			}, clientConn))
			resp9 := decodeSegmentFrame()
			require.Equal(t, int16(9), resp9.Header.StreamId)
			require.IsType(t, &message.ProtocolError{}, resp9.Body.Message)

			// 9. Mismatched protocol version (v4 frame inside a v5 segment) must be rejected with ProtocolError
			var v4InV5Buf bytes.Buffer
			require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 10, &message.Query{Query: "USE cycling;"}), &v4InV5Buf))
			require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
				Header:  &segment.Header{IsSelfContained: true},
				Payload: &segment.Payload{UncompressedData: v4InV5Buf.Bytes()},
			}, clientConn))
			resp10 := decodeSegmentFrame()
			require.Equal(t, int16(10), resp10.Header.StreamId)
			require.IsType(t, &message.ProtocolError{}, resp10.Body.Message)

			require.NoError(t, clientConn.Close())
			require.NoError(t, <-done)
		})
	}

	// Edge cases that terminate the connection after sending ProtocolError
	t.Run("frame_claiming_more_bytes_than_self_contained_segment", func(t *testing.T) {
		clientConn, done := startPipeConn(srv)
		rawCodec := frame.NewRawCodec()
		segCodec := segment.NewCodec()

		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 1, message.NewStartup()), clientConn))
		_, err := rawCodec.DecodeFrame(clientConn)
		require.NoError(t, err)

		// 13-byte payload inside a self-contained segment claiming BodyLength = 16 MiB (DefaultMaxFrameSize)
		var badSegPayload [13]byte
		badSegPayload[0] = byte(primitive.ProtocolVersion5)
		binary.BigEndian.PutUint16(badSegPayload[2:4], 77) // stream 77
		badSegPayload[4] = byte(primitive.OpCodeQuery)
		binary.BigEndian.PutUint32(badSegPayload[5:9], uint32(DefaultMaxFrameSize))

		var msBefore, msAfter runtime.MemStats
		runtime.ReadMemStats(&msBefore)

		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: true},
			Payload: &segment.Payload{UncompressedData: badSegPayload[:]},
		}, clientConn))

		seg, err := segCodec.DecodeSegment(clientConn)
		require.NoError(t, err)
		rf, err := rawCodec.DecodeFrame(bytes.NewReader(seg.Payload.UncompressedData))
		require.NoError(t, err)
		runtime.ReadMemStats(&msAfter)

		require.Equal(t, int16(77), rf.Header.StreamId)
		require.IsType(t, &message.ProtocolError{}, rf.Body.Message)
		require.Less(t, msAfter.TotalAlloc-msBefore.TotalAlloc, uint64(2*1024*1024), "must not allocate 16 MiB for truncated self-contained frame")

		_, err = segCodec.DecodeSegment(clientConn)
		require.ErrorIs(t, err, io.EOF)
		_ = clientConn.Close()
		<-done
	})

	t.Run("self_contained_segment_during_split_frame", func(t *testing.T) {
		clientConn, done := startPipeConn(srv)
		rawCodec := frame.NewRawCodec()
		segCodec := segment.NewCodec()

		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 1, message.NewStartup()), clientConn))
		_, err := rawCodec.DecodeFrame(clientConn)
		require.NoError(t, err)

		var multiBuf bytes.Buffer
		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 88, &message.Query{
			Query: "SELECT id FROM cycling.riders;",
		}), &multiBuf))
		fullBytes := multiBuf.Bytes()

		// Send only the first half as a non-self-contained segment
		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: false},
			Payload: &segment.Payload{UncompressedData: fullBytes[:12]},
		}, clientConn))

		// Now send a self-contained segment while the split frame is still incomplete
		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: true},
			Payload: &segment.Payload{UncompressedData: fullBytes},
		}, clientConn))

		seg, err := segCodec.DecodeSegment(clientConn)
		require.NoError(t, err)
		rf, err := rawCodec.DecodeFrame(bytes.NewReader(seg.Payload.UncompressedData))
		require.NoError(t, err)
		require.Equal(t, int16(88), rf.Header.StreamId)
		require.IsType(t, &message.ProtocolError{}, rf.Body.Message)

		_, err = segCodec.DecodeSegment(clientConn)
		require.ErrorIs(t, err, io.EOF)
		_ = clientConn.Close()
		<-done
	})

	t.Run("split_frame_over_max_frame_size", func(t *testing.T) {
		clientConn, done := startPipeConn(srv)
		rawCodec := frame.NewRawCodec()
		segCodec := segment.NewCodec()

		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 1, message.NewStartup()), clientConn))
		_, err := rawCodec.DecodeFrame(clientConn)
		require.NoError(t, err)

		// First segment of a split frame claiming BodyLength = DefaultMaxFrameSize + 100
		var oversizedHdr [16]byte
		oversizedHdr[0] = byte(primitive.ProtocolVersion5)
		binary.BigEndian.PutUint16(oversizedHdr[2:4], 99) // stream 99
		oversizedHdr[4] = byte(primitive.OpCodeQuery)
		binary.BigEndian.PutUint32(oversizedHdr[5:9], uint32(DefaultMaxFrameSize+100))

		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: false},
			Payload: &segment.Payload{UncompressedData: oversizedHdr[:]},
		}, clientConn))

		seg, err := segCodec.DecodeSegment(clientConn)
		require.NoError(t, err)
		rf, err := rawCodec.DecodeFrame(bytes.NewReader(seg.Payload.UncompressedData))
		require.NoError(t, err)
		require.Equal(t, int16(99), rf.Header.StreamId)
		require.IsType(t, &message.ProtocolError{}, rf.Body.Message)

		_, err = segCodec.DecodeSegment(clientConn)
		require.ErrorIs(t, err, io.EOF)
		_ = clientConn.Close()
		<-done
	})

	t.Run("split_frame_throw_on_overload_keeps_connection_open", func(t *testing.T) {
		srv.SetMaxInFlightBytes(64)
		defer srv.SetMaxInFlightBytes(DefaultMaxInFlightBytes)

		clientConn, done := startPipeConn(srv)
		rawCodec := frame.NewRawCodec()
		segCodec := segment.NewCodec()

		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 1, message.NewStartup(
			message.StartupOptionThrowOnOverload, "1",
		)), clientConn))
		_, err := rawCodec.DecodeFrame(clientConn)
		require.NoError(t, err)

		// Send a ~550-byte query split across 3 segments on stream 55 (exceeds 64-byte budget)
		var bigBuf bytes.Buffer
		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 55, &message.Query{
			Query: "SELECT id FROM cycling.riders WHERE name = '" + strings.Repeat("O", 512) + "' ALLOW FILTERING;",
		}), &bigBuf))
		fullBytes := bigBuf.Bytes()

		// First segment triggers Overloaded reply immediately
		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: false},
			Payload: &segment.Payload{UncompressedData: fullBytes[:20]},
		}, clientConn))

		seg, err := segCodec.DecodeSegment(clientConn)
		require.NoError(t, err)
		rf, err := rawCodec.DecodeFrame(bytes.NewReader(seg.Payload.UncompressedData))
		require.NoError(t, err)
		require.Equal(t, int16(55), rf.Header.StreamId)
		require.IsType(t, &message.Overloaded{}, rf.Body.Message)

		// Remaining 2 segments of the overloaded split frame are discarded cleanly
		for _, part := range [][]byte{fullBytes[20:200], fullBytes[200:]} {
			require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
				Header:  &segment.Header{IsSelfContained: false},
				Payload: &segment.Payload{UncompressedData: part},
			}, clientConn))
		}

		// Subsequent small query on stream 56 on the SAME connection must succeed!
		var smallBuf bytes.Buffer
		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 56, &message.Query{
			Query: "USE cycling;",
		}), &smallBuf))
		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: true},
			Payload: &segment.Payload{UncompressedData: smallBuf.Bytes()},
		}, clientConn))

		seg56, err := segCodec.DecodeSegment(clientConn)
		require.NoError(t, err)
		rf56, err := rawCodec.DecodeFrame(bytes.NewReader(seg56.Payload.UncompressedData))
		require.NoError(t, err)
		require.Equal(t, int16(56), rf56.Header.StreamId)
		require.IsType(t, &message.SetKeyspaceResult{}, rf56.Body.Message)

		require.NoError(t, clientConn.Close())
		require.NoError(t, <-done)
	})

	t.Run("first_split_segment_longer_than_declared_frame_replies_with_stream_id", func(t *testing.T) {
		clientConn, done := startPipeConn(srv)
		rawCodec := frame.NewRawCodec()
		segCodec := segment.NewCodec()

		require.NoError(t, rawCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion5, 1, message.NewStartup()), clientConn))
		_, err := rawCodec.DecodeFrame(clientConn)
		require.NoError(t, err)

		// First split segment is 20 bytes long, but its 9-byte header declares BodyLength = 4 (total target 13 bytes)
		var overlongSeg [20]byte
		overlongSeg[0] = byte(primitive.ProtocolVersion5)
		binary.BigEndian.PutUint16(overlongSeg[2:4], 66) // stream 66
		overlongSeg[4] = byte(primitive.OpCodeQuery)
		binary.BigEndian.PutUint32(overlongSeg[5:9], 4)

		require.NoError(t, segCodec.EncodeSegment(&segment.Segment{
			Header:  &segment.Header{IsSelfContained: false},
			Payload: &segment.Payload{UncompressedData: overlongSeg[:]},
		}, clientConn))

		seg, err := segCodec.DecodeSegment(clientConn)
		require.NoError(t, err)
		rf, err := rawCodec.DecodeFrame(bytes.NewReader(seg.Payload.UncompressedData))
		require.NoError(t, err)
		require.Equal(t, int16(66), rf.Header.StreamId)
		require.IsType(t, &message.ProtocolError{}, rf.Body.Message)

		_, err = segCodec.DecodeSegment(clientConn)
		require.ErrorIs(t, err, io.EOF)
		_ = clientConn.Close()
		<-done
	})
}
