package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/db"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Server is a concurrent CQL native protocol server that accepts multiple TCP connections,
// multiplexes up to 32k stream IDs per connection, enforces global admission control
// (both request count and in-flight byte budgets), caches prepared statements
// in a sharded bounded LRU, decodes wire frames using
// datastax/go-cassandra-native-protocol, and executes statements on the
// database.
type Server struct {
	codec             frame.RawCodec
	db                *db.DB
	maxStreamsPerConn int
	admission         *admissionController

	// statements caches prepared statements by id, for EXECUTE and for
	// unprepared QUERY messages alike.
	statements *lruCache[*db.Prepared]
	// testHook, if set, runs before each statement executes (tests).
	testHook func(ctx context.Context, query string) error

	mu           sync.RWMutex
	maxFrameSize int
	writeTimeout time.Duration
	listeners    map[net.Listener]struct{}
	conns        map[net.Conn]struct{}
	closed       bool
	shuttingDown bool
	serverCtx    context.Context
	serverCancel context.CancelFunc
	connWG       sync.WaitGroup
}

// New creates a CQL protocol server executing statements on d.
func New(d *db.DB) *Server {
	maxGlobal := runtime.NumCPU() * 64
	if maxGlobal < 256 {
		maxGlobal = 256
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		codec:             frame.NewRawCodec(),
		db:                d,
		maxStreamsPerConn: DefaultMaxConcurrentStreamsPerConn,
		admission:         newAdmissionController(maxGlobal, DefaultMaxInFlightBytes),
		statements:        newLRUCache[*db.Prepared](DefaultPreparedCacheCapacity),
		maxFrameSize:      DefaultMaxFrameSize,
		writeTimeout:      DefaultWriteTimeout,
		listeners:         make(map[net.Listener]struct{}),
		conns:             make(map[net.Conn]struct{}),
		serverCtx:         ctx,
		serverCancel:      cancel,
	}
}

// ListenAndServe listens on the TCP network address addr (e.g. ":9042") and
// serves incoming CQL client connections concurrently.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts incoming connections on the Listener ln, spawning a new goroutine
// for each connection. Transient accept errors (such as EMFILE) are retried with
// exponential backoff from 5ms up to 1s.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return net.ErrClosed
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.listeners, ln)
		s.mu.Unlock()
		_ = ln.Close()
	}()

	var tempDelay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.RLock()
			closed := s.closed
			s.mu.RUnlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			if isTemporaryAcceptError(err) {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if maxDelay := 1 * time.Second; tempDelay > maxDelay {
					tempDelay = maxDelay
				}
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-s.serverCtx.Done():
					timer.Stop()
					return nil
				}
				continue
			}
			return err
		}
		tempDelay = 0

		go func(c net.Conn) {
			_ = s.serveConn(c)
		}(conn)
	}
}

// Shutdown gracefully stops the server: it closes listeners and sets an immediate
// read deadline on active connections so reader loops stop reading new frames while
// keeping sockets open for in-flight responses to be written. If ctx expires before
// in-flight requests finish, remaining request contexts are canceled and connections
// are closed.
func (s *Server) Shutdown(ctx context.Context) error {
	now := time.Now()
	s.mu.Lock()
	s.closed = true
	s.shuttingDown = true
	for ln := range s.listeners {
		_ = ln.Close()
	}
	for c := range s.conns {
		_ = c.SetReadDeadline(now)
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.serverCancel()
		return nil
	case <-ctx.Done():
		s.serverCancel()
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}
}

// serveConn reads CQL native protocol frames from conn and multiplexes in-flight
// stream requests concurrently (just like Cassandra's native transport), writing
// response frames back thread-safely as each request completes.
func (s *Server) serveConn(conn net.Conn) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return net.ErrClosed
	}
	s.conns[conn] = struct{}{}
	s.connWG.Add(1)
	s.mu.Unlock()

	connCtx, connCancel := context.WithCancel(s.serverCtx)
	defer func() {
		connCancel()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
		s.connWG.Done()
	}()

	session := &Session{}
	reader := bufioReaderPool.Get().(*bufio.Reader)
	reader.Reset(conn)
	defer func() {
		reader.Reset(nil)
		bufioReaderPool.Put(reader)
	}()

	var (
		inFlight           sync.WaitGroup
		sem                = make(chan struct{}, s.maxStreamsPerConn)
		multiSegBuf        bytes.Buffer
		multiTargetLen     int
		multiReceivedLen   int
		multiStreamID      int16
		multiDiscarding    bool
		multiReservedBytes int64
	)

	defer func() {
		if multiReservedBytes > 0 {
			s.admission.releaseBytes(multiReservedBytes)
		}
		if !s.isShuttingDown() {
			connCancel()
		}
		inFlight.Wait()
	}()

	for {
		if !session.isModernLayout() {
			// Peek up to 4 bytes before DecodeHeader so if the codec rejects an unknown
			// protocol version, we still reply on the client's actual StreamId.
			hdrPeek, _ := reader.Peek(4)
			peekStreamID := extractPeekedStreamID(hdrPeek)
			if err := s.readAndDispatchFrame(conn, connCtx, session, reader, peekStreamID, -1, 0, &inFlight, sem); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
					(s.isShuttingDown() && isTimeoutError(err)) {
					session.writeMu.Lock()
					wErr := session.writeErr
					session.writeMu.Unlock()
					return wErr
				}
				return err
			}
			continue
		}

		// Protocol v5+ segment framing layout (active after STARTUP -> READY).
		seg, err := session.segmentCodec().DecodeSegment(reader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
				(s.isShuttingDown() && isTimeoutError(err)) {
				session.writeMu.Lock()
				wErr := session.writeErr
				session.writeMu.Unlock()
				return wErr
			}
			return err
		}

		if seg.Header.IsSelfContained {
			if multiTargetLen > 0 {
				peekStreamID := extractPeekedStreamID(seg.Payload.UncompressedData)
				resp := frame.NewFrame(
					normalizedErrorVersion(session.protocolVersion()),
					peekStreamID,
					&message.ProtocolError{
						ErrorMessage: fmt.Sprintf("received self-contained v5 segment while multi-segment frame (%d/%d bytes) is incomplete", multiReceivedLen, multiTargetLen),
					},
				)
				_ = s.writeFrame(conn, session, resp)
				return fmt.Errorf("received self-contained v5 segment while multi-segment frame (%d/%d bytes) is incomplete", multiReceivedLen, multiTargetLen)
			}
			payload := seg.Payload.UncompressedData
			payloadReader := bytes.NewReader(payload)
			for payloadReader.Len() > 0 {
				remBefore := payloadReader.Len()
				offset := len(payload) - remBefore
				peekStreamID := extractPeekedStreamID(payload[offset:])
				maxAvailBody := remBefore - int(primitive.FrameHeaderLengthV3AndHigher)
				if maxAvailBody < 0 {
					maxAvailBody = 0
				}
				if err := s.readAndDispatchFrame(conn, connCtx, session, payloadReader, peekStreamID, maxAvailBody, 0, &inFlight, sem); err != nil {
					return err
				}
			}
			continue
		}

		// Multi-segment frame part: validate the 9-byte frame header, protocol version,
		// maxFrameSize, and in-flight byte budget on the very first segment before
		// accumulating subsequent segments in memory.
		chunk := seg.Payload.UncompressedData
		if multiTargetLen == 0 {
			peekStreamID := extractPeekedStreamID(chunk)
			hdr, err := s.codec.DecodeHeader(bytes.NewReader(chunk))
			if err != nil {
				resp := frame.NewFrame(
					normalizedErrorVersion(session.protocolVersion()),
					peekStreamID,
					&message.ProtocolError{
						ErrorMessage: fmt.Sprintf("failed to decode multi-segment frame header: %v", err),
					},
				)
				_ = s.writeFrame(conn, session, resp)
				return fmt.Errorf("failed to decode multi-segment frame header: %w", err)
			}
			if errResp, _ := s.validateFrameHeader(session, hdr); errResp != nil {
				_ = s.writeFrame(conn, session, errResp)
				return fmt.Errorf("invalid multi-segment frame header: %v", errResp.Body.Message)
			}
			multiTargetLen = int(primitive.FrameHeaderLengthV3AndHigher) + int(hdr.BodyLength)
			multiStreamID = hdr.StreamId
			multiReceivedLen = 0
			multiDiscarding = false

			isControl := hdr.OpCode == primitive.OpCodeOptions ||
				hdr.OpCode == primitive.OpCodeStartup ||
				hdr.OpCode == primitive.OpCodeRegister
			if !isControl && hdr.BodyLength > 0 {
				overloaded, err := s.admission.acquireBytes(connCtx, int64(hdr.BodyLength), session.getThrowOnOverload())
				if err != nil {
					return err
				}
				if overloaded {
					multiDiscarding = true
					resp := frame.NewFrame(
						hdr.Version,
						hdr.StreamId,
						&message.Overloaded{ErrorMessage: "server is overloaded (in-flight byte limit reached)"},
					)
					if err := s.writeFrame(conn, session, resp); err != nil {
						return err
					}
				} else {
					multiReservedBytes = int64(hdr.BodyLength)
				}
			}
		}

		if multiReceivedLen+len(chunk) > multiTargetLen {
			resp := frame.NewFrame(
				normalizedErrorVersion(session.protocolVersion()),
				multiStreamID,
				&message.ProtocolError{
					ErrorMessage: fmt.Sprintf("multi-segment payload length %d exceeds target frame length %d", multiReceivedLen+len(chunk), multiTargetLen),
				},
			)
			_ = s.writeFrame(conn, session, resp)
			return fmt.Errorf("multi-segment payload length %d exceeds target frame length %d", multiReceivedLen+len(chunk), multiTargetLen)
		}
		multiReceivedLen += len(chunk)
		if !multiDiscarding {
			multiSegBuf.Write(chunk)
		}
		if multiReceivedLen == multiTargetLen {
			wasDiscarding := multiDiscarding
			assembled := multiSegBuf.Bytes()
			reserved := multiReservedBytes
			multiSegBuf = bytes.Buffer{}
			multiTargetLen = 0
			multiReceivedLen = 0
			multiStreamID = 0
			multiDiscarding = false
			multiReservedBytes = 0
			if wasDiscarding {
				continue
			}
			maxAvailBody := len(assembled) - int(primitive.FrameHeaderLengthV3AndHigher)
			if err := s.readAndDispatchFrame(conn, connCtx, session, bytes.NewReader(assembled), extractPeekedStreamID(assembled), maxAvailBody, reserved, &inFlight, sem); err != nil {
				return err
			}
		}
	}
}

// SetMaxConcurrentRequests configures the global request concurrency limit across
// all connections on this server. Safe to call before or during Serve.
func (s *Server) SetMaxConcurrentRequests(limit int) {
	s.admission.setMaxRequests(limit)
}

// SetMaxInFlightBytes configures the global in-flight request payload byte budget
// across all connections on this server.
func (s *Server) SetMaxInFlightBytes(bytes int64) {
	s.admission.setMaxBytes(bytes)
}

// SetMaxFrameSize configures the maximum allowed frame body size in bytes.
func (s *Server) SetMaxFrameSize(bytes int) {
	if bytes <= 0 {
		bytes = DefaultMaxFrameSize
	}
	s.mu.Lock()
	s.maxFrameSize = bytes
	s.mu.Unlock()
}

// SetWriteTimeout configures the per-frame socket write deadline. A zero or negative
// duration disables write deadlines.
func (s *Server) SetWriteTimeout(d time.Duration) {
	s.mu.Lock()
	s.writeTimeout = d
	s.mu.Unlock()
}

func (s *Server) getMaxFrameSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxFrameSize
}

func (s *Server) getWriteTimeout() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writeTimeout
}

func (s *Server) isShuttingDown() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shuttingDown
}
