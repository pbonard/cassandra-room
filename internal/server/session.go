package server

import (
	"sync"

	"github.com/datastax/go-cassandra-native-protocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
)

var (
	plainSegmentCodec = segment.NewCodec()
	lz4SegmentCodec   = segment.NewCodecWithCompression(lz4.Compressor{})
)

// Session tracks per-connection state (such as the active keyspace set by USE,
// the frame/segment compression algorithm negotiated during STARTUP, and whether
// the connection has transitioned to Protocol v5 segment framing).
type Session struct {
	mu                  sync.RWMutex
	initialized         bool
	version             primitive.ProtocolVersion
	pendingModernLayout bool
	modernLayout        bool
	keyspace            string
	user                string
	throwOnOverload     bool
	compressionAlgo     string
	compressor          compression.Compressor

	writeMu  sync.Mutex
	writeErr error
}

// isInitialized returns true if STARTUP has already completed on this session.
func (s *Session) isInitialized() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialized
}

// setInitializedWithVersion marks the session as having completed STARTUP negotiation
// with the given protocol version.
func (s *Session) setInitializedWithVersion(v primitive.ProtocolVersion) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initialized = true
	s.version = v
}

// protocolVersion returns the protocol version negotiated during STARTUP (or 0 if not yet initialized).
func (s *Session) protocolVersion() primitive.ProtocolVersion {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// setPendingModernLayout arms the transition to Protocol v5+ segment framing once
// the STARTUP READY response frame has been written.
func (s *Session) setPendingModernLayout(v bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingModernLayout = v
}

// consumePendingModernLayout transitions the session to Protocol v5+ segment framing
// if a v5 STARTUP just completed.
func (s *Session) consumePendingModernLayout() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingModernLayout {
		s.pendingModernLayout = false
		s.modernLayout = true
		return true
	}
	return false
}

// isModernLayout returns true if this connection has transitioned to Protocol v5+ segment framing.
func (s *Session) isModernLayout() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.modernLayout
}

// segmentCodec returns the v5 segment codec configured for this session.
func (s *Session) segmentCodec() segment.Codec {
	if s != nil {
		s.mu.RLock()
		algo := s.compressionAlgo
		s.mu.RUnlock()
		if algo == "lz4" {
			return lz4SegmentCodec
		}
	}
	return plainSegmentCodec
}

// Keyspace returns the currently active keyspace for this connection.
func (s *Session) Keyspace() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.keyspace
}

// setKeyspace updates the active keyspace for this connection.
func (s *Session) setKeyspace(ks string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keyspace = ks
}

// getThrowOnOverload returns true if the client requested THROW_ON_OVERLOAD=1 in STARTUP.
func (s *Session) getThrowOnOverload() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.throwOnOverload
}

// setThrowOnOverload sets whether the connection should immediately receive
// ERROR OVERLOADED instead of applying backpressure when admission control is full.
func (s *Session) setThrowOnOverload(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.throwOnOverload = v
}

// compression returns the negotiated compression algorithm ("lz4", "snappy", or "")
// and its Compressor implementation for this session.
func (s *Session) compression() (string, compression.Compressor) {
	if s == nil {
		return "", nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compressionAlgo, s.compressor
}

// setCompression configures the frame/segment compression algorithm for this session.
func (s *Session) setCompression(algo string, comp compression.Compressor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compressionAlgo = algo
	s.compressor = comp
}

// User returns the role the session logged in as, or "".
func (s *Session) User() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.user
}

func (s *Session) setUser(u string) {
	s.mu.Lock()
	s.user = u
	s.mu.Unlock()
}
