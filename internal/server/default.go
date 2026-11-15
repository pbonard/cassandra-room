package server

import "time"

const (
	// DefaultMaxConcurrentStreamsPerConn is the default limit of in-flight concurrent
	// requests per single CQL TCP connection (CQL v3+ supports up to 32768 stream IDs).
	DefaultMaxConcurrentStreamsPerConn = 1024

	// DefaultMaxFrameSize is the maximum allowed CQL frame body size in bytes (16 MiB,
	// matching Cassandra's native_transport_max_frame_size_in_mb default).
	DefaultMaxFrameSize = 16 * 1024 * 1024

	// DefaultMaxInFlightBytes is the global byte budget for in-flight request payloads
	// across all connections (64 MiB, matching Cassandra's native_transport_max_request_data_in_flight).
	DefaultMaxInFlightBytes int64 = 64 * 1024 * 1024

	// DefaultWriteTimeout is the per-frame write deadline applied to client connections.
	DefaultWriteTimeout = 10 * time.Second

	// DefaultPreparedCacheCapacity is the maximum number of prepared statements cached in LRU.
	DefaultPreparedCacheCapacity = 4096
)
