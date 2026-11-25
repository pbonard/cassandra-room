package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"time"
)

// Client encryption (client_encryption_options): with a TLS
// configuration, connections to the native port are encrypted. Optional
// mode (optional: true) also accepts unencrypted ones, telling them apart
// by their first byte: a TLS handshake starts with a record of type 22.

const tlsHandshakeTimeout = 10 * time.Second

// SetTLS encrypts client connections with cfg (nil: none); optional also
// accepts unencrypted connections on the same port.
func (s *Server) SetTLS(cfg *tls.Config, optional bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tlsConfig, s.tlsOptional = cfg, optional
}

// secure wraps an accepted connection for client encryption, completing
// the handshake; nil if the connection is to be dropped.
func (s *Server) secure(c net.Conn) net.Conn {
	s.mu.RLock()
	cfg, optional := s.tlsConfig, s.tlsOptional
	s.mu.RUnlock()
	if cfg == nil {
		return c
	}
	_ = c.SetReadDeadline(time.Now().Add(tlsHandshakeTimeout))
	if optional {
		r := bufio.NewReader(c)
		first, err := r.Peek(1)
		if err != nil {
			_ = c.Close()
			return nil
		}
		c = &peekedConn{Conn: c, r: r}
		if first[0] != 0x16 {
			_ = c.SetReadDeadline(time.Time{})
			return c // unencrypted
		}
	}
	tc := tls.Server(c, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), tlsHandshakeTimeout)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = c.Close()
		return nil
	}
	_ = c.SetReadDeadline(time.Time{})
	return tc
}

// peekedConn is a connection whose first bytes were read into r.
type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(b []byte) (int, error) { return c.r.Read(b) }
