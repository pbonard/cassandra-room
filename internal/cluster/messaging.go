// Package cluster connects nodes: internode messaging (MessagingService),
// SWIM membership with failure detection (after hashicorp/memberlist), the
// Cluster Metadata Service that commits metadata epochs (CEP-21,
// Transactional Cluster Metadata) and the token ring with its replication
// strategies (SimpleStrategy, NetworkTopologyStrategy).
package cluster

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pierrec/lz4/v4"
)

// Verb identifies a message's handler (Verb).
type Verb uint8

const (
	VerbGossip Verb = iota + 1
	VerbMutation
	VerbReadRow
	VerbReadRange
	VerbExecute
	VerbRepair
	VerbPing
	VerbPingReq
	VerbBroadcast
	VerbPaxosPrepare
	VerbPaxosAccept
	VerbMetadataFetch
	VerbMetadataPush
	VerbLWTPrepare
	VerbLWTPropose
	VerbLWTCommit
	VerbStreamPush
	VerbReadDigest
	VerbCounterMutation
)

// Handler handles a request and returns the response payload.
type Handler func(from string, payload []byte) ([]byte, error)

// Messaging sends requests between nodes over TCP. Each frame is
//
//	length (4) | kind (1) | verb (1) | id (8) | payload
//
// kind is request, response or error response. Requests to a node share
// one outbound connection, which carries their responses back.
type Messaging struct {
	addr     string
	ln       net.Listener
	handlers map[Verb]Handler

	mu      sync.Mutex
	conns   map[string]*outConn
	inbound map[net.Conn]struct{}
	closed  bool

	nextID atomic.Uint64
	wg     sync.WaitGroup
	// dialTimeout bounds connection attempts.
	dialTimeout time.Duration
	opts        MessagingOptions
}

// MessagingOptions configure internode connections
// (server_encryption_options, internode_compression).
type MessagingOptions struct {
	// TLS, if set, encrypts every connection; the same configuration
	// serves and dials, so nodes authenticate each other.
	TLS *tls.Config
	// Compress compresses frame payloads above a few hundred bytes with
	// LZ4, as Cassandra's internode_compression: all.
	Compress bool
}

const (
	kindRequest  = 1
	kindResponse = 2
	kindError    = 3
	maxFrame     = 256 << 20
)

// ErrTimeout is returned when a request gets no response in time.
var ErrTimeout = errors.New("cluster: request timed out")

// NewMessaging listens on addr ("host:port").
func NewMessaging(addr string) (*Messaging, error) {
	return NewMessagingWith(addr, MessagingOptions{})
}

// NewMessagingWith listens on addr with options.
func NewMessagingWith(addr string, o MessagingOptions) (*Messaging, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	m := &Messaging{addr: ln.Addr().String(), ln: ln, handlers: map[Verb]Handler{},
		conns: map[string]*outConn{}, inbound: map[net.Conn]struct{}{}, dialTimeout: 2 * time.Second, opts: o}
	if o.TLS != nil {
		m.ln = tls.NewListener(ln, o.TLS)
	}
	return m, nil
}

const (
	compressedFlag = 0x80
	compressMin    = 512
)

// pack compresses a payload if worth it, returning the kind flag.
func (m *Messaging) pack(payload []byte) (byte, []byte) {
	if !m.opts.Compress || len(payload) < compressMin {
		return 0, payload
	}
	buf := make([]byte, binary.MaxVarintLen64+lz4.CompressBlockBound(len(payload)))
	n := binary.PutUvarint(buf, uint64(len(payload)))
	var c lz4.Compressor
	k, err := c.CompressBlock(payload, buf[n:])
	if err != nil || k == 0 || n+k >= len(payload) {
		return 0, payload
	}
	return compressedFlag, buf[:n+k]
}

func unpack(kind byte, payload []byte) (byte, []byte, error) {
	if kind&compressedFlag == 0 {
		return kind, payload, nil
	}
	size, n := binary.Uvarint(payload)
	if n <= 0 || size > maxFrame {
		return 0, nil, errors.New("cluster: corrupt compressed frame")
	}
	out := make([]byte, size)
	k, err := lz4.UncompressBlock(payload[n:], out)
	if err != nil || uint64(k) != size {
		return 0, nil, errors.New("cluster: corrupt compressed frame")
	}
	return kind &^ compressedFlag, out, nil
}

// Addr returns the address the node listens on.
func (m *Messaging) Addr() string { return m.addr }

// Handle registers the handler of a verb. Call before Start.
func (m *Messaging) Handle(v Verb, h Handler) { m.handlers[v] = h }

// Start accepts connections.
func (m *Messaging) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			c, err := m.ln.Accept()
			if err != nil {
				return
			}
			m.mu.Lock()
			if m.closed {
				m.mu.Unlock()
				c.Close()
				return
			}
			m.inbound[c] = struct{}{}
			m.mu.Unlock()
			m.wg.Add(1)
			go m.serve(c)
		}
	}()
}

// Close stops listening and closes every connection.
func (m *Messaging) Close() error {
	m.mu.Lock()
	m.closed = true
	err := m.ln.Close()
	for c := range m.inbound {
		c.Close()
	}
	for _, oc := range m.conns {
		oc.close(errors.New("cluster: messaging closed"))
	}
	m.mu.Unlock()
	m.wg.Wait()
	return err
}

func writeFrame(w io.Writer, kind byte, verb Verb, id uint64, payload []byte) error {
	hdr := make([]byte, 14, 14+len(payload))
	binary.BigEndian.PutUint32(hdr, uint32(10+len(payload)))
	hdr[4], hdr[5] = kind, byte(verb)
	binary.BigEndian.PutUint64(hdr[6:], id)
	_, err := w.Write(append(hdr, payload...))
	return err
}

func readFrame(r io.Reader) (kind byte, verb Verb, id uint64, payload []byte, err error) {
	var hdr [14]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n < 10 || n > maxFrame {
		err = fmt.Errorf("cluster: bad frame length %d", n)
		return
	}
	kind, verb, id = hdr[4], Verb(hdr[5]), binary.BigEndian.Uint64(hdr[6:])
	payload = make([]byte, n-10)
	if _, err = io.ReadFull(r, payload); err != nil {
		return
	}
	kind, payload, err = unpack(kind, payload)
	return
}

// serve handles an inbound connection's requests, each in its goroutine.
func (m *Messaging) serve(c net.Conn) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.inbound, c)
		m.mu.Unlock()
		c.Close()
	}()
	r := bufio.NewReaderSize(c, 64<<10)
	var wmu sync.Mutex
	from := ""
	for {
		kind, verb, id, payload, err := readFrame(r)
		if err != nil {
			return
		}
		if kind != kindRequest {
			continue
		}
		if verb == 0 { // hello: the peer's address
			from = string(payload)
			continue
		}
		go func() {
			h := m.handlers[verb]
			var resp []byte
			var herr error
			if h == nil {
				herr = fmt.Errorf("cluster: no handler for verb %d", verb)
			} else {
				resp, herr = h(from, payload)
			}
			k := byte(kindResponse)
			if herr != nil {
				k, resp = kindError, []byte(herr.Error())
			}
			flag, resp := m.pack(resp)
			wmu.Lock()
			_ = writeFrame(c, k|flag, verb, id, resp)
			wmu.Unlock()
		}()
	}
}

// outConn is the connection requests to one node go through.
type outConn struct {
	c       net.Conn
	wmu     sync.Mutex
	mu      sync.Mutex
	pending map[uint64]chan result
	err     error
}

type result struct {
	payload []byte
	err     error
}

func (oc *outConn) close(err error) {
	oc.mu.Lock()
	if oc.err == nil {
		oc.err = err
	}
	for id, ch := range oc.pending {
		ch <- result{err: err}
		delete(oc.pending, id)
	}
	oc.mu.Unlock()
	oc.c.Close()
}

func (m *Messaging) conn(to string) (*outConn, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("cluster: messaging closed")
	}
	if oc := m.conns[to]; oc != nil {
		oc.mu.Lock()
		dead := oc.err != nil
		oc.mu.Unlock()
		if !dead {
			m.mu.Unlock()
			return oc, nil
		}
		delete(m.conns, to)
	}
	m.mu.Unlock()
	c, err := net.DialTimeout("tcp", to, m.dialTimeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if m.opts.TLS != nil {
		cfg := m.opts.TLS.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName, _, _ = net.SplitHostPort(to)
		}
		tc := tls.Client(c, cfg)
		_ = tc.SetDeadline(time.Now().Add(m.dialTimeout))
		if err := tc.Handshake(); err != nil {
			c.Close()
			return nil, err
		}
		_ = tc.SetDeadline(time.Time{})
		c = tc
	}
	oc := &outConn{c: c, pending: map[uint64]chan result{}}
	if err := writeFrame(c, kindRequest, 0, 0, []byte(m.addr)); err != nil {
		c.Close()
		return nil, err
	}
	m.mu.Lock()
	if old := m.conns[to]; old != nil && old.err == nil {
		m.mu.Unlock()
		c.Close()
		return old, nil
	}
	m.conns[to] = oc
	m.mu.Unlock()
	go func() {
		r := bufio.NewReaderSize(c, 64<<10)
		for {
			kind, _, id, payload, err := readFrame(r)
			if err != nil {
				oc.close(fmt.Errorf("cluster: connection to %s lost: %w", to, err))
				return
			}
			oc.mu.Lock()
			ch := oc.pending[id]
			delete(oc.pending, id)
			oc.mu.Unlock()
			if ch == nil {
				continue
			}
			if kind == kindError {
				ch <- result{err: errors.New(string(payload))}
			} else {
				ch <- result{payload: payload}
			}
		}
	}()
	return oc, nil
}

// Request sends a request to node to and waits for its response.
func (m *Messaging) Request(ctx context.Context, to string, verb Verb, payload []byte) ([]byte, error) {
	if to == m.addr {
		h := m.handlers[verb]
		if h == nil {
			return nil, fmt.Errorf("cluster: no handler for verb %d", verb)
		}
		return h(m.addr, payload)
	}
	oc, err := m.conn(to)
	if err != nil {
		return nil, err
	}
	id := m.nextID.Add(1)
	ch := make(chan result, 1)
	oc.mu.Lock()
	if oc.err != nil {
		oc.mu.Unlock()
		return nil, oc.err
	}
	oc.pending[id] = ch
	oc.mu.Unlock()
	flag, packed := m.pack(payload)
	oc.wmu.Lock()
	err = writeFrame(oc.c, kindRequest|flag, verb, id, packed)
	oc.wmu.Unlock()
	if err != nil {
		oc.close(err)
		return nil, err
	}
	select {
	case r := <-ch:
		return r.payload, r.err
	case <-ctx.Done():
		oc.mu.Lock()
		delete(oc.pending, id)
		oc.mu.Unlock()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrTimeout
		}
		return nil, ctx.Err()
	}
}
