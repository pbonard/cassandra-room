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
	"hash/crc32"
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
	VerbStreamFile
	numVerbs
)

// Handler handles a request and returns the response payload. ctx
// carries the request's deadline: work the requester has given up on can
// stop.
type Handler func(ctx context.Context, from string, payload []byte) ([]byte, error)

// Messaging sends requests between nodes over TCP, as Cassandra's
// MessagingService does (4.0's internode protocol, simplified):
//
//   - Each peer gets up to three outbound connections, by message class:
//     urgent (membership, metadata), small (reads, writes), large
//     (streaming, range reads, and any message over 64 KiB), so a large
//     message never delays a heartbeat or a write.
//   - A connection starts with a handshake that agrees on the protocol
//     version; frames carry a CRC32C of their header and payload, the
//     sender's deadline, and are optionally LZ4-compressed.
//   - Outbound requests reserve their size against limits per
//     connection, per endpoint and globally; past them a request fails as
//     overloaded instead of queueing without bound. Inbound, a connection
//     whose requests exceed the limits stops being read until they finish,
//     which pushes back on the sender through TCP.
//   - Requests already past their deadline when they arrive, or when a
//     worker frees up, are dropped; each kind of work (reads, writes,
//     gossip, metadata, streaming) has a bounded number of workers.
//
// Responses travel back on the connection that carried the request.
type Messaging struct {
	addr     string
	ln       net.Listener
	handlers [numVerbs]Handler
	opts     MessagingOptions

	mu      sync.Mutex
	conns   map[outKey]*outConn
	inbound map[net.Conn]struct{}
	closed  bool
	peerVer map[string]uint16 // negotiated protocol version, by peer

	nextID atomic.Uint64
	wg     sync.WaitGroup

	stages [numStages]chan struct{}

	// Limits, in bytes (internode_application_*_queue_*).
	outEndpoint map[string]*budget
	outGlobal   budget
	inEndpoint  map[string]*budget
	inGlobal    budget

	dropped    [numVerbs]atomic.Uint64
	overloaded atomic.Uint64
}

// MessagingOptions configure internode connections
// (server_encryption_options, internode_compression, queue capacities).
type MessagingOptions struct {
	// TLS, if set, encrypts every connection; the same configuration
	// serves and dials, so nodes authenticate each other.
	TLS *tls.Config
	// Compress compresses frame payloads over 512 bytes with LZ4, as
	// Cassandra's internode_compression: all.
	Compress bool
	// Queue capacities: per connection, then a reserve shared per
	// endpoint, then one shared by all (Cassandra: 4 MiB, 128 MiB,
	// 512 MiB, both for sending and receiving). Zero uses the defaults.
	QueueCapacity, EndpointReserve, GlobalReserve int64
}

const (
	// ProtocolVersion is this node's messaging version; MinProtocolVersion
	// the oldest it can talk to.
	ProtocolVersion    = 2
	MinProtocolVersion = 2

	kindRequest    = 1
	kindResponse   = 2
	kindError      = 3
	compressedFlag = 0x80
	compressMin    = 512
	maxFrame       = 256 << 20
	// largeMessage is the size above which a message goes on the large
	// connection (OTCP_LARGE_MESSAGE_THRESHOLD).
	largeMessage = 64 << 10
	// headerLen: length (4) | kind (1) | verb (1) | id (8) | deadline (8) | crc (4)
	headerLen = 26
)

var (
	// ErrTimeout is returned when a request gets no response in time.
	ErrTimeout = errors.New("cluster: request timed out")
	// ErrOverloaded is returned when a request would exceed the queue
	// limits towards its destination.
	ErrOverloaded  = errors.New("cluster: too many bytes queued for the destination")
	errCorrupt     = errors.New("cluster: corrupt frame (checksum mismatch)")
	castagnoli     = crc32.MakeTable(crc32.Castagnoli)
	handshakeMagic = [4]byte{'C', 'R', 'M', 'S'}
)

// Message classes (ConnectionType).
const (
	classUrgent = iota
	classSmall
	classLarge
	numClasses
)

func classOf(v Verb, size int) int {
	switch {
	case size > largeMessage:
		return classLarge
	case v == VerbStreamPush || v == VerbStreamFile || v == VerbReadRange || v == VerbRepair:
		return classLarge
	case v == VerbGossip || v == VerbPing || v == VerbPingReq || v == VerbBroadcast ||
		v == VerbPaxosPrepare || v == VerbPaxosAccept || v == VerbMetadataFetch || v == VerbMetadataPush:
		return classUrgent
	}
	return classSmall
}

// Stages: bounded workers per kind of work (Stage).
const (
	stageRead = iota
	stageMutation
	stageGossip
	stageMetadata
	stageStreaming
	stageExecute
	numStages
)

var stageWorkers = [numStages]int{stageRead: 64, stageMutation: 64, stageGossip: 16, stageMetadata: 8, stageStreaming: 4, stageExecute: 32}

func stageOf(v Verb) int {
	switch v {
	case VerbReadRow, VerbReadDigest, VerbReadRange:
		return stageRead
	case VerbMutation, VerbCounterMutation, VerbLWTPrepare, VerbLWTPropose, VerbLWTCommit:
		return stageMutation
	case VerbGossip, VerbPing, VerbPingReq, VerbBroadcast:
		return stageGossip
	case VerbPaxosPrepare, VerbPaxosAccept, VerbMetadataFetch, VerbMetadataPush:
		return stageMetadata
	case VerbStreamFile:
		return stageStreaming
	}
	// (Stream and repair orchestration, which wait on other nodes'
	// streaming workers, run with statements, so two nodes streaming to
	// each other cannot exhaust each other's streaming workers.)
	return stageExecute
}

// budget is a byte limit.
type budget struct {
	mu   sync.Mutex
	used int64
	cap  int64
	free *sync.Cond // signalled on release, for blocking acquirers
}

func newBudget(c int64) *budget {
	b := &budget{cap: c}
	b.free = sync.NewCond(&b.mu)
	return b
}

func (b *budget) tryAcquire(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+n > b.cap {
		return false
	}
	b.used += n
	return true
}

func (b *budget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	b.mu.Unlock()
	if b.free != nil {
		b.free.Broadcast()
	}
}

// reserve takes n bytes from the first budget with room for all of them,
// trying the connection's own, then the endpoint's and the global
// reserve together. It returns the release function, or nil.
func reserve(n int64, own, endpoint, global *budget) func() {
	if own.tryAcquire(n) {
		return func() { own.release(n) }
	}
	if endpoint.tryAcquire(n) {
		if global.tryAcquire(n) {
			return func() { endpoint.release(n); global.release(n) }
		}
		endpoint.release(n)
	}
	return nil
}

// NewMessaging listens on addr ("host:port").
func NewMessaging(addr string) (*Messaging, error) {
	return NewMessagingWith(addr, MessagingOptions{})
}

// NewMessagingWith listens on addr with options.
func NewMessagingWith(addr string, o MessagingOptions) (*Messaging, error) {
	if o.QueueCapacity <= 0 {
		o.QueueCapacity = 4 << 20
	}
	if o.EndpointReserve <= 0 {
		o.EndpointReserve = 128 << 20
	}
	if o.GlobalReserve <= 0 {
		o.GlobalReserve = 512 << 20
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	m := &Messaging{addr: ln.Addr().String(), ln: ln, opts: o,
		conns: map[outKey]*outConn{}, inbound: map[net.Conn]struct{}{}, peerVer: map[string]uint16{},
		outEndpoint: map[string]*budget{}, inEndpoint: map[string]*budget{}}
	m.outGlobal.cap, m.inGlobal.cap = o.GlobalReserve, o.GlobalReserve
	m.inGlobal.free = sync.NewCond(&m.inGlobal.mu)
	for i := range m.stages {
		m.stages[i] = make(chan struct{}, stageWorkers[i])
	}
	if o.TLS != nil {
		m.ln = tls.NewListener(ln, o.TLS)
	}
	return m, nil
}

// Addr returns the address the node listens on.
func (m *Messaging) Addr() string { return m.addr }

// Handle registers the handler of a verb. Call before Start.
func (m *Messaging) Handle(v Verb, h Handler) { m.handlers[v] = h }

// PeerVersion returns the protocol version agreed with a peer, 0 if not
// connected yet.
func (m *Messaging) PeerVersion(addr string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int(m.peerVer[addr])
}

// Stats are messaging counters (InternodeOutboundMetrics, dropped
// messages).
type Stats struct {
	// Dropped counts requests dropped because they expired, by verb.
	Dropped map[Verb]uint64
	// Overloaded counts requests refused for exceeding the queue limits.
	Overloaded uint64
}

// Stats returns the counters.
func (m *Messaging) Stats() Stats {
	s := Stats{Dropped: map[Verb]uint64{}, Overloaded: m.overloaded.Load()}
	for v := range m.dropped {
		if n := m.dropped[v].Load(); n > 0 {
			s.Dropped[Verb(v)] = n
		}
	}
	return s
}

func (m *Messaging) endpointBudget(peers map[string]*budget, addr string, blocking bool) *budget {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := peers[addr]
	if b == nil {
		b = &budget{cap: m.opts.EndpointReserve}
		if blocking {
			b.free = sync.NewCond(&b.mu)
		}
		peers[addr] = b
	}
	return b
}

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
	m.inGlobal.free.Broadcast()
	m.wg.Wait()
	return err
}

// ---- frames ----

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

type frame struct {
	kind     byte
	verb     Verb
	id       uint64
	deadline int64 // unix nanoseconds, 0 for none
	payload  []byte
	size     int // bytes on the wire
}

func writeFrame(w io.Writer, kind byte, verb Verb, id uint64, deadline int64, payload []byte) error {
	b := make([]byte, headerLen, headerLen+len(payload))
	binary.BigEndian.PutUint32(b, uint32(headerLen-4+len(payload)))
	b[4], b[5] = kind, byte(verb)
	binary.BigEndian.PutUint64(b[6:], id)
	binary.BigEndian.PutUint64(b[14:], uint64(deadline))
	crc := crc32.Update(crc32.Checksum(b[4:22], castagnoli), castagnoli, payload)
	binary.BigEndian.PutUint32(b[22:], crc)
	_, err := w.Write(append(b, payload...))
	return err
}

func readFrame(r io.Reader) (frame, error) {
	var hdr [headerLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n < headerLen-4 || n > maxFrame {
		return frame{}, fmt.Errorf("cluster: bad frame length %d", n)
	}
	f := frame{kind: hdr[4], verb: Verb(hdr[5]), id: binary.BigEndian.Uint64(hdr[6:]),
		deadline: int64(binary.BigEndian.Uint64(hdr[14:])), size: int(n) + 4}
	f.payload = make([]byte, int(n)-(headerLen-4))
	if _, err := io.ReadFull(r, f.payload); err != nil {
		return frame{}, err
	}
	crc := crc32.Update(crc32.Checksum(hdr[4:22], castagnoli), castagnoli, f.payload)
	if crc != binary.BigEndian.Uint32(hdr[22:]) {
		return frame{}, errCorrupt
	}
	var err error
	f.kind, f.payload, err = unpack(f.kind, f.payload)
	return f, err
}

// ---- handshake (HandshakeProtocol) ----

// hello is magic (4) | version (2) | min version (2) | class (1) | address.
func writeHello(w io.Writer, class int, addr string) error {
	b := append(handshakeMagic[:], 0, 0, 0, 0, byte(class))
	binary.BigEndian.PutUint16(b[4:], ProtocolVersion)
	binary.BigEndian.PutUint16(b[6:], MinProtocolVersion)
	b = binary.BigEndian.AppendUint16(b, uint16(len(addr)))
	_, err := w.Write(append(b, addr...))
	return err
}

func readHello(r io.Reader) (version, minVersion uint16, class int, addr string, err error) {
	var b [11]byte
	if _, err = io.ReadFull(r, b[:]); err != nil {
		return
	}
	if [4]byte(b[:4]) != handshakeMagic {
		return 0, 0, 0, "", errors.New("cluster: not a messaging connection")
	}
	version, minVersion, class = binary.BigEndian.Uint16(b[4:]), binary.BigEndian.Uint16(b[6:]), int(b[8])
	a := make([]byte, binary.BigEndian.Uint16(b[9:]))
	if _, err = io.ReadFull(r, a); err != nil {
		return
	}
	return version, minVersion, class, string(a), nil
}

// negotiate picks the version both sides speak, or fails.
func negotiate(peerVersion, peerMin uint16) (uint16, error) {
	v := min(peerVersion, ProtocolVersion)
	if v < peerMin || v < MinProtocolVersion {
		return 0, fmt.Errorf("cluster: no common messaging version (ours %d-%d, peer's %d-%d)",
			MinProtocolVersion, ProtocolVersion, peerMin, peerVersion)
	}
	return v, nil
}

// ---- inbound ----

func (m *Messaging) serve(c net.Conn) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.inbound, c)
		m.mu.Unlock()
		c.Close()
	}()
	r := bufio.NewReaderSize(c, 64<<10)
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	version, minVersion, _, from, err := readHello(r)
	if err != nil {
		return
	}
	v, err := negotiate(version, minVersion)
	reply := append(handshakeMagic[:], 0, 0, 0)
	binary.BigEndian.PutUint16(reply[4:], ProtocolVersion)
	if err == nil {
		reply[6] = 1
	}
	if _, werr := c.Write(reply); werr != nil || err != nil {
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	m.mu.Lock()
	m.peerVer[from] = v
	m.mu.Unlock()

	own := newBudget(m.opts.QueueCapacity)
	endpoint := m.endpointBudget(m.inEndpoint, from, true)
	var wmu sync.Mutex
	for {
		f, err := readFrame(r)
		if err != nil {
			return // a corrupt frame closes the connection, as Cassandra does
		}
		if f.kind != kindRequest {
			continue
		}
		if f.deadline != 0 && time.Now().UnixNano() > f.deadline {
			m.dropped[f.verb%numVerbs].Add(1)
			continue
		}
		// Hold the request's bytes while it is processed; past the limits,
		// stop reading until some are released (backpressure).
		release := m.reserveInbound(int64(f.size), own, endpoint)
		if release == nil {
			return // closing
		}
		go m.dispatch(c, &wmu, from, f, release)
	}
}

// reserveInbound waits for room for n bytes; nil if messaging closed.
func (m *Messaging) reserveInbound(n int64, own, endpoint *budget) func() {
	for {
		if r := reserve(n, own, endpoint, &m.inGlobal); r != nil {
			return func() { r(); own.free.Broadcast(); endpoint.free.Broadcast(); m.inGlobal.free.Broadcast() }
		}
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return nil
		}
		// Wait for any release (with a timeout, as releases may come from
		// other connections' budgets).
		own.mu.Lock()
		waitTimeout(own.free, 10*time.Millisecond)
		own.mu.Unlock()
	}
}

func waitTimeout(c *sync.Cond, d time.Duration) {
	t := time.AfterFunc(d, c.Broadcast)
	c.Wait()
	t.Stop()
}

func (m *Messaging) dispatch(c net.Conn, wmu *sync.Mutex, from string, f frame, release func()) {
	defer release()
	ctx := context.Background()
	cancel := func() {}
	if f.deadline != 0 {
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, f.deadline))
	}
	defer cancel()
	stage := m.stages[stageOf(f.verb)]
	select {
	case stage <- struct{}{}:
	case <-ctx.Done():
		m.dropped[f.verb%numVerbs].Add(1)
		return
	}
	defer func() { <-stage }()
	// (The deadline itself, not ctx.Err: the context's timer may not have
	// fired yet when a worker frees up right at the deadline.)
	if f.deadline != 0 && time.Now().UnixNano() >= f.deadline {
		m.dropped[f.verb%numVerbs].Add(1)
		return
	}
	var resp []byte
	var herr error
	if h := m.handlers[f.verb%numVerbs]; h == nil {
		herr = fmt.Errorf("cluster: no handler for verb %d", f.verb)
	} else {
		resp, herr = h(ctx, from, f.payload)
	}
	k := byte(kindResponse)
	if herr != nil {
		k, resp = kindError, []byte(herr.Error())
	}
	flag, resp := m.pack(resp)
	wmu.Lock()
	_ = writeFrame(c, k|flag, f.verb, f.id, 0, resp)
	wmu.Unlock()
}

// ---- outbound ----

type outKey struct {
	addr  string
	class int
}

// outConn is one connection to a peer, for one message class.
type outConn struct {
	c       net.Conn
	wmu     sync.Mutex
	mu      sync.Mutex
	pending map[uint64]chan result
	err     error
	own     *budget // the connection's queue capacity
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

func (m *Messaging) conn(to string, class int) (*outConn, error) {
	key := outKey{to, class}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("cluster: messaging closed")
	}
	if oc := m.conns[key]; oc != nil {
		oc.mu.Lock()
		dead := oc.err != nil
		oc.mu.Unlock()
		if !dead {
			m.mu.Unlock()
			return oc, nil
		}
		delete(m.conns, key)
	}
	m.mu.Unlock()
	c, err := m.dial(to, class)
	if err != nil {
		return nil, err
	}
	oc := &outConn{c: c, pending: map[uint64]chan result{}, own: &budget{cap: m.opts.QueueCapacity}}
	m.mu.Lock()
	if old := m.conns[key]; old != nil && old.err == nil {
		m.mu.Unlock()
		c.Close()
		return old, nil
	}
	m.conns[key] = oc
	m.mu.Unlock()
	go func() {
		r := bufio.NewReaderSize(c, 64<<10)
		for {
			f, err := readFrame(r)
			if err != nil {
				oc.close(fmt.Errorf("cluster: connection to %s lost: %w", to, err))
				return
			}
			oc.mu.Lock()
			ch := oc.pending[f.id]
			delete(oc.pending, f.id)
			oc.mu.Unlock()
			if ch == nil {
				continue
			}
			if f.kind == kindError {
				ch <- result{err: errors.New(string(f.payload))}
			} else {
				ch <- result{payload: f.payload}
			}
		}
	}()
	return oc, nil
}

// dial connects and performs the handshake.
func (m *Messaging) dial(to string, class int) (net.Conn, error) {
	const timeout = 2 * time.Second
	c, err := net.DialTimeout("tcp", to, timeout)
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
		c = tls.Client(c, cfg)
	}
	_ = c.SetDeadline(time.Now().Add(timeout))
	fail := func(err error) (net.Conn, error) {
		c.Close()
		return nil, err
	}
	if err := writeHello(c, class, m.addr); err != nil {
		return fail(err)
	}
	var reply [7]byte
	if _, err := io.ReadFull(c, reply[:]); err != nil {
		return fail(err)
	}
	if [4]byte(reply[:4]) != handshakeMagic {
		return fail(errors.New("cluster: not a messaging peer"))
	}
	peer := binary.BigEndian.Uint16(reply[4:])
	if reply[6] != 1 {
		return fail(fmt.Errorf("cluster: %s refused messaging version %d (it speaks %d)", to, ProtocolVersion, peer))
	}
	_ = c.SetDeadline(time.Time{})
	m.mu.Lock()
	m.peerVer[to] = min(peer, ProtocolVersion)
	m.mu.Unlock()
	return c, nil
}

// Request sends a request to node to and waits for its response.
func (m *Messaging) Request(ctx context.Context, to string, verb Verb, payload []byte) ([]byte, error) {
	if to == m.addr {
		h := m.handlers[verb%numVerbs]
		if h == nil {
			return nil, fmt.Errorf("cluster: no handler for verb %d", verb)
		}
		return h(ctx, m.addr, payload)
	}
	class := classOf(verb, len(payload))
	oc, err := m.conn(to, class)
	if err != nil {
		return nil, err
	}
	flag, packed := m.pack(payload)
	size := int64(headerLen + len(packed))
	release := reserve(size, oc.own, m.endpointBudget(m.outEndpoint, to, false), &m.outGlobal)
	if release == nil {
		m.overloaded.Add(1)
		return nil, ErrOverloaded
	}
	defer release()
	var deadline int64
	if d, ok := ctx.Deadline(); ok {
		deadline = d.UnixNano()
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
	oc.wmu.Lock()
	err = writeFrame(oc.c, kindRequest|flag, verb, id, deadline, packed)
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
