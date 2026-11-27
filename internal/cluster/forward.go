package cluster

import (
	"context"
	"encoding/binary"
	"errors"
)

// Forwarding (Cassandra's FORWARD_TO and RESPOND_TO message parameters):
// to reach several replicas in a remote data center, a coordinator sends
// the request once, to one of them, with the others' addresses. That node
// delivers it to each of them over its own (local) connections, and each
// sends its response straight back to the coordinator, under a request id
// the coordinator registered for it. So a write crosses the link between
// data centers once per data center instead of once per replica.
//
// A forwarded node that cannot be reached gets no request; the node that
// was to relay it reports that to the coordinator as that node's error.

// Reply is one node's response to a request.
type Reply struct {
	From    string
	Payload []byte
	Err     error
}

// relayWait is a request sent through another node, awaiting the response
// of the node it is for.
type relayWait struct {
	from, via string
	ch        chan Reply
}

// RequestForward sends a request to via, which also delivers it to each of
// forward; every node answers this node directly. It returns a channel that
// receives exactly one reply per node (via and each of forward), in the
// order they arrive; a node that has not answered when ctx ends gets
// ErrTimeout (or ctx's error).
//
// It falls back to sending to each node directly when the request could
// not be sent to via at all.
func (m *Messaging) RequestForward(ctx context.Context, via string, forward []string, verb Verb, payload []byte) <-chan Reply {
	out := make(chan Reply, len(forward)+1)
	direct := func(nodes []string) {
		for _, to := range nodes {
			go func() {
				p, err := m.Request(ctx, to, verb, payload)
				out <- Reply{From: to, Payload: p, Err: err}
			}()
		}
	}
	if len(forward) == 0 || !m.canForward(via, forward) {
		direct(append([]string{via}, forward...))
		return out
	}
	ids := make([]uint64, len(forward))
	m.relayMu.Lock()
	for i, to := range forward {
		ids[i] = m.nextID.Add(1)
		m.relays[ids[i]] = relayWait{from: to, via: via, ch: out}
	}
	m.relayMu.Unlock()
	// takeAll unregisters the forwarded requests not answered yet.
	takeAll := func() []relayWait {
		m.relayMu.Lock()
		defer m.relayMu.Unlock()
		var ws []relayWait
		for _, id := range ids {
			if w, ok := m.relays[id]; ok {
				delete(m.relays, id)
				ws = append(ws, w)
			}
		}
		return ws
	}
	stop := context.AfterFunc(ctx, func() {
		err := ErrTimeout
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = ctx.Err()
		}
		for _, w := range takeAll() {
			w.ch <- Reply{From: w.from, Err: err}
		}
	})
	m.forwards.Add(1)
	body := encodeForward(forward, ids, payload)
	go func() {
		p, err := m.request(ctx, via, verb, kindForward, body)
		var notSent errNotSent
		if errors.As(err, &notSent) {
			// Nothing left: the others are sent their copies directly.
			if stop() {
				var nodes []string
				for _, w := range takeAll() {
					nodes = append(nodes, w.from)
				}
				direct(nodes)
			}
		}
		out <- Reply{From: via, Payload: p, Err: err}
	}()
	return out
}

// canForward reports whether a request can go through via for forward
// (every node speaks forwarding: the cluster runs one version).
func (m *Messaging) canForward(via string, _ []string) bool { return via != m.addr }

// relay delivers a forwarded request to node to, for the coordinator; when
// it cannot, it reports the failure to the coordinator through reply.
func (m *Messaging) relay(to, coordinator string, id uint64, verb Verb, deadline int64, body []byte, reply func(kind byte, resp []byte)) {
	m.relayed.Add(1)
	msg := binary.AppendUvarint(nil, uint64(len(coordinator)))
	msg = append(append(msg, coordinator...), body...)
	if err := m.send(to, kindRelayed, verb, id, deadline, msg); err != nil {
		// Reported under the forwarding request's own frame id, so the
		// forwarded node's id leads the payload.
		e := binary.AppendUvarint([]byte{idMarker}, id)
		reply(kindRelayedError, append(e, err.Error()...))
	}
}

// idMarker leads a relayed error sent by the relaying node (whose frame id
// is the forwarding request's): the forwarded node's id follows, then the
// error.
const idMarker = 0xff

// send writes a message that expects no response on this connection.
func (m *Messaging) send(to string, kind byte, verb Verb, id uint64, deadline int64, payload []byte) error {
	flag, packed := m.pack(payload)
	oc, err := m.conn(to, classOf(verb, len(packed)))
	if err != nil {
		return err
	}
	release := reserve(int64(headerLen+len(packed)), oc.own, m.endpointBudget(m.outEndpoint, to, false), &m.outGlobal)
	if release == nil {
		m.overloaded.Add(1)
		return ErrOverloaded
	}
	defer release()
	oc.wmu.Lock()
	err = writeFrameTo(oc.c, kind|flag, verb, id, deadline, packed)
	oc.wmu.Unlock()
	if err != nil {
		oc.close(err)
	}
	return err
}

// deliverRelayed hands a relayed response, received from sender, to the
// request awaiting it.
func (m *Messaging) deliverRelayed(f frame, sender string) {
	id, payload := f.id, f.payload
	if f.kind == kindRelayedError && len(payload) > 0 && payload[0] == idMarker {
		// From the relaying node: the forwarded node could not be reached.
		fid, n := binary.Uvarint(payload[1:])
		if n <= 0 {
			return
		}
		id, payload = fid, append([]byte("cluster: forwarding failed: "), payload[1+n:]...)
	}
	m.relayMu.Lock()
	w, ok := m.relays[id]
	if ok && sender != w.from && sender != w.via {
		ok = false // not from a node the request involves
	}
	if ok {
		delete(m.relays, id)
	}
	m.relayMu.Unlock()
	if !ok {
		return
	}
	r := Reply{From: w.from}
	if f.kind == kindRelayedError {
		r.Err = errors.New(string(payload))
	} else {
		r.Payload = payload
	}
	w.ch <- r
}

// encodeForward prefixes a payload with the forwarded nodes and their ids.
func encodeForward(nodes []string, ids []uint64, payload []byte) []byte {
	b := binary.AppendUvarint(nil, uint64(len(nodes)))
	for i, n := range nodes {
		b = binary.AppendUvarint(b, ids[i])
		b = binary.AppendUvarint(b, uint64(len(n)))
		b = append(b, n...)
	}
	return append(b, payload...)
}

var errBadForward = errors.New("cluster: corrupt forwarding header")

func decodeForward(b []byte) (nodes []string, ids []uint64, payload []byte, err error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > 1024 {
		return nil, nil, nil, errBadForward
	}
	b = b[k:]
	for range n {
		id, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, nil, nil, errBadForward
		}
		b = b[k:]
		l, k := binary.Uvarint(b)
		if k <= 0 || uint64(len(b)-k) < l {
			return nil, nil, nil, errBadForward
		}
		nodes, ids = append(nodes, string(b[k:k+int(l)])), append(ids, id)
		b = b[k+int(l):]
	}
	return nodes, ids, b, nil
}

func decodeRespondTo(b []byte) (string, []byte, error) {
	l, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < l {
		return "", nil, errBadForward
	}
	return string(b[k : k+int(l)]), b[k+int(l):], nil
}
