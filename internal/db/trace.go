package db

import (
	"strings"

	"crypto/rand"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Query tracing (TRACING ON): a traced request carries a Trace its
// coordinator records events into, from parsing to the replicas it asks
// and what they answer. The node keeps recent traces (system_traces'
// 24 hour TTL, at most traceLimit) and serves them as the virtual tables
// system_traces.sessions and system_traces.events. Replicas do not record
// events of their own.

const (
	traceTTL   = 24 * time.Hour
	traceLimit = 1000
)

// Trace is one traced request (a system_traces.sessions row and its
// events).
type Trace struct {
	ID       [16]byte
	Started  time.Time
	Client   string
	Request  string
	Params   map[string]string
	duration time.Duration

	mu     sync.Mutex
	events []traceEvent
}

type traceEvent struct {
	id       [16]byte // a timeuuid
	at       time.Time
	activity string
	source   string
}

// NewTrace starts a trace of a request from client.
func NewTrace(request, client string, params map[string]string) *Trace {
	t := &Trace{Started: time.Now(), Client: client, Request: request, Params: params}
	t.ID = timeUUID(t.Started)
	return t
}

func timeUUID(at time.Time) [16]byte {
	var u [16]byte
	_, _ = rand.Read(u[8:])
	// 100ns intervals since 1582-10-15.
	ts := uint64(at.UnixNano()/100) + 0x01b21dd213814000
	u[0], u[1], u[2], u[3] = byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts)
	u[4], u[5] = byte(ts>>40), byte(ts>>32)
	u[6], u[7] = byte(ts>>56)&0x0f|0x10, byte(ts>>48)
	u[8] = u[8]&0x3f | 0x80
	return u
}

// event records an activity; it is safe to call on a nil Trace.
func (t *Trace) event(format string, args ...any) {
	if t == nil {
		return
	}
	now := time.Now()
	t.mu.Lock()
	t.events = append(t.events, traceEvent{id: timeUUID(now), at: now, activity: fmt.Sprintf(format, args...)})
	t.mu.Unlock()
}

// traces keeps a node's recent traces.
type traces struct {
	mu   sync.Mutex
	byID map[[16]byte]*Trace
}

// FinishTrace records a finished trace on this node.
func (d *DB) FinishTrace(t *Trace) {
	t.event("Request complete")
	t.mu.Lock()
	t.duration = time.Since(t.Started)
	t.mu.Unlock()
	d.traces.mu.Lock()
	defer d.traces.mu.Unlock()
	if d.traces.byID == nil {
		d.traces.byID = map[[16]byte]*Trace{}
	}
	d.traces.byID[t.ID] = t
	if len(d.traces.byID) > traceLimit {
		var oldest *Trace
		for _, x := range d.traces.byID {
			if oldest == nil || x.Started.Before(oldest.Started) {
				oldest = x
			}
		}
		delete(d.traces.byID, oldest.ID)
	}
}

func (d *DB) liveTraces() []*Trace {
	d.traces.mu.Lock()
	defer d.traces.mu.Unlock()
	var out []*Trace
	for id, t := range d.traces.byID {
		if time.Since(t.Started) > traceTTL {
			delete(d.traces.byID, id)
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started.Before(out[j].Started) })
	return out
}

func (d *DB) traceSource() (addr net.IP, port int) {
	addr, port = d.opts.Address, 7000
	if d.proxy != nil {
		if h, p, err := net.SplitHostPort(d.proxy.self); err == nil {
			if ip := net.ParseIP(h); ip != nil {
				addr = ip
			}
			port, _ = strconv.Atoi(p)
		}
	}
	return addr, port
}

func traceSessionRows(d *DB) []map[string][]byte {
	src, port := d.traceSource()
	var out []map[string][]byte
	for _, t := range d.liveTraces() {
		t.mu.Lock()
		params := map[string]string{}
		for k, v := range t.Params {
			params[k] = v
		}
		r := map[string][]byte{"session_id": uuidV(t.ID[:]), "coordinator": inetV(src), "coordinator_port": intV(port),
			"duration": intV(int(t.duration / time.Microsecond)), "request": textV(t.Request),
			"command": textV("QUERY"), "started_at": bigintV(t.Started.UnixMilli()), "parameters": textMapV(params)}
		if ip := net.ParseIP(t.Client); ip != nil {
			r["client"] = inetV(ip)
		}
		t.mu.Unlock()
		out = append(out, r)
	}
	return out
}

func traceEventRows(d *DB) []map[string][]byte {
	src, port := d.traceSource()
	var out []map[string][]byte
	for _, t := range d.liveTraces() {
		t.mu.Lock()
		for _, e := range t.events {
			out = append(out, map[string][]byte{"session_id": uuidV(t.ID[:]), "event_id": uuidV(e.id[:]),
				"activity": textV(e.activity), "source": inetV(src), "source_port": intV(port),
				"source_elapsed": intV(int(e.at.Sub(t.Started) / time.Microsecond)), "thread": textV("coordinator")})
		}
		t.mu.Unlock()
	}
	return out
}

func inetV(ip net.IP) []byte {
	if v4 := ip.To4(); v4 != nil {
		return []byte(v4)
	}
	return []byte(ip.To16())
}

// clName is a consistency level's CQL name.
func clName(cl primitive.ConsistencyLevel) string {
	if f := strings.Fields(cl.String()); len(f) >= 2 {
		return f[1]
	}
	return cl.String()
}
