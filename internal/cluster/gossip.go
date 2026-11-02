package cluster

import (
	"context"
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Node statuses (VersionedValue STATUS).
const (
	StatusJoining = "JOINING"
	StatusNormal  = "NORMAL"
	StatusLeaving = "LEAVING"
	StatusLeft    = "LEFT"
)

// EndpointState is what gossip spreads about a node (EndpointState and
// its application states). Generation is the node's start time; Version
// grows with each heartbeat and change, so the newest state wins.
type EndpointState struct {
	Addr           string // internode address
	HostID         string
	DataCenter     string
	Rack           string
	Tokens         []int64
	Status         string
	NativeAddr     string
	NativePort     int
	SchemaVersion  string
	SchemaEpoch    int64
	ReleaseVersion string
	Generation     int64
	Version        int64
}

func (s *EndpointState) newer(o *EndpointState) bool {
	return s.Generation > o.Generation || s.Generation == o.Generation && s.Version > o.Version
}

// Gossiper keeps every node's state, exchanging it with a random live node
// (and sometimes a seed or an unreachable node) every interval, and marks
// nodes down when their heartbeat stops advancing (a simple failure
// detector in place of phi accrual).
type Gossiper struct {
	m         *Messaging
	seeds     []string
	interval  time.Duration
	downAfter time.Duration

	mu     sync.Mutex
	states map[string]*EndpointState
	seen   map[string]time.Time // when each node's version last advanced
	alive  map[string]bool
	local  *EndpointState

	listeners []func(addr string, up bool)
	changed   []func()
	stop      chan struct{}
	done      chan struct{}
}

// NewGossiper creates a gossiper for the node whose state is local.
func NewGossiper(m *Messaging, local EndpointState, seeds []string) *Gossiper {
	local.Addr = m.Addr()
	local.Generation = time.Now().UnixNano()
	g := &Gossiper{m: m, seeds: seeds, interval: time.Second, downAfter: 5 * time.Second,
		states: map[string]*EndpointState{}, seen: map[string]time.Time{}, alive: map[string]bool{},
		stop: make(chan struct{}), done: make(chan struct{})}
	g.local = &local
	g.states[local.Addr] = g.local
	g.alive[local.Addr] = true
	m.Handle(VerbGossip, g.handle)
	return g
}

// SetTiming sets the gossip interval and the failure detection delay.
func (g *Gossiper) SetTiming(interval, downAfter time.Duration) {
	g.interval, g.downAfter = interval, downAfter
}

// OnLiveness registers a callback for nodes going up or down.
func (g *Gossiper) OnLiveness(f func(addr string, up bool)) {
	g.mu.Lock()
	g.listeners = append(g.listeners, f)
	g.mu.Unlock()
}

// OnChange registers a callback for any state change (ring, schema).
func (g *Gossiper) OnChange(f func()) {
	g.mu.Lock()
	g.changed = append(g.changed, f)
	g.mu.Unlock()
}

// Update changes the local state and spreads it.
func (g *Gossiper) Update(f func(s *EndpointState)) {
	g.mu.Lock()
	f(g.local)
	g.local.Version++
	cbs := g.changed
	g.mu.Unlock()
	for _, cb := range cbs {
		cb()
	}
}

// Local returns a copy of the local state.
func (g *Gossiper) Local() EndpointState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return *g.local
}

// States returns copies of every known node's state.
func (g *Gossiper) States() []EndpointState {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]EndpointState, 0, len(g.states))
	for _, s := range g.states {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// IsAlive reports whether a node is up.
func (g *Gossiper) IsAlive(addr string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.alive[addr]
}

// Start begins gossiping; it first exchanges state with the seeds.
func (g *Gossiper) Start() {
	for _, s := range g.seeds {
		if s != g.local.Addr {
			g.exchange(s)
		}
	}
	go g.loop()
}

// Stop stops gossiping.
func (g *Gossiper) Stop() {
	close(g.stop)
	<-g.done
}

func (g *Gossiper) loop() {
	defer close(g.done)
	t := time.NewTicker(g.interval)
	defer t.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-t.C:
		}
		g.mu.Lock()
		g.local.Version++ // heartbeat
		var live, dead []string
		for addr := range g.states {
			if addr == g.local.Addr {
				continue
			}
			if g.alive[addr] {
				live = append(live, addr)
			} else {
				dead = append(dead, addr)
			}
		}
		g.mu.Unlock()
		if len(live) > 0 {
			g.exchange(live[rand.Intn(len(live))])
		}
		if len(dead) > 0 && rand.Intn(len(live)+1) == 0 {
			g.exchange(dead[rand.Intn(len(dead))])
		}
		if len(g.seeds) > 0 && (len(live) == 0 || rand.Intn(len(live)+1) == 0) {
			if s := g.seeds[rand.Intn(len(g.seeds))]; s != g.local.Addr {
				g.exchange(s)
			}
		}
		g.checkLiveness()
	}
}

// exchange sends our states to a node and merges its states back (one
// round of SYN/ACK, with full states: the clusters are small).
func (g *Gossiper) exchange(to string) {
	g.mu.Lock()
	payload, _ := json.Marshal(g.states)
	g.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), g.interval*2+time.Second)
	defer cancel()
	resp, err := g.m.Request(ctx, to, VerbGossip, payload)
	if err != nil {
		return
	}
	g.merge(resp)
}

func (g *Gossiper) handle(_ string, payload []byte) ([]byte, error) {
	g.merge(payload)
	g.mu.Lock()
	defer g.mu.Unlock()
	return json.Marshal(g.states)
}

func (g *Gossiper) merge(payload []byte) {
	var in map[string]*EndpointState
	if json.Unmarshal(payload, &in) != nil {
		return
	}
	now := time.Now()
	g.mu.Lock()
	changed := false
	var ups []string
	for addr, s := range in {
		if addr == g.local.Addr {
			continue
		}
		cur := g.states[addr]
		if cur == nil || s.newer(cur) {
			if cur == nil || cur.Status != s.Status || len(cur.Tokens) != len(s.Tokens) || cur.SchemaVersion != s.SchemaVersion {
				changed = true
			}
			g.states[addr] = s
			g.seen[addr] = now
			if !g.alive[addr] {
				g.alive[addr] = true
				ups = append(ups, addr)
				changed = true
			}
		}
	}
	listeners, cbs := g.listeners, g.changed
	g.mu.Unlock()
	for _, a := range ups {
		for _, f := range listeners {
			f(a, true)
		}
	}
	if changed {
		for _, f := range cbs {
			f()
		}
	}
}

func (g *Gossiper) checkLiveness() {
	now := time.Now()
	g.mu.Lock()
	var downs []string
	for addr := range g.states {
		if addr == g.local.Addr || !g.alive[addr] {
			continue
		}
		if now.Sub(g.seen[addr]) > g.downAfter {
			g.alive[addr] = false
			downs = append(downs, addr)
		}
	}
	listeners, cbs := g.listeners, g.changed
	g.mu.Unlock()
	for _, a := range downs {
		for _, f := range listeners {
			f(a, false)
		}
	}
	if len(downs) > 0 {
		for _, f := range cbs {
			f()
		}
	}
}

// Remove forgets a node (after it left the ring).
func (g *Gossiper) Remove(addr string) {
	g.mu.Lock()
	delete(g.states, addr)
	delete(g.alive, addr)
	delete(g.seen, addr)
	cbs := g.changed
	g.mu.Unlock()
	for _, f := range cbs {
		f()
	}
}
