package cluster

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Membership tracks which nodes are alive. Cluster metadata (tokens, node
// states, schema) is not gossiped: it comes from the metadata log (see
// CMS, after Cassandra's CEP-21 Transactional Cluster Metadata). What
// gossip carries is each node's identity, liveness and metadata epoch,
// which tells others when they need to catch up.
//
// Liveness is SWIM as hashicorp/memberlist implements it, with its
// Lifeguard extensions:
//
//   - probing: every probe interval one node, in shuffled round-robin
//     order, is pinged; if it does not answer, IndirectChecks other nodes are
//     asked to ping it, so one bad link does not condemn it;
//   - suspicion: a node that fails both is suspect, not dead; it is declared
//     dead when a suspicion timeout expires, which starts at max and shrinks
//     towards min (SuspicionMult * log10(N) * probe interval) as other nodes
//     independently confirm the suspicion (Lifeguard);
//   - refutation: a node told it is suspect or dead raises its incarnation
//     and announces that it is alive; liveness orders (generation,
//     incarnation), so a restarted node (a new generation) also wins over
//     stale rumours;
//   - awareness: a local health score grows when this node's own probes
//     look unreliable (helpers that do not answer), scaling its probe
//     interval and timeouts, so a slow node does not accuse healthy ones
//     (Lifeguard);
//   - dissemination: alive, suspect and dead messages and state changes are
//     piggybacked on probes and gossiped to GossipNodes random nodes every
//     gossip interval, each retransmitted RetransmitMult * ceil(log10(N+1))
//     times, a newer message about a node replacing the older one
//     (TransmitLimitedQueue);
//   - push-pull: full views are exchanged with a random node every
//     PushPullInterval and with the seeds at start (anti-entropy).

// EndpointState is what gossip spreads about a node. Generation is the
// node's start time; Version grows with each change of its state, so the
// newest wins.
type EndpointState struct {
	Addr       string // internode address
	HostID     string
	Epoch      int64 // the node's cluster metadata epoch
	Generation int64
	Version    int64
}

func (s *EndpointState) newer(o *EndpointState) bool {
	return s.Generation > o.Generation || s.Generation == o.Generation && s.Version > o.Version
}

// Liveness states (memberlist's NodeStateType).
const (
	stateAlive   = "alive"
	stateSuspect = "suspect"
	stateDead    = "dead"
)

type member struct {
	State       EndpointState
	Liveness    string
	Incarnation uint64
	changed     time.Time
}

// Config tunes membership; the defaults are memberlist's LAN settings.
type Config struct {
	ProbeInterval, ProbeTimeout time.Duration
	IndirectChecks              int
	SuspicionMult               int
	SuspicionMaxTimeoutMult     int
	// MinSuspicionTimeout, if set, replaces the computed minimum (tests).
	MinSuspicionTimeout time.Duration
	GossipInterval      time.Duration
	GossipNodes         int
	RetransmitMult      int
	PushPullInterval    time.Duration
	GossipToTheDeadTime time.Duration
	AwarenessMax        int
}

// DefaultConfig is memberlist's DefaultLANConfig.
func DefaultConfig() Config {
	return Config{ProbeInterval: time.Second, ProbeTimeout: 500 * time.Millisecond, IndirectChecks: 3,
		SuspicionMult: 4, SuspicionMaxTimeoutMult: 6, GossipInterval: 200 * time.Millisecond, GossipNodes: 3,
		RetransmitMult: 4, PushPullInterval: 30 * time.Second, GossipToTheDeadTime: 30 * time.Second, AwarenessMax: 8}
}

// Gossiper maintains the cluster's membership.
type Gossiper struct {
	m     *Messaging
	seeds []string
	cfg   Config
	self  string

	mu         sync.Mutex
	members    map[string]*member
	suspicions map[string]*suspicion
	probeOrder []string
	probeIndex int
	queue      []*queued
	health     int // Lifeguard awareness score

	listeners []func(addr string, up bool)
	changed   []func()
	stop      chan struct{}
	done      sync.WaitGroup
}

// NewGossiper creates the membership of the node whose state is local.
func NewGossiper(m *Messaging, local EndpointState, seeds []string) *Gossiper {
	local.Addr = m.Addr()
	local.Generation = time.Now().UnixNano()
	g := &Gossiper{m: m, seeds: seeds, cfg: DefaultConfig(), self: local.Addr,
		members: map[string]*member{}, suspicions: map[string]*suspicion{}, stop: make(chan struct{})}
	g.members[g.self] = &member{State: local, Liveness: stateAlive, changed: time.Now()}
	m.Handle(VerbGossip, g.handlePushPull)
	m.Handle(VerbPing, g.handlePing)
	m.Handle(VerbPingReq, g.handlePingReq)
	m.Handle(VerbBroadcast, g.handleBroadcast)
	return g
}

// SetConfig replaces the configuration; call before Start.
func (g *Gossiper) SetConfig(c Config) { g.cfg = c }

// Config returns the configuration.
func (g *Gossiper) Config() Config { return g.cfg }

// OnLiveness registers a callback for nodes going up or down (dead).
func (g *Gossiper) OnLiveness(f func(addr string, up bool)) {
	g.mu.Lock()
	g.listeners = append(g.listeners, f)
	g.mu.Unlock()
}

// OnChange registers a callback for membership or epoch changes.
func (g *Gossiper) OnChange(f func()) {
	g.mu.Lock()
	g.changed = append(g.changed, f)
	g.mu.Unlock()
}

// Update changes the local state and announces it.
func (g *Gossiper) Update(f func(s *EndpointState)) {
	g.mu.Lock()
	me := g.members[g.self]
	f(&me.State)
	me.State.Version++
	st := me.State
	g.queueLocked(message{Kind: kindState, Node: g.self, Inc: me.Incarnation, State: &st})
	g.mu.Unlock()
}

// Local returns a copy of the local state.
func (g *Gossiper) Local() EndpointState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.members[g.self].State
}

// States returns copies of every known node's state.
func (g *Gossiper) States() []EndpointState {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]EndpointState, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, m.State)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// IsAlive reports whether a node is up: alive or suspect (a suspect has
// not failed until its suspicion times out).
func (g *Gossiper) IsAlive(addr string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.members[addr]
	return m != nil && m.Liveness != stateDead
}

// Start joins through the seeds and starts probing and gossiping.
func (g *Gossiper) Start() {
	for _, s := range g.seeds {
		if s != g.self {
			g.pushPull(s)
		}
	}
	g.every(g.cfg.GossipInterval, g.gossip)
	g.every(g.cfg.PushPullInterval, func() {
		if n := g.randomNodes(1, false); len(n) > 0 {
			g.pushPull(n[0])
		} else if len(g.seeds) > 0 {
			if s := g.seeds[rand.Intn(len(g.seeds))]; s != g.self {
				g.pushPull(s)
			}
		}
	})
	g.done.Add(1)
	go func() {
		defer g.done.Done()
		for {
			g.mu.Lock()
			interval := g.scaleLocked(g.cfg.ProbeInterval)
			g.mu.Unlock()
			select {
			case <-g.stop:
				return
			case <-time.After(interval):
			}
			g.probe()
		}
	}()
}

func (g *Gossiper) every(d time.Duration, f func()) {
	g.done.Add(1)
	go func() {
		defer g.done.Done()
		// Stagger the first run, as memberlist does.
		select {
		case <-g.stop:
			return
		case <-time.After(time.Duration(rand.Int63n(int64(d)))):
		}
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			f()
			select {
			case <-g.stop:
				return
			case <-t.C:
			}
		}
	}()
}

// Stop stops probing and gossiping.
func (g *Gossiper) Stop() {
	close(g.stop)
	g.done.Wait()
	g.mu.Lock()
	for _, s := range g.suspicions {
		s.timer.Stop()
	}
	g.mu.Unlock()
}

// Remove forgets a node.
func (g *Gossiper) Remove(addr string) {
	g.mu.Lock()
	delete(g.members, addr)
	g.mu.Unlock()
}

func (g *Gossiper) scaleLocked(d time.Duration) time.Duration {
	return d * time.Duration(g.health+1)
}

func (g *Gossiper) applyHealth(delta int) {
	g.mu.Lock()
	g.health = max(0, min(g.cfg.AwarenessMax-1, g.health+delta))
	g.mu.Unlock()
}

// ---- probing ----

type pingMsg struct {
	Target     string
	Broadcasts []message `json:",omitempty"`
}

type ackMsg struct {
	Ack        bool
	Broadcasts []message `json:",omitempty"`
}

// probe pings the next node in shuffled round-robin order.
func (g *Gossiper) probe() {
	g.mu.Lock()
	var target *member
	for checked := 0; checked <= len(g.members); checked++ {
		if g.probeIndex >= len(g.probeOrder) {
			g.probeOrder = g.probeOrder[:0]
			for a := range g.members {
				g.probeOrder = append(g.probeOrder, a)
			}
			rand.Shuffle(len(g.probeOrder), func(i, j int) { g.probeOrder[i], g.probeOrder[j] = g.probeOrder[j], g.probeOrder[i] })
			g.probeIndex = 0
		}
		if len(g.probeOrder) == 0 {
			break
		}
		m := g.members[g.probeOrder[g.probeIndex]]
		g.probeIndex++
		if m != nil && m.State.Addr != g.self && m.Liveness != stateDead {
			target = m
			break
		}
	}
	if target == nil {
		g.mu.Unlock()
		return
	}
	addr, inc := target.State.Addr, target.Incarnation
	interval, timeout := g.scaleLocked(g.cfg.ProbeInterval), g.scaleLocked(g.cfg.ProbeTimeout)
	g.mu.Unlock()
	g.probeNode(addr, inc, interval, timeout)
}

// ping sends a direct ping carrying broadcasts and applies the ack's.
func (g *Gossiper) ping(addr string, timeout time.Duration) bool {
	req, _ := json.Marshal(pingMsg{Target: addr, Broadcasts: g.takeBroadcasts()})
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	b, err := g.m.Request(ctx, addr, VerbPing, req)
	if err != nil {
		return false
	}
	var ack ackMsg
	if json.Unmarshal(b, &ack) != nil {
		return false
	}
	g.apply(ack.Broadcasts)
	return ack.Ack
}

// probeNode is Memberlist.probeNode: a direct ping, then indirect pings
// through IndirectChecks other nodes, then suspicion.
func (g *Gossiper) probeNode(addr string, inc uint64, interval, timeout time.Duration) {
	if g.ping(addr, timeout) {
		g.applyHealth(-1)
		return
	}
	helpers := g.randomNodes(g.cfg.IndirectChecks, true, addr)
	type reply struct{ answered, ack bool }
	replies := make(chan reply, len(helpers))
	req, _ := json.Marshal(pingMsg{Target: addr})
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	for _, h := range helpers {
		go func() {
			b, err := g.m.Request(ctx, h, VerbPingReq, req)
			if err != nil {
				replies <- reply{}
				return
			}
			var ack ackMsg
			_ = json.Unmarshal(b, &ack)
			replies <- reply{answered: true, ack: ack.Ack}
		}()
	}
	missing := 0
	for range helpers {
		r := <-replies
		if r.ack {
			g.applyHealth(-1)
			return
		}
		if !r.answered {
			missing++
		}
	}
	// Lifeguard: helpers that do not even answer (memberlist's missing
	// nacks) suggest that this node is the unhealthy one.
	if len(helpers) == 0 {
		g.applyHealth(1)
	} else {
		g.applyHealth(missing)
	}
	g.suspect(message{Kind: stateSuspect, Node: addr, Inc: inc, From: g.self})
}

func (g *Gossiper) handlePing(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var p pingMsg
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	g.apply(p.Broadcasts)
	return json.Marshal(ackMsg{Ack: p.Target == g.self, Broadcasts: g.takeBroadcasts()})
}

// handlePingReq pings a target on another node's behalf.
func (g *Gossiper) handlePingReq(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var p pingMsg
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	g.mu.Lock()
	timeout := g.scaleLocked(g.cfg.ProbeTimeout)
	g.mu.Unlock()
	return json.Marshal(ackMsg{Ack: g.ping(p.Target, timeout)})
}

// randomNodes picks up to k nodes other than this one and the excluded
// ones, not dead (alive only if aliveOnly).
func (g *Gossiper) randomNodes(k int, aliveOnly bool, exclude ...string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []string
	for a, m := range g.members {
		if a == g.self || m.Liveness == stateDead || (aliveOnly && m.Liveness != stateAlive) {
			continue
		}
		skip := false
		for _, x := range exclude {
			skip = skip || x == a
		}
		if !skip {
			out = append(out, a)
		}
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	if len(out) > k {
		out = out[:k]
	}
	return out
}

// ---- liveness rules (aliveNode, suspectNode, deadNode) ----

const kindState = "state"

// message is a disseminated change.
type message struct {
	Kind  string // alive, suspect, dead, state
	Node  string
	Gen   int64
	Inc   uint64
	From  string         `json:",omitempty"`
	State *EndpointState `json:",omitempty"`
}

func (g *Gossiper) apply(msgs []message) {
	for _, msg := range msgs {
		switch msg.Kind {
		case stateAlive, kindState:
			g.alive(msg)
		case stateSuspect:
			g.suspect(msg)
		case stateDead:
			g.dead(msg)
		}
	}
}

func (g *Gossiper) notify(up, down []string, changed bool) {
	g.mu.Lock()
	listeners, cbs := g.listeners, g.changed
	g.mu.Unlock()
	for _, a := range up {
		for _, f := range listeners {
			f(a, true)
		}
	}
	for _, a := range down {
		for _, f := range listeners {
			f(a, false)
		}
	}
	if changed || len(up) > 0 || len(down) > 0 {
		for _, f := range cbs {
			f()
		}
	}
}

// alive handles alive and state messages: a newer state replaces the old;
// a newer generation, or a higher incarnation, makes the node alive.
func (g *Gossiper) alive(msg message) {
	if msg.Node == g.self {
		return
	}
	g.mu.Lock()
	m := g.members[msg.Node]
	gen := msg.Gen
	if msg.State != nil {
		gen = msg.State.Generation
	}
	if m == nil {
		if msg.State == nil {
			g.mu.Unlock()
			return
		}
		g.members[msg.Node] = &member{State: *msg.State, Liveness: stateAlive, Incarnation: msg.Inc, changed: time.Now()}
		g.queueLocked(msg)
		g.mu.Unlock()
		g.notify([]string{msg.Node}, nil, true)
		return
	}
	changed := false
	if msg.State != nil && msg.State.newer(&m.State) {
		changed = msg.State.Epoch != m.State.Epoch
		if msg.State.Generation > m.State.Generation {
			m.Incarnation = 0 // a new life
		}
		m.State = *msg.State
		g.queueLocked(msg)
	}
	var up []string
	newer := gen > m.State.Generation || gen == m.State.Generation && msg.Inc > m.Incarnation
	if newer || gen == m.State.Generation && msg.Inc == m.Incarnation && msg.State != nil && m.Liveness == stateDead && changed {
		if m.Liveness == stateDead {
			up = append(up, msg.Node)
		}
		if s := g.suspicions[msg.Node]; s != nil {
			s.timer.Stop()
			delete(g.suspicions, msg.Node)
		}
		m.Liveness, m.Incarnation, m.changed = stateAlive, msg.Inc, time.Now()
		g.queueLocked(message{Kind: stateAlive, Node: msg.Node, Gen: gen, Inc: msg.Inc})
	}
	g.mu.Unlock()
	g.notify(up, nil, changed)
}

// suspect handles a suspicion (suspectNode).
func (g *Gossiper) suspect(msg message) {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.members[msg.Node]
	if m == nil || msg.Inc < m.Incarnation || msg.Gen != 0 && msg.Gen < m.State.Generation {
		return
	}
	if msg.Node == g.self {
		g.refuteLocked(msg.Inc)
		return
	}
	if s := g.suspicions[msg.Node]; s != nil {
		if s.confirm(msg.From) {
			g.queueLocked(msg)
		}
		return
	}
	if m.Liveness != stateAlive {
		return
	}
	msg.Gen = m.State.Generation
	g.queueLocked(msg)
	m.Liveness, m.Incarnation, m.changed = stateSuspect, msg.Inc, time.Now()
	n := len(g.members)
	k := g.cfg.SuspicionMult - 2
	if n-2 < k {
		k = 0
	}
	minT := suspicionTimeout(g.cfg.SuspicionMult, n, g.cfg.ProbeInterval)
	if g.cfg.MinSuspicionTimeout > 0 {
		minT = g.cfg.MinSuspicionTimeout
	}
	maxT := time.Duration(g.cfg.SuspicionMaxTimeoutMult) * minT
	node, since := msg.Node, m.changed
	g.suspicions[node] = newSuspicion(msg.From, k, minT, maxT, func() {
		g.mu.Lock()
		cur := g.members[node]
		expired := cur != nil && cur.Liveness == stateSuspect && cur.changed.Equal(since)
		var inc uint64
		var gen int64
		if cur != nil {
			inc, gen = cur.Incarnation, cur.State.Generation
		}
		g.mu.Unlock()
		if expired {
			g.dead(message{Kind: stateDead, Node: node, Gen: gen, Inc: inc, From: g.self})
		}
	})
}

// dead handles a death (deadNode).
func (g *Gossiper) dead(msg message) {
	g.mu.Lock()
	m := g.members[msg.Node]
	if m == nil || msg.Inc < m.Incarnation || msg.Gen != 0 && msg.Gen < m.State.Generation || m.Liveness == stateDead {
		g.mu.Unlock()
		return
	}
	if msg.Node == g.self {
		g.refuteLocked(msg.Inc)
		g.mu.Unlock()
		return
	}
	if s := g.suspicions[msg.Node]; s != nil {
		s.timer.Stop()
		delete(g.suspicions, msg.Node)
	}
	g.queueLocked(msg)
	m.Liveness, m.Incarnation, m.changed = stateDead, msg.Inc, time.Now()
	g.mu.Unlock()
	g.notify(nil, []string{msg.Node}, false)
}

// refuteLocked answers an accusation about this node: it raises its
// incarnation above the accused one and announces that it is alive.
func (g *Gossiper) refuteLocked(accused uint64) {
	me := g.members[g.self]
	me.Incarnation = max(me.Incarnation, accused) + 1
	g.health = min(g.cfg.AwarenessMax-1, g.health+1)
	g.queueLocked(message{Kind: stateAlive, Node: g.self, Gen: me.State.Generation, Inc: me.Incarnation})
}

// ---- suspicion (Lifeguard) ----

type suspicion struct {
	k, n          int
	min, max      time.Duration
	start         time.Time
	timer         *time.Timer
	fire          func()
	confirmations map[string]bool
}

func newSuspicion(from string, k int, minT, maxT time.Duration, fire func()) *suspicion {
	s := &suspicion{k: k, min: minT, max: maxT, start: time.Now(), fire: fire, confirmations: map[string]bool{from: true}}
	timeout := maxT
	if k < 1 {
		timeout = minT
	}
	s.timer = time.AfterFunc(timeout, fire)
	return s
}

// confirm counts an independent confirmation and shortens the timeout:
// max - log(n+1)/log(k+1) * (max - min) (remainingSuspicionTime).
func (s *suspicion) confirm(from string) bool {
	if s.n >= s.k || s.confirmations[from] {
		return false
	}
	s.confirmations[from] = true
	s.n++
	frac := math.Log(float64(s.n)+1) / math.Log(float64(s.k)+1)
	timeout := max(time.Duration(float64(s.max)-frac*float64(s.max-s.min)), s.min)
	if remaining := timeout - time.Since(s.start); s.timer.Stop() {
		if remaining > 0 {
			s.timer.Reset(remaining)
		} else {
			go s.fire()
		}
	}
	return true
}

// suspicionTimeout is SuspicionMult * max(1, log10(n)) * interval.
func suspicionTimeout(mult, n int, interval time.Duration) time.Duration {
	scale := math.Max(1, math.Log10(math.Max(1, float64(n))))
	return time.Duration(float64(mult) * scale * float64(interval))
}

// ---- dissemination (TransmitLimitedQueue) ----

type queued struct {
	msg       message
	transmits int
}

// queueLocked queues a message; it replaces a queued message about the
// same node's liveness (or state).
func (g *Gossiper) queueLocked(msg message) {
	key := func(m message) string {
		if m.Kind == kindState {
			return "s/" + m.Node
		}
		return "l/" + m.Node
	}
	k := key(msg)
	out := g.queue[:0]
	for _, it := range g.queue {
		if key(it.msg) != k {
			out = append(out, it)
		}
	}
	g.queue = append(out, &queued{msg: msg})
}

// retransmitLimit is RetransmitMult * ceil(log10(n+1)).
func retransmitLimit(mult, n int) int {
	return mult * int(math.Ceil(math.Log10(float64(n+1))))
}

// takeBroadcasts returns the messages to piggyback, least transmitted
// first, dropping those sent retransmitLimit times.
func (g *Gossiper) takeBroadcasts() []message {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.queue) == 0 {
		return nil
	}
	limit := retransmitLimit(g.cfg.RetransmitMult, len(g.members))
	sort.SliceStable(g.queue, func(i, j int) bool { return g.queue[i].transmits < g.queue[j].transmits })
	var out []message
	keep := g.queue[:0]
	for i, it := range g.queue {
		if i < 32 {
			out = append(out, it.msg)
			it.transmits++
		}
		if it.transmits < limit {
			keep = append(keep, it)
		}
	}
	g.queue = keep
	return out
}

// gossip sends pending messages to GossipNodes random nodes, including
// recently dead ones (Memberlist.gossip).
func (g *Gossiper) gossip() {
	g.mu.Lock()
	var targets []string
	for a, m := range g.members {
		if a == g.self || m.Liveness == stateDead && time.Since(m.changed) > g.cfg.GossipToTheDeadTime {
			continue
		}
		targets = append(targets, a)
	}
	pending := len(g.queue) > 0
	g.mu.Unlock()
	if !pending {
		return
	}
	rand.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })
	if len(targets) > g.cfg.GossipNodes {
		targets = targets[:g.cfg.GossipNodes]
	}
	for _, t := range targets {
		msgs := g.takeBroadcasts()
		if len(msgs) == 0 {
			return
		}
		b, _ := json.Marshal(msgs)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), g.cfg.ProbeTimeout)
			defer cancel()
			_, _ = g.m.Request(ctx, t, VerbBroadcast, b)
		}()
	}
}

func (g *Gossiper) handleBroadcast(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var msgs []message
	if err := json.Unmarshal(payload, &msgs); err != nil {
		return nil, err
	}
	g.apply(msgs)
	return nil, nil
}

// ---- push-pull ----

type pushPullState struct {
	State       EndpointState
	Liveness    string
	Incarnation uint64
}

func (g *Gossiper) snapshot() []pushPullState {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]pushPullState, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, pushPullState{State: m.State, Liveness: m.Liveness, Incarnation: m.Incarnation})
	}
	return out
}

// pushPull exchanges full views with a node (Memberlist.pushPullNode).
func (g *Gossiper) pushPull(to string) {
	b, _ := json.Marshal(g.snapshot())
	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.ProbeInterval+time.Second)
	defer cancel()
	resp, err := g.m.Request(ctx, to, VerbGossip, b)
	if err != nil {
		return
	}
	g.mergeState(resp)
}

func (g *Gossiper) handlePushPull(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	g.mergeState(payload)
	return json.Marshal(g.snapshot())
}

// mergeState applies a remote view (mergeState): states as state messages;
// a remote suspicion or death is only a suspicion here, as in memberlist.
func (g *Gossiper) mergeState(payload []byte) {
	var remote []pushPullState
	if json.Unmarshal(payload, &remote) != nil {
		return
	}
	for _, r := range remote {
		st := r.State
		g.alive(message{Kind: kindState, Node: st.Addr, Gen: st.Generation, Inc: r.Incarnation, State: &st})
		if r.Liveness == stateAlive {
			g.alive(message{Kind: stateAlive, Node: st.Addr, Gen: st.Generation, Inc: r.Incarnation})
		} else {
			g.suspect(message{Kind: stateSuspect, Node: st.Addr, Gen: st.Generation, Inc: r.Incarnation, From: "push-pull"})
		}
	}
}
