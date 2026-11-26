package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMessaging(t *testing.T) {
	a, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	b, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	b.Handle(VerbMutation, func(_ context.Context, from string, p []byte) ([]byte, error) {
		if string(p) == "fail" {
			return nil, errors.New("boom")
		}
		return append([]byte(from+":"), p...), nil
	})
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		resp, err := a.Request(ctx, b.Addr(), VerbMutation, []byte("hi"))
		require.NoError(t, err)
		require.Equal(t, a.Addr()+":hi", string(resp))
	}
	_, err = a.Request(ctx, b.Addr(), VerbMutation, []byte("fail"))
	require.EqualError(t, err, "boom")
	tctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	b.Handle(VerbReadRow, func(context.Context, string, []byte) ([]byte, error) { time.Sleep(time.Second); return nil, nil })
	_, err = a.Request(tctx, b.Addr(), VerbReadRow, nil)
	require.ErrorIs(t, err, ErrTimeout)
}

func fastConfig() Config {
	c := DefaultConfig()
	c.ProbeInterval, c.ProbeTimeout = 50*time.Millisecond, 25*time.Millisecond
	c.GossipInterval, c.PushPullInterval = 20*time.Millisecond, 200*time.Millisecond
	c.MinSuspicionTimeout = 150 * time.Millisecond
	return c
}

func startMembers(t *testing.T, n int) ([]*Messaging, []*Gossiper) {
	var ms []*Messaging
	var gs []*Gossiper
	for i := 0; i < n; i++ {
		m, err := NewMessaging("127.0.0.1:0")
		require.NoError(t, err)
		ms = append(ms, m)
	}
	for i, m := range ms {
		g := NewGossiper(m, EndpointState{HostID: string(rune('a' + i))}, []string{ms[0].Addr()})
		g.SetConfig(fastConfig())
		m.Start()
		gs = append(gs, g)
	}
	for _, g := range gs {
		g.Start()
	}
	return ms, gs
}

// TestMembership checks SWIM membership: joining through a seed, failure
// detection through probes and suspicion, and a restarted node coming
// back with a new generation.
func TestMembership(t *testing.T) {
	ms, gs := startMembers(t, 4)
	require.Eventually(t, func() bool {
		for _, g := range gs {
			if len(g.States()) != 4 {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	// A state change spreads.
	gs[1].Update(func(s *EndpointState) { s.Epoch = 7 })
	require.Eventually(t, func() bool {
		for _, s := range gs[3].States() {
			if s.Addr == ms[1].Addr() && s.Epoch == 7 {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	// A stopped node is suspected, then declared dead by everyone.
	addr := ms[3].Addr()
	gs[3].Stop()
	ms[3].Close()
	require.Eventually(t, func() bool {
		return !gs[0].IsAlive(addr) && !gs[1].IsAlive(addr) && !gs[2].IsAlive(addr)
	}, 5*time.Second, 10*time.Millisecond)
	// It restarts on the same address: a new generation is alive again.
	m, err := NewMessaging(addr)
	require.NoError(t, err)
	g := NewGossiper(m, EndpointState{HostID: "d"}, []string{ms[0].Addr()})
	g.SetConfig(fastConfig())
	m.Start()
	g.Start()
	require.Eventually(t, func() bool { return gs[0].IsAlive(addr) && gs[2].IsAlive(addr) }, 5*time.Second, 10*time.Millisecond)
	for _, x := range append(gs[:3], g) {
		x.Stop()
	}
	for _, x := range append(ms[:3], m) {
		x.Close()
	}
}

// TestRefute checks that a node told it is suspect refutes it.
func TestRefute(t *testing.T) {
	ms, gs := startMembers(t, 3)
	require.Eventually(t, func() bool { return len(gs[2].States()) == 3 }, 5*time.Second, 10*time.Millisecond)
	gs[0].suspect(message{Kind: stateSuspect, Node: ms[1].Addr(), From: "test"})
	// Node 1 hears about it and raises its incarnation: alive everywhere.
	require.Eventually(t, func() bool {
		gs[0].mu.Lock()
		defer gs[0].mu.Unlock()
		m := gs[0].members[ms[1].Addr()]
		return m.Liveness == stateAlive && m.Incarnation > 0
	}, 5*time.Second, 10*time.Millisecond)
	for _, g := range gs {
		g.Stop()
	}
	for _, m := range ms {
		m.Close()
	}
}

func TestSuspicionTimeout(t *testing.T) {
	fired := make(chan time.Time, 1)
	start := time.Now()
	s := newSuspicion("a", 3, 100*time.Millisecond, 1000*time.Millisecond, func() { fired <- time.Now() })
	for _, from := range []string{"b", "c", "d"} {
		require.True(t, s.confirm(from))
	}
	require.False(t, s.confirm("e"), "k confirmations at most")
	at := <-fired
	require.Less(t, at.Sub(start), 300*time.Millisecond, "k confirmations bring the timeout to min")
}

// memStore is a Store for tests: the metadata is a list of items, each
// entry adds one (and may set the members).
type memStore struct {
	mu      sync.Mutex
	meta    memMeta
	log     []Entry
	members []string
}

type memMeta struct {
	Epoch   int64
	Items   []string
	IDs     []string
	Members []string `json:",omitempty"`
}

type memEntry struct {
	Item, ID string
	Members  []string `json:",omitempty"`
}

func (s *memStore) Current() (int64, []byte, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(s.meta)
	members := s.meta.Members
	if len(members) == 0 {
		members = s.members
	}
	return s.meta.Epoch, b, members
}

func (s *memStore) Contains(meta []byte, id string) (bool, error) {
	var m memMeta
	err := json.Unmarshal(meta, &m)
	return slices.Contains(m.IDs, id), err
}

func (s *memStore) Append(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case e.Epoch <= s.meta.Epoch:
		return nil
	case e.Epoch > s.meta.Epoch+1:
		return ErrGap
	}
	var x memEntry
	if err := json.Unmarshal(e.Value, &x); err != nil {
		return err
	}
	s.meta.Epoch = e.Epoch
	s.meta.Items = append(s.meta.Items, x.Item)
	s.meta.IDs = append(s.meta.IDs, x.ID)
	if x.Members != nil {
		s.meta.Members = x.Members
	}
	s.log = append(s.log, e)
	return nil
}

func (s *memStore) Since(epoch int64) ([]Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch >= s.meta.Epoch {
		return nil, true
	}
	for i, e := range s.log {
		if e.Epoch == epoch+1 {
			return slices.Clone(s.log[i:]), true
		}
	}
	return nil, false
}

func (s *memStore) Snapshot(b []byte) error {
	var m memMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.Epoch > s.meta.Epoch {
		s.meta, s.log = m, nil
	}
	return nil
}

func (s *memStore) items() (int64, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.Epoch, slices.Clone(s.meta.Items)
}

func addItem(item string, members []string) func([]byte, string) ([]byte, error) {
	return func(_ []byte, id string) ([]byte, error) {
		return json.Marshal(memEntry{Item: item, ID: id, Members: members})
	}
}

func startCMS(t *testing.T, n int) ([]*Messaging, []*memStore, []*CMS) {
	var ms []*Messaging
	var members []string
	for i := 0; i < n; i++ {
		m, err := NewMessaging("127.0.0.1:0")
		require.NoError(t, err)
		ms = append(ms, m)
		members = append(members, m.Addr())
	}
	var stores []*memStore
	var cmss []*CMS
	for _, m := range ms {
		s := &memStore{members: members}
		stores = append(stores, s)
		cmss = append(cmss, NewCMS(m, s))
		m.Start()
	}
	t.Cleanup(func() {
		for _, m := range ms {
			m.Close()
		}
	})
	return ms, stores, cmss
}

// TestCMS commits concurrent changes from every node: each epoch holds one
// change, none is lost, and every node ends with the same metadata.
func TestCMS(t *testing.T) {
	_, stores, cmss := startCMS(t, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cmss[i].Commit(context.Background(), addItem(fmt.Sprintf("%d-%d", i, j), nil))
				require.NoError(t, err)
			}()
		}
	}
	wg.Wait()
	require.Eventually(t, func() bool {
		_, want := stores[0].items()
		for i := range stores {
			e, items := stores[i].items()
			if e != 30 || len(items) != 30 || !slices.Equal(items, want) {
				t.Logf("node %d: epoch %d items %d", i, e, len(items))
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

// TestCMSCatchUp checks that a node that missed entries replays them from
// a peer's log, and falls back to a snapshot when the log does not reach.
func TestCMSCatchUp(t *testing.T) {
	_, stores, cmss := startCMS(t, 3)
	for i := 0; i < 5; i++ {
		_, err := cmss[0].Commit(context.Background(), addItem(fmt.Sprint(i), nil))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { e, _ := stores[2].items(); return e == 5 }, 5*time.Second, 10*time.Millisecond)

	// A node that lost its state replays the log from epoch 0.
	late := &memStore{members: stores[0].members}
	m, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Close()
	c := NewCMS(m, late)
	m.Start()
	require.NoError(t, c.CatchUp(cmss[1].self))
	e, items := late.items()
	require.Equal(t, int64(5), e)
	require.Equal(t, []string{"0", "1", "2", "3", "4"}, items)
	require.Len(t, late.log, 5, "replayed entry by entry")

	// When the peer's log no longer reaches back, a snapshot.
	stores[1].mu.Lock()
	stores[1].log = stores[1].log[3:]
	stores[1].mu.Unlock()
	fresh := &memStore{members: stores[0].members}
	m2, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	defer m2.Close()
	c2 := NewCMS(m2, fresh)
	m2.Start()
	require.NoError(t, c2.CatchUp(cmss[1].self))
	e, items = fresh.items()
	require.Equal(t, int64(5), e)
	require.Len(t, items, 5)
	require.Empty(t, fresh.log)

	// Sync makes a node catch up from the asking one.
	_, err = cmss[0].Commit(context.Background(), addItem("5", nil))
	require.NoError(t, err)
	got, err := cmss[0].Sync(context.Background(), m.Addr())
	require.NoError(t, err)
	require.Equal(t, int64(6), got)
}

// TestCMSMembersFollowEpochs commits while the members change: every epoch
// is decided by the members recorded at the epoch before, so nodes never
// disagree on an epoch.
func TestCMSMembersFollowEpochs(t *testing.T) {
	ms, stores, cmss := startCMS(t, 5)
	addrs := func(idx ...int) []string {
		var out []string
		for _, i := range idx {
			out = append(out, ms[i].Addr())
		}
		return out
	}
	// Start with members 0-2.
	_, err := cmss[0].Commit(context.Background(), addItem("init", addrs(0, 1, 2)))
	require.NoError(t, err)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := range cmss {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := cmss[i].Commit(ctx, addItem(fmt.Sprintf("%d-%d", i, j), nil))
				cancel()
				require.NoError(t, err)
			}
		}()
	}
	// Move the members one at a time to 2-4, and back.
	for _, set := range [][]int{{0, 1, 2, 3}, {1, 2, 3}, {1, 2, 3, 4}, {2, 3, 4}, {0, 2, 3, 4}, {0, 1, 2}} {
		_, err := cmss[2].Commit(context.Background(), addItem(fmt.Sprint(set), addrs(set...)))
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	for _, c := range cmss {
		for _, mb := range ms {
			_ = c.CatchUp(mb.Addr())
		}
	}
	_, want := stores[0].items()
	for i := range stores {
		_, items := stores[i].items()
		require.Equal(t, want, items, "node %d", i)
	}
}

func TestNetworkTopologyStrategy(t *testing.T) {
	var states []Node
	for i, spec := range []struct{ dc, rack string }{{"dc1", "r1"}, {"dc1", "r1"}, {"dc1", "r2"}, {"dc2", "r1"}, {"dc2", "r1"}} {
		states = append(states, Node{Addr: string(rune('a' + i)), DataCenter: spec.dc, Rack: spec.rack,
			Tokens: RandomTokens(16), Status: StatusNormal})
	}
	r := NewRing(states)
	repl := map[string]string{"class": "NetworkTopologyStrategy", "dc1": "2", "dc2": "1"}
	for i := 0; i < 100; i++ {
		reps := r.Replicas(repl, int64(i)*7919*1e12)
		require.Len(t, reps, 3)
		dcs, racks := map[string]int{}, map[string]bool{}
		for _, n := range reps {
			dcs[n.DataCenter]++
			if n.DataCenter == "dc1" {
				racks[n.Rack] = true
			}
		}
		require.Equal(t, map[string]int{"dc1": 2, "dc2": 1}, dcs)
		require.Len(t, racks, 2, "dc1's replicas are on both racks")
	}
}

func TestRingReplicas(t *testing.T) {
	var nodes []Node
	for i := 0; i < 3; i++ {
		nodes = append(nodes, Node{Addr: string(rune('a' + i)), Tokens: RandomTokens(8), Status: StatusNormal})
	}
	nodes = append(nodes, Node{Addr: "joining", Tokens: RandomTokens(8), Status: StatusJoining})
	r := NewRing(nodes)
	require.Len(t, r.Nodes(), 3, "joining nodes are not in the current ring")
	require.Len(t, NewFutureRing(nodes).Nodes(), 4)
	repl := map[string]string{"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "2"}
	for _, tok := range []int64{-1 << 63, -5, 0, 1 << 62} {
		reps := r.Replicas(repl, tok)
		require.Len(t, reps, 2)
		require.NotEqual(t, reps[0].Addr, reps[1].Addr)
	}
}

// TestCMSPersistence checks that a member that restarts still refuses a
// ballot older than one it promised before.
func TestCMSPersistence(t *testing.T) {
	m, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Close()
	var saved []byte
	c := NewCMS(m, &memStore{members: []string{m.Addr()}})
	require.NoError(t, c.SetPersistence(nil, func(b []byte) error { saved = b; return nil }))
	prepare := func(c *CMS, round int64) bool {
		b, _ := json.Marshal(paxosReq{Epoch: 1, Ballot: ballot{Round: round, Node: "x"}})
		out, err := c.handlePrepare(context.Background(), "", b)
		require.NoError(t, err)
		var r paxosResp
		require.NoError(t, json.Unmarshal(out, &r))
		return r.OK
	}
	require.True(t, prepare(c, 10))
	require.NotEmpty(t, saved)
	restarted := NewCMS(m, &memStore{members: []string{m.Addr()}})
	require.NoError(t, restarted.SetPersistence(saved, func(b []byte) error { saved = b; return nil }))
	require.False(t, prepare(restarted, 5), "a lower ballot than the one promised before the restart")
	require.True(t, prepare(restarted, 11))
}

// TestCMSLaggingAcceptor: a member that misses epochs before the one being
// decided does not vote on it (it catches up instead); and catching up
// asks members until the epoch advances, not just until one answers.
func TestCMSLaggingAcceptor(t *testing.T) {
	_, stores, cmss := startCMS(t, 3)
	for i := 0; i < 3; i++ {
		_, err := cmss[0].Commit(context.Background(), addItem(fmt.Sprint(i), nil))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { e, _ := stores[2].items(); return e == 3 }, 5*time.Second, 10*time.Millisecond)
	// Node 2 loses its state (epoch 0).
	stores[2].mu.Lock()
	stores[2].meta = memMeta{Members: stores[2].members}
	stores[2].log = nil
	stores[2].mu.Unlock()
	req, _ := json.Marshal(paxosReq{Epoch: 4, Ballot: ballot{Round: 1, Node: cmss[0].self}})
	out, err := cmss[2].handlePrepare(context.Background(), cmss[0].self, req)
	require.NoError(t, err)
	var r paxosResp
	require.NoError(t, json.Unmarshal(out, &r))
	require.True(t, r.Lagging, "no vote on epoch 4 from a node at epoch 0")
	require.False(t, r.OK)
	require.Eventually(t, func() bool { e, _ := stores[2].items(); return e == 3 }, 5*time.Second, 10*time.Millisecond, "it caught up")

	// Node 1 falls behind node 0; node 2 is as far behind as node 1.
	_, err = cmss[0].Commit(context.Background(), addItem("x", nil))
	require.NoError(t, err)
	stores[1].mu.Lock()
	stores[1].meta.Epoch, stores[1].log = 3, stores[1].log[:3]
	stores[1].meta.Items, stores[1].meta.IDs = stores[1].meta.Items[:3], stores[1].meta.IDs[:3]
	stores[1].mu.Unlock()
	stores[2].mu.Lock()
	stores[2].meta.Epoch, stores[2].log = 3, stores[2].log[:3]
	stores[2].meta.Items, stores[2].meta.IDs = stores[2].meta.Items[:3], stores[2].meta.IDs[:3]
	stores[2].mu.Unlock()
	// Node 1 asks node 2 first (no further), then node 0.
	cmss[1].catchUpFromMembers([]string{cmss[2].self, cmss[0].self})
	e, _ := stores[1].items()
	require.EqualValues(t, 4, e, "caught up past a member as far behind")
}

// TestMergeDeadAndStaleState: a member another node reports dead, unknown
// here, is not taken for up; and a dead member's newer state, relayed late,
// does not bring it back (only a newer generation or incarnation does).
func TestMergeDeadAndStaleState(t *testing.T) {
	m, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Close()
	g := NewGossiper(m, EndpointState{}, nil)
	dead := EndpointState{Addr: "10.0.0.9:7000", Generation: 5, Epoch: 1}
	b, _ := json.Marshal([]pushPullState{{State: dead, Liveness: stateDead, Incarnation: 2}})
	g.mergeState(b)
	require.False(t, g.IsAlive(dead.Addr), "reported dead")

	later := dead
	later.Epoch = 2
	g.alive(message{Kind: kindState, Node: dead.Addr, Gen: later.Generation, Inc: 2, State: &later})
	require.False(t, g.IsAlive(dead.Addr), "a newer state of the same life does not revive it")

	g.alive(message{Kind: stateAlive, Node: dead.Addr, Gen: dead.Generation, Inc: 3})
	require.True(t, g.IsAlive(dead.Addr), "it refuted with a higher incarnation")
}

// TestNetworkTopologyStrategyIdleRack: a rack whose only node owns no
// tokens does not keep the skipped nodes of the other racks out: every
// token still gets its data center's replication factor.
func TestNetworkTopologyStrategyIdleRack(t *testing.T) {
	r := NewRing([]Node{
		{Addr: "a", DataCenter: "dc1", Rack: "r1", Tokens: RandomTokens(8), Status: StatusNormal},
		{Addr: "b", DataCenter: "dc1", Rack: "r1", Tokens: RandomTokens(8), Status: StatusNormal},
		{Addr: "c", DataCenter: "dc1", Rack: "r2", Status: StatusNormal},
	})
	repl := map[string]string{"class": "NetworkTopologyStrategy", "dc1": "2"}
	for i := 0; i < 50; i++ {
		require.Len(t, r.Replicas(repl, int64(i)*7919*1e12), 2)
	}
}
