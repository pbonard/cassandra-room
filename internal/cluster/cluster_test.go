package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// TestCMS commits concurrent changes from every node: each epoch holds one
// change, none is lost, and every node ends with the same metadata.
func TestCMS(t *testing.T) {
	type meta struct {
		Epoch int64
		Items []string
		IDs   []string
	}
	var ms []*Messaging
	for i := 0; i < 3; i++ {
		m, err := NewMessaging("127.0.0.1:0")
		require.NoError(t, err)
		ms = append(ms, m)
	}
	var members []string
	for _, m := range ms {
		members = append(members, m.Addr())
	}
	state := make([][]byte, 3)
	var mus [3]sync.Mutex
	var cmss []*CMS
	for i, m := range ms {
		state[i], _ = json.Marshal(meta{})
		cmss = append(cmss, NewCMS(m, func() []string { return members }, func() (int64, []byte) {
			mus[i].Lock()
			defer mus[i].Unlock()
			var x meta
			_ = json.Unmarshal(state[i], &x)
			return x.Epoch, state[i]
		}, func(b []byte) error {
			var x, cur meta
			if err := json.Unmarshal(b, &x); err != nil {
				return err
			}
			mus[i].Lock()
			defer mus[i].Unlock()
			_ = json.Unmarshal(state[i], &cur)
			if x.Epoch > cur.Epoch {
				state[i] = b
			}
			return nil
		}, func(b []byte, id string) (int64, bool, error) {
			var x meta
			err := json.Unmarshal(b, &x)
			for _, y := range x.IDs {
				if y == id {
					return x.Epoch, true, err
				}
			}
			return x.Epoch, false, err
		}))
		m.Start()
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cmss[i].Commit(context.Background(), func(cur []byte, id string) ([]byte, error) {
					var x meta
					_ = json.Unmarshal(cur, &x)
					x.Epoch++
					x.Items = append(x.Items, fmt.Sprintf("%d-%d", i, j))
					x.IDs = append(x.IDs, id)
					return json.Marshal(x)
				})
				require.NoError(t, err)
			}()
		}
	}
	wg.Wait()
	require.Eventually(t, func() bool {
		for i := range ms {
			_, b := cmss[i].current()
			var x meta
			_ = json.Unmarshal(b, &x)
			if x.Epoch != 30 || len(x.Items) != 30 {
				t.Logf("node %d: epoch %d items %d", i, x.Epoch, len(x.Items))
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	for _, m := range ms {
		m.Close()
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
	cur := func() (int64, []byte) { return 0, []byte("{}") }
	var saved []byte
	c := NewCMS(m, func() []string { return []string{m.Addr()} }, cur, func([]byte) error { return nil }, func([]byte, string) (int64, bool, error) { return 0, false, nil })
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
	restarted := NewCMS(m, func() []string { return []string{m.Addr()} }, cur, func([]byte) error { return nil }, func([]byte, string) (int64, bool, error) { return 0, false, nil })
	require.NoError(t, restarted.SetPersistence(saved, func(b []byte) error { saved = b; return nil }))
	require.False(t, prepare(restarted, 5), "a lower ballot than the one promised before the restart")
	require.True(t, prepare(restarted, 11))
}
