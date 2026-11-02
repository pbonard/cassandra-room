package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMessaging(t *testing.T) {
	a, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	b, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	b.Handle(VerbMutation, func(from string, p []byte) ([]byte, error) {
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
	b.Handle(VerbReadRow, func(string, []byte) ([]byte, error) { time.Sleep(time.Second); return nil, nil })
	_, err = a.Request(tctx, b.Addr(), VerbReadRow, nil)
	require.ErrorIs(t, err, ErrTimeout)
}

func TestGossipAndRing(t *testing.T) {
	var ms []*Messaging
	var gs []*Gossiper
	for i := 0; i < 3; i++ {
		m, err := NewMessaging("127.0.0.1:0")
		require.NoError(t, err)
		ms = append(ms, m)
	}
	seeds := []string{ms[0].Addr()}
	for i, m := range ms {
		g := NewGossiper(m, EndpointState{HostID: string(rune('a' + i)), DataCenter: "dc1", Rack: "r1",
			Tokens: RandomTokens(8), Status: StatusNormal}, seeds)
		g.SetTiming(50*time.Millisecond, 300*time.Millisecond)
		m.Start()
		gs = append(gs, g)
	}
	for _, g := range gs {
		g.Start()
	}
	require.Eventually(t, func() bool {
		for _, g := range gs {
			if len(g.States()) != 3 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	r := NewRing(gs[0].States())
	repl := map[string]string{"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "2"}
	for _, tok := range []int64{-1 << 63, -5, 0, 1 << 62} {
		reps := r.Replicas(repl, tok)
		require.Len(t, reps, 2)
		require.NotEqual(t, reps[0].Addr, reps[1].Addr)
	}
	// A stopped node is detected as down.
	gs[2].Stop()
	ms[2].Close()
	require.Eventually(t, func() bool { return !gs[0].IsAlive(ms[2].Addr()) }, 5*time.Second, 20*time.Millisecond)
	for _, g := range gs[:2] {
		g.Stop()
	}
	for _, m := range ms[:2] {
		m.Close()
	}
}

func TestNetworkTopologyStrategy(t *testing.T) {
	var states []EndpointState
	for i, spec := range []struct{ dc, rack string }{{"dc1", "r1"}, {"dc1", "r1"}, {"dc1", "r2"}, {"dc2", "r1"}, {"dc2", "r1"}} {
		states = append(states, EndpointState{Addr: string(rune('a' + i)), DataCenter: spec.dc, Rack: spec.rack,
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
