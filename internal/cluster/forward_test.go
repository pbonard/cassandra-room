package cluster

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// forwardNodes starts n connected messaging nodes whose mutation handler
// answers with its own address, recording who it was sent by.
func forwardNodes(t *testing.T, n int) ([]*Messaging, *sync.Map) {
	var nodes []*Messaging
	froms := &sync.Map{}
	for range n {
		m, err := NewMessaging("127.0.0.1:0")
		require.NoError(t, err)
		m.Handle(VerbMutation, func(_ context.Context, from string, p []byte) ([]byte, error) {
			froms.Store(m.Addr(), from)
			if string(p) == "block" {
				time.Sleep(time.Second)
			}
			return []byte(m.Addr()), nil
		})
		m.Handle(VerbPing, func(context.Context, string, []byte) ([]byte, error) { return nil, nil })
		m.Start()
		t.Cleanup(func() { m.Close() })
		nodes = append(nodes, m)
	}
	// The coordinator learns everyone's messaging version.
	for _, o := range nodes[1:] {
		_, err := nodes[0].Request(context.Background(), o.Addr(), VerbPing, nil)
		require.NoError(t, err)
	}
	return nodes, froms
}

func collect(t *testing.T, ch <-chan Reply, n int) map[string]Reply {
	out := map[string]Reply{}
	for range n {
		select {
		case r := <-ch:
			out[r.From] = r
		case <-time.After(5 * time.Second):
			t.Fatal("a reply is missing")
		}
	}
	return out
}

// TestRequestForward: one request to a node, which delivers it to two
// others; all three answer the coordinator directly.
func TestRequestForward(t *testing.T) {
	nodes, froms := forwardNodes(t, 4)
	a, b, c, d := nodes[0], nodes[1], nodes[2], nodes[3]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	replies := collect(t, a.RequestForward(ctx, b.Addr(), []string{c.Addr(), d.Addr()}, VerbMutation, []byte("w")), 3)
	for _, n := range []*Messaging{b, c, d} {
		r := replies[n.Addr()]
		require.NoError(t, r.Err)
		require.Equal(t, n.Addr(), string(r.Payload))
		from, _ := froms.Load(n.Addr())
		require.Equal(t, a.Addr(), from, "forwarded requests come from the coordinator")
	}
	require.EqualValues(t, 1, a.Stats().Forwards)
	require.EqualValues(t, 2, b.Stats().Relayed)
	a.relayMu.Lock()
	require.Empty(t, a.relays)
	a.relayMu.Unlock()
}

// TestRequestForwardFailures: a forwarded node that cannot be reached is
// reported by the relaying node; one that does not answer times out; a
// relaying node that cannot be reached leaves the others sent directly.
func TestRequestForwardFailures(t *testing.T) {
	nodes, _ := forwardNodes(t, 3)
	a, b, c := nodes[0], nodes[1], nodes[2]
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gone := ln.Addr().String()
	ln.Close()
	a.mu.Lock()
	a.peerVer[gone] = ProtocolVersion
	a.mu.Unlock()

	// Unreachable forwarded node: an error well before the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	replies := collect(t, a.RequestForward(ctx, b.Addr(), []string{c.Addr(), gone}, VerbMutation, []byte("w")), 3)
	require.NoError(t, replies[b.Addr()].Err)
	require.NoError(t, replies[c.Addr()].Err)
	require.ErrorContains(t, replies[gone].Err, "forwarding failed")
	require.Less(t, time.Since(start), 3*time.Second)

	// A forwarded node too slow: a timeout.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	replies = collect(t, a.RequestForward(ctx2, b.Addr(), []string{c.Addr()}, VerbMutation, []byte("block")), 2)
	require.True(t, errors.Is(replies[c.Addr()].Err, ErrTimeout), "%v", replies[c.Addr()].Err)

	// Unreachable relaying node: the others get the request directly.
	replies = collect(t, a.RequestForward(context.Background(), gone, []string{b.Addr(), c.Addr()}, VerbMutation, []byte("w")), 3)
	require.Error(t, replies[gone].Err)
	require.NoError(t, replies[b.Addr()].Err)
	require.NoError(t, replies[c.Addr()].Err)

	a.relayMu.Lock()
	require.Empty(t, a.relays)
	a.relayMu.Unlock()
}

// TestRequestForwardOldPeer: towards a node without forwarding, every node
// is sent the request directly.
func TestRequestForwardOldPeer(t *testing.T) {
	nodes, _ := forwardNodes(t, 3)
	a, b, c := nodes[0], nodes[1], nodes[2]
	a.mu.Lock()
	a.peerVer[c.Addr()] = 4
	a.mu.Unlock()
	replies := collect(t, a.RequestForward(context.Background(), b.Addr(), []string{c.Addr()}, VerbMutation, []byte("w")), 2)
	require.NoError(t, replies[b.Addr()].Err)
	require.NoError(t, replies[c.Addr()].Err)
	require.Zero(t, a.Stats().Forwards)
	require.Zero(t, b.Stats().Relayed)
}
