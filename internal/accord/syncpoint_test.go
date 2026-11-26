package accord

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (c *simCluster) setRanges(durable bool, interval time.Duration) {
	split := func(r Range) []RangeShard {
		var out []RangeShard
		for tok := max(r.Lo, 0); tok <= min(r.Hi, int64(len(c.keys)-1)); tok++ {
			out = append(out, RangeShard{Range: Range{Prefix: r.Prefix, Lo: tok, Hi: tok}, Nodes: c.replicas(Key{Token: tok})})
		}
		return out
	}
	for _, id := range c.ids {
		n := c.net.nodes[id]
		n.SetTopology(&Topology{Epoch: 1, Replicas: c.replicas, Nodes: c.ids, RangeReplicas: split})
		if durable {
			n.cfg.DurabilityRanges = func() []Range { return []Range{{Lo: math.MinInt64, Hi: math.MaxInt64}} }
			n.cfg.DurabilityInterval = interval
		}
	}
}

// TestSyncPoint: a sync point follows every transaction with a lower id;
// once applied everywhere, those are all applied or invalidated, and a
// new transaction with a lower id is refused.
func TestSyncPoint(t *testing.T) {
	c := newSimCluster(t, 5, 3, 6)
	c.setRanges(false, 0)
	c.net.drop = 0.02
	done := make(chan []*opRecord)
	go func() { done <- c.run(6, 150, nil) }()
	time.Sleep(150 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := c.node(1).SyncPoint(ctx, Range{Lo: math.MinInt64, Hi: math.MaxInt64}, true)
	require.NoError(t, err)
	for _, nid := range c.ids {
		n := c.net.nodes[nid]
		n.s.mu.Lock()
		sp := n.s.cmds[id]
		require.NotNil(t, sp, nid)
		require.Equal(t, Applied, sp.Status, nid)
		for _, ids := range sp.Deps {
			for _, d := range ids {
				if dc := n.s.cmds[d]; dc != nil && containsAny(n, dc.Keys) {
					require.Contains(t, []Status{Applied, Invalidated, Truncated}, dc.Status, "%s: dependency %v of the sync point", nid, d)
				}
			}
		}
		n.s.mu.Unlock()
	}
	records := <-done
	// A transaction with an id below the sync point is refused.
	old := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: id.HLC - 1, Node: "n0"}, Kind: Write}
	_, err = c.node(0).coordinate(context.Background(), old, appendTxn(t, c.keys[0], 9999))
	require.True(t, errors.Is(err, errStale) || errors.Is(err, errRefused), "%v", err)
	c.settle()
	c.check(records)
}

func containsAny(n *Node, keys []Key) bool { return len(n.owned(keys, 0)) > 0 }

// TestDurability runs durability rounds during a workload with message
// loss: the checks still hold, records below the redundancy bound are
// forgotten, and a late message about one of them is refused.
func TestDurability(t *testing.T) {
	c := newSimCluster(t, 5, 3, 6)
	c.setRanges(true, 300*time.Millisecond)
	c.net.drop = 0.02
	records := c.run(6, 200, nil)
	c.settle()
	c.check(records)
	var last string
	ok := assert.Eventually(t, func() bool {
		for _, id := range c.ids {
			n := c.net.nodes[id]
			st := n.Stats()
			if st.Redundant == 0 || st.Commands > 20 || st.Keys > 0 {
				last = fmt.Sprintf("%s: %d bounds, %d commands, %d keys; %d rounds, %d majority", id, st.Redundant, st.Commands, st.Keys, st.DurabilityRounds, st.MajorityRounds)
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "records and keys below the redundancy bound forgotten")
	if !ok {
		t.Fatal(last)
	}
	var first *opRecord
	for _, r := range records {
		if r.err == nil && len(r.appends) > 0 {
			first = r
			break
		}
	}
	require.NotNil(t, first)
	// A late PreAccept of an old transaction is refused, and its recovery
	// finds it durable.
	n0 := c.node(0)
	old := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: first.res.ExecuteAt.HLC, Node: first.res.ExecuteAt.Node}, Kind: Write}
	var k Key
	for ks := range first.appends {
		k, _ = parseKeyString(ks)
	}
	r, err := n0.s.preAccept(&preAcceptReq{ID: old, Txn: appendTxn(t, k, 1)})
	require.NoError(t, err)
	if containsAny(n0, []Key{k}) {
		require.Equal(t, stale, r.Outcome)
	}
	before := len(c.stores[c.replicas(k)[0]].data[keyString(k)])
	require.NoError(t, n0.recover(context.Background(), old, appendTxn(t, k, 1), []Key{k}))
	c.settle()
	require.Len(t, c.stores[c.replicas(k)[0]].data[keyString(k)], before)
}

// TestBootstrapSyncPoint: a node bootstrapped at a sync point does not wait
// for unknown transactions before it.
func TestBootstrapSyncPoint(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	c.setRanges(false, 0)
	_, err := c.node(0).Coordinate(context.Background(), appendTxn(t, c.keys[0], 1))
	require.NoError(t, err)
	require.NoError(t, c.node(2).Bootstrap(context.Background(), []Range{{Lo: math.MinInt64, Hi: math.MaxInt64}}))
	at := bootstrappedAt(c.node(2), c.keys[0])
	require.False(t, at.IsZero())
	// Every replica applied the transaction before the sync point.
	for _, id := range c.ids {
		require.Len(t, c.stores[id].data[keyString(c.keys[0])], 1)
	}
	// The bootstrap point survives a restart.
	c.crash(2)
	c.start(2)
	require.Equal(t, at, bootstrappedAt(c.node(2), c.keys[0]))
}

// bootstrappedAt is a node's bootstrap point over a key (zero if none).
func bootstrappedAt(n *Node, k Key) Timestamp {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()
	return boundFor(n.s.bootstrapped, k)
}

// TestBootstrapPerRange: bootstrap points are per range. A node
// bootstrapping a range it receives later keeps waiting for unknown
// transactions on the ranges it already replicated (a single node-wide
// point let it skip them there); a node taking over ranges from a leaving
// one learns their points from the node that took them and streamed the
// data (SendBootstrapped); the points survive a restart.
func TestBootstrapPerRange(t *testing.T) {
	c := newSimCluster(t, 3, 3, 2)
	c.setRanges(false, 0)
	ka, kb := c.keys[0], c.keys[1]
	ra, rb := Range{Lo: ka.Token, Hi: ka.Token}, Range{Lo: kb.Token, Hi: kb.Token}
	ctx := context.Background()
	_, err := c.node(0).Coordinate(ctx, appendTxn(t, ka, 1))
	require.NoError(t, err)
	require.NoError(t, c.node(2).Bootstrap(ctx, []Range{ra}))
	atA := bootstrappedAt(c.node(2), ka)
	require.False(t, atA.IsZero())
	require.True(t, bootstrappedAt(c.node(2), kb).IsZero(), "the other range is not bootstrapped")

	// Later, n1 takes range b's point and streams it to n2.
	_, err = c.node(0).Coordinate(ctx, appendTxn(t, kb, 2))
	require.NoError(t, err)
	pts, err := c.node(1).BootstrapPoints(ctx, []Range{ra, rb})
	require.NoError(t, err)
	require.Len(t, pts.For([]Range{rb}), 1)
	require.NoError(t, c.node(1).SendBootstrapped(ctx, "n2", pts.For([]Range{rb})))
	atB := bootstrappedAt(c.node(2), kb)
	require.False(t, atB.IsZero())
	require.True(t, atA.Less(atB))
	require.Equal(t, atA, bootstrappedAt(c.node(2), ka), "range a keeps its own point")

	c.crash(2)
	c.start(2)
	require.Equal(t, atA, bootstrappedAt(c.node(2), ka))
	require.Equal(t, atB, bootstrappedAt(c.node(2), kb))
}

// TestMajorityDurability: with a replica down, durability rounds fall back
// to sync points applied at a majority, which shrink applied records.
func TestMajorityDurability(t *testing.T) {
	c := newSimCluster(t, 3, 3, 3)
	c.setRanges(true, 200*time.Millisecond)
	c.crash(2)
	records := c.run(2, 30, nil)
	for _, r := range records {
		require.NoError(t, r.err)
	}
	require.Eventually(t, func() bool {
		n := c.node(0)
		if n.Stats().MajorityRounds == 0 {
			return false
		}
		n.s.mu.Lock()
		defer n.s.mu.Unlock()
		for _, cmd := range n.s.cmds {
			if cmd.Status == Applied && cmd.ID.Kind != SyncPoint && cmd.LocalDeps != nil {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond, "applied records below the majority bound compacted")
	// Back up, the node catches up through the next universal round,
	// whose sync point waits for (and fetches) what it missed.
	rounds := c.node(0).Stats().DurabilityRounds
	c.start(2)
	require.Eventually(t, func() bool { return c.node(0).Stats().DurabilityRounds > rounds }, 60*time.Second, 100*time.Millisecond)
	c.settle()
	c.check(records)
}

// TestBootstrapSkipsOnlyItsRanges: an unknown dependency older than a
// bootstrap point is skipped on the bootstrapped range (its effects are in
// the streamed data), and waited for on another range (this node knew that
// range's transactions all along: a single node-wide point skipped it, and
// the command could execute before a transaction it depends on).
func TestBootstrapSkipsOnlyItsRanges(t *testing.T) {
	c := newSimCluster(t, 3, 3, 2)
	c.setRanges(false, 0)
	ka, kb := c.keys[0], c.keys[1]
	n := c.node(2)
	ctx := context.Background()
	unknown := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n.clock.Next(0), Node: "n0"}, Kind: Write}
	require.NoError(t, n.Bootstrap(ctx, []Range{{Lo: ka.Token, Hi: ka.Token}}))
	require.True(t, unknown.Timestamp.Less(bootstrappedAt(n, ka)))

	for _, tc := range []struct {
		k    Key
		wait bool
	}{{ka, false}, {kb, true}} {
		n.s.mu.Lock()
		id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n.clock.Next(0), Node: "n1"}, Kind: Write}
		cmd := n.s.get(id)
		cmd.Txn = appendTxn(t, tc.k, 9)
		n.s.registerLocked(cmd, cmd.Txn.Keys)
		cmd.Status, cmd.ExecuteAt = Stable, id.Timestamp
		cmd.Deps = Deps{keyString(tc.k): {unknown}}
		n.s.maybeExecuteLocked(cmd)
		ready, blocked := cmd.ready, cmd.blockedOn
		n.s.mu.Unlock()
		if tc.wait {
			require.False(t, ready, "key %s: not bootstrapped, waits", tc.k.ID)
			require.NotNil(t, blocked)
			require.Equal(t, unknown, *blocked)
		} else {
			require.True(t, ready, "key %s: bootstrapped, the dependency is in the data", tc.k.ID)
		}
	}
}
