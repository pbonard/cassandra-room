package accord

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

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
	require.Eventually(t, func() bool {
		for _, id := range c.ids {
			n := c.net.nodes[id]
			st := n.Stats()
			if st.Redundant == 0 || st.Commands > 20 {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "records below the redundancy bound forgotten")
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
	at := c.node(2).bootstrappedBefore()
	require.False(t, at.IsZero())
	// Every replica applied the transaction before the sync point.
	for _, id := range c.ids {
		require.Len(t, c.stores[id].data[keyString(c.keys[0])], 1)
	}
	// The bootstrap point survives a restart.
	c.crash(2)
	c.start(2)
	require.Equal(t, at, c.node(2).bootstrappedBefore())
}
