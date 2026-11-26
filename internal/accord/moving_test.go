package accord

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ranges moving between nodes: a node taking over a key is first a pending
// replica of it (it receives writes, its data is streaming to it), while it
// may still be a read replica of other keys of the same transactions.

// leaveTopology is epoch 2 of a 3-node, RF 2 sim cluster where n0 takes
// over k1 (replicas n1, n2) from n2, which leaves: n0 is a pending
// replica of k1, and still a read replica of k0 (replicas n0, n1).
func leaveTopology(c *simCluster) *Topology {
	k1 := c.keys[1]
	return &Topology{Epoch: 2, Replicas: c.replicas, Nodes: c.ids,
		Pending: func(k Key) []NodeID {
			if k == k1 {
				return []NodeID{"n0"}
			}
			return nil
		}}
}

// transferTxn appends to two keys.
func transferTxn(t *testing.T, a, b Key) *Txn {
	u, err := json.Marshal(listUpdate{Reads: []Key{a, b}, Appends: map[string]int{keyString(a): 1, keyString(b): 2}})
	require.NoError(t, err)
	return &Txn{Kind: Write, Keys: []Key{a, b}, Update: u}
}

func handle[R any](t *testing.T, ctx context.Context, n *Node, from NodeID, typ msgType, req any) R {
	b, err := json.Marshal(req)
	require.NoError(t, err)
	payload, err := json.Marshal(envelope{T: typ, B: b})
	require.NoError(t, err)
	out, err := n.Handle(ctx, from, payload)
	require.NoError(t, err)
	var r R
	require.NoError(t, json.Unmarshal(out, &r))
	return r
}

// TestApplyWaitsForEpoch: a replica behind the epoch a transaction
// executes at waits for that epoch (and asks for it) before applying it,
// instead of taking its keys from the older epoch it knows: there it did
// not own k1 yet, and once it learnt the newer epoch the transaction,
// already applied, gained k1 without its write ever applying.
func TestApplyWaitsForEpoch(t *testing.T) {
	c := newSimCluster(t, 3, 2, 2)
	k0, k1 := c.keys[0], c.keys[1]
	epoch2 := leaveTopology(c)
	c.node(1).SetTopology(epoch2)
	n0 := c.node(0)
	asked := make(chan int64, 1)
	n0.cfg.FetchEpoch = func(from NodeID, e int64) {
		require.Equal(t, NodeID("n1"), from)
		select {
		case asked <- e:
		default:
		}
		go func() {
			time.Sleep(50 * time.Millisecond) // catching up
			n0.SetTopology(epoch2)
		}()
	}
	txn := transferTxn(t, k0, k1)
	at := Timestamp{Epoch: 2, HLC: n0.clock.Next(0), Node: "n1"}
	w := Writes{keyString(k0): []byte("1"), keyString(k1): []byte("2")}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rep := handle[applyReply](t, ctx, n0, "n1", mApply, &applyReq{ID: TxnID{Timestamp: at, Kind: Write}, ExecuteAt: at, Deps: Deps{}, Txn: txn, Writes: w})
	require.Equal(t, ok, rep.Outcome)
	select {
	case e := <-asked:
		require.Equal(t, int64(2), e)
	default:
		t.Fatal("n0 did not ask for epoch 2")
	}
	st := c.stores["n0"]
	st.mu.Lock()
	defer st.mu.Unlock()
	require.Len(t, st.data[keyString(k0)], 1)
	require.Len(t, st.data[keyString(k1)], 1, "the write to the key n0 is pending on applied too")
	require.Zero(t, n0.Stats().LateOwned)
}

// TestReadSkipsPendingKeys: a replica asked to read for a transaction
// reads only the keys it is a read replica of. Keys it is only pending on
// have data still streaming to it (it answered them from what it had, and
// the coordinator then skipped their own shard), and an earlier
// transaction on them it does not know must not hold up the read of the
// others (it did until the request timed out).
func TestReadSkipsPendingKeys(t *testing.T) {
	c := newSimCluster(t, 3, 2, 2)
	k0, k1 := c.keys[0], c.keys[1]
	n0 := c.node(0)
	n0.SetTopology(leaveTopology(c))
	unknown := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n0.clock.Next(0), Node: "n2"}, Kind: Write}
	at := Timestamp{Epoch: 2, HLC: n0.clock.Next(0), Node: "n1"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	rep := handle[commitReply](t, ctx, n0, "n1", mCommit, &commitReq{ID: TxnID{Timestamp: at, Kind: Write}, ExecuteAt: at,
		Deps: Deps{keyString(k1): {unknown}}, Txn: transferTxn(t, k0, k1), Keys: []Key{k0, k1}, Stable: true, Read: true})
	require.Equal(t, ok, rep.Outcome)
	require.Less(t, time.Since(start), time.Second)
	require.Contains(t, rep.Reads, keyString(k0))
	require.NotContains(t, rep.Reads, keyString(k1), "only pending on k1")
}

// TestChangedRangesPending: a range whose pending replicas change (a node
// starts joining or leaving) changed, though its read replicas did not:
// the epoch is synchronised with a sync point, so that transactions of the
// epoch before, which do not reach the pending replicas, are done before
// data is streamed to them.
func TestChangedRangesPending(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	n := c.node(0)
	n.cfg.EpochRanges = func() []Range { return []Range{{Lo: math.MinInt64, Hi: math.MaxInt64}} }
	shards := func(pending ...NodeID) func(Range) []RangeShard {
		return func(r Range) []RangeShard {
			return []RangeShard{{Range: r, Nodes: []NodeID{"n0", "n1"}, Pending: pending}}
		}
	}
	e1 := &Topology{Epoch: 1, RangeReplicas: shards()}
	e2 := &Topology{Epoch: 2, RangeReplicas: shards("n2")}
	require.Len(t, n.changedRanges(e1, e2), 1)
	require.Empty(t, n.changedRanges(e2, &Topology{Epoch: 3, RangeReplicas: shards("n2")}))
}

// TestBootstrappedWakes: a command waiting on a dependency it does not
// know executes as soon as the node learns a bootstrap point above the
// dependency (its effects are in the data the node received), not at the
// next topology change.
func TestBootstrappedWakes(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n := c.node(2)
	unknown := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n.clock.Next(0), Node: "n0"}, Kind: Write}
	n.s.mu.Lock()
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n.clock.Next(0), Node: "n1"}, Kind: Write}
	cmd := n.s.get(id)
	cmd.Txn = appendTxn(t, k, 7)
	n.s.registerLocked(cmd, cmd.Txn.Keys)
	cmd.Status, cmd.ExecuteAt = PreApplied, id.Timestamp
	cmd.Deps = Deps{keyString(k): {unknown}}
	cmd.Writes = Writes{keyString(k): []byte("7")}
	n.s.maybeExecuteLocked(cmd)
	require.False(t, cmd.ready)
	n.s.mu.Unlock()

	point := Timestamp{Epoch: 1, HLC: n.clock.Next(0), Node: "n0"}
	require.NoError(t, n.s.setBootstrapped(BootstrapPoints{{R: Range{Lo: k.Token, Hi: k.Token}, TS: point}}))
	require.Eventually(t, func() bool {
		st, _ := c.status(2, id)
		return st == Applied
	}, 200*time.Millisecond, 10*time.Millisecond, "well before the progress log gives up waiting (RecoverAfter)")
}

// TestLateOwned: a command applied here that gains a key it owned at the
// epoch it executed at is counted (its write there was lost); one gaining
// a key from a later epoch (a range this node starts receiving, whose data
// holds the write) is not.
func TestLateOwned(t *testing.T) {
	for _, tc := range []struct {
		epoch int64
		late  bool
	}{{1, false}, {2, true}} {
		c := newSimCluster(t, 3, 2, 2)
		k0, k1 := c.keys[0], c.keys[1]
		n := c.node(0)
		n.s.mu.Lock()
		at := Timestamp{Epoch: tc.epoch, HLC: n.clock.Next(0), Node: "n1"}
		id := TxnID{Timestamp: at, Kind: Write}
		cmd := n.s.get(id)
		cmd.Txn = transferTxn(t, k0, k1)
		cmd.Keys = cmd.Txn.Keys
		cmd.owned = []Key{k0} // as when it applied, before epoch 2
		cmd.Status, cmd.ExecuteAt = Applied, at
		n.s.mu.Unlock()
		n.SetTopology(leaveTopology(c))
		n.s.mu.Lock()
		require.Equal(t, []Key{k0, k1}, cmd.owned)
		n.s.mu.Unlock()
		if tc.late {
			require.Equal(t, int64(1), n.Stats().LateOwned, "executed at epoch 2, when it owned k1")
		} else {
			require.Zero(t, n.Stats().LateOwned, "k1 is received with the data streamed")
		}
	}
}

// TestExecuteAtHLCDominates: a transaction's execution timestamp exceeds
// every conflicting one its replica witnessed in HLC too, not only in
// timestamp order (epoch first). The per-key maximum kept only the
// largest timestamp: having seen (epoch 1, HLC h+1s) and then (epoch 2,
// HLC h), it kept the latter, and a new transaction at (epoch 2, HLC
// h+500) took the fast path. It executed after the epoch 1 one but wrote
// its cells at a lower HLC, so they lost to it: an update lost across an
// epoch change.
func TestExecuteAtHLCDominates(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n := c.node(0)
	n.SetTopology(&Topology{Epoch: 2, Replicas: c.replicas, Nodes: c.ids})
	h := n.clock.Next(0)
	witness := func(ts Timestamp) {
		n.s.mu.Lock()
		defer n.s.mu.Unlock()
		cmd := n.s.get(TxnID{Timestamp: ts, Kind: Write})
		cmd.Txn = appendTxn(t, k, 1)
		n.s.registerLocked(cmd, cmd.Txn.Keys)
		n.s.witnessLocked(cmd, ts)
	}
	early := Timestamp{Epoch: 1, HLC: h + 1_000_000, Node: "n1"}
	witness(early)
	witness(Timestamp{Epoch: 2, HLC: h, Node: "n2"})
	id := TxnID{Timestamp: Timestamp{Epoch: 2, HLC: h + 500, Node: "n2"}, Kind: Write}
	rep, err := n.s.preAccept(&preAcceptReq{ID: id, Txn: appendTxn(t, k, 2)})
	require.NoError(t, err)
	require.False(t, rep.FastVote)
	require.Greater(t, rep.ExecuteAt.HLC, early.HLC)
	require.True(t, early.Less(rep.ExecuteAt))
}

// TestMergeMax: the coordinator's execution timestamp from several
// replicas' proposals is at least each in epoch and in HLC.
func TestMergeMax(t *testing.T) {
	a := Timestamp{Epoch: 1, HLC: 900, Node: "n1"}
	b := Timestamp{Epoch: 2, HLC: 100, Node: "n2"}
	m := mergeMax(a, b)
	require.Equal(t, Timestamp{Epoch: 2, HLC: 900, Node: "n1"}, m)
	require.False(t, m.Less(a))
	require.False(t, m.Less(b))
	require.Equal(t, m, mergeMax(b, a))
}
