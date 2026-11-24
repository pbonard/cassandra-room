package accord

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPrivilegedFastPath: a coordinator that replicates the key counts its
// own vote, so with one of three replicas down the fast path (2 of 3)
// still executes at the TxnID.
func TestPrivilegedFastPath(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	c.crash(2)
	out, err := c.node(0).Coordinate(context.Background(), appendTxn(t, k, 1))
	require.NoError(t, err)
	var res listResult
	require.NoError(t, json.Unmarshal(out, &res))
	n := c.node(0)
	n.s.mu.Lock()
	var id TxnID
	for cid := range n.s.cmds {
		id = cid
	}
	n.s.mu.Unlock()
	require.True(t, id.Privileged)
	require.Equal(t, id.Timestamp, res.ExecuteAt, "the fast path with 2 of 3 replicas")
}

// TestPrivilegedRecovery: a recovery that reaches the privileged
// coordinator before it decided invalidates the transaction, and the
// coordinator can then no longer take the fast path.
func TestPrivilegedRecovery(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n0 := c.node(0)
	txn := appendTxn(t, k, 1)
	id := n0.newTxnID(Write, txn.Keys...)
	require.True(t, id.Privileged)
	r, err := n0.s.preAccept(&preAcceptReq{ID: id, Txn: txn})
	require.NoError(t, err)
	require.True(t, r.FastVote)
	var r1 preAcceptReply
	require.NoError(t, n0.send(context.Background(), "n1", mPreAccept, &preAcceptReq{ID: id, Txn: txn}, &r1))
	require.True(t, r1.FastVote)
	// Before the coordinator decides, a recovery from n2 reaches it.
	require.NoError(t, c.node(2).recover(context.Background(), id, txn, txn.Keys))
	// If the recovery quorum included the coordinator, it invalidated the
	// transaction and the fast path is blocked; otherwise it assumed the
	// coordinator's vote and committed it at its TxnID, which the fast
	// path then agrees with. Either way every replica ends the same.
	fast := n0.s.fastCommit(id, txn, Deps{})
	want := Invalidated
	if fast {
		want = Applied
	}
	require.Eventually(t, func() bool {
		for i := 0; i < 3; i++ {
			if s, _ := c.status(i, id); s != want {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
}

// TestMediumPath: with RF 5 and two replicas down, a fast quorum (4) is
// impossible but a slow quorum votes for the TxnID: one Accept round
// commits it there.
func TestMediumPath(t *testing.T) {
	c := newSimCluster(t, 5, 5, 1)
	for _, id := range c.ids {
		c.net.nodes[id].cfg.NoPrivilegedCoordinator = true
	}
	c.crash(3)
	c.crash(4)
	out, err := c.node(0).Coordinate(context.Background(), appendTxn(t, c.keys[0], 1))
	require.NoError(t, err)
	var res listResult
	require.NoError(t, json.Unmarshal(out, &res))
	require.Equal(t, "n0", res.ExecuteAt.Node, "executed at its TxnID")
}

// TestForgetTombstones: transactions applied everywhere are truncated,
// then forgotten; a late PreAccept of a forgotten transaction is
// recovered and invalidated, never applied twice.
func TestForgetTombstones(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	for _, id := range c.ids {
		n := c.net.nodes[id]
		n.cfg.RetainApplied = 50 * time.Millisecond
		n.cfg.TombstoneRetention = 100 * time.Millisecond
	}
	txn := appendTxn(t, k, 1)
	_, err := c.node(0).Coordinate(context.Background(), txn)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for i := 0; i < 3; i++ {
			n := c.node(i)
			n.s.mu.Lock()
			left := len(n.s.cmds)
			n.s.mu.Unlock()
			if left > 0 {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond, "every record forgotten")
	// A late PreAccept of the same transaction.
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 1, Node: "n0"}, Kind: Write}
	n1 := c.node(1)
	r, err := n1.s.preAccept(&preAcceptReq{ID: id, Txn: txn})
	require.NoError(t, err)
	require.False(t, r.FastVote, "the key's witnessed timestamps survive")
	require.Eventually(t, func() bool { s, _ := c.status(1, id); return s == Invalidated }, 10*time.Second, 20*time.Millisecond)
	for _, id := range c.ids {
		require.Len(t, c.stores[id].data[keyString(k)], 1)
	}
}

// TestEpochSync: a node joins in epoch 2. Pending in epoch 1, it already
// receives writes; once every node has finished the transactions of epoch
// 1, the synchronised epoch advances and a coordinator still on epoch 1 is
// refused until it learns epoch 2.
func TestEpochSync(t *testing.T) {
	c := newSimCluster(t, 4, 3, 1)
	k := c.keys[0] // replicas n0, n1, n2
	pending := &Topology{Epoch: 1, Replicas: c.replicas, Nodes: c.ids, Pending: func(Key) []NodeID { return []NodeID{"n3"} }}
	for i := 0; i < 4; i++ {
		c.node(i).SetTopology(pending)
	}
	_, err := c.node(0).Coordinate(context.Background(), appendTxn(t, k, 1))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		c.stores["n3"].mu.Lock()
		defer c.stores["n3"].mu.Unlock()
		return len(c.stores["n3"].data[keyString(k)]) == 1
	}, 5*time.Second, 10*time.Millisecond, "the pending replica received the write")

	// Epoch synchronisation by sync points: n1 leads; epoch 2 moves the
	// key to n1, n2, n3; epoch 3 changes nothing.
	split := func(nodes ...NodeID) func(Range) []RangeShard {
		return func(r Range) []RangeShard { return []RangeShard{{Range: r, Nodes: nodes}} }
	}
	full := []Range{{Lo: math.MinInt64, Hi: math.MaxInt64}}
	for i := 0; i < 4; i++ {
		n := c.node(i)
		n.cfg.EpochRanges = func() []Range { return full }
		n.cfg.IsLeader = func() bool { return n.cfg.ID == "n1" }
		n.SetTopology(&Topology{Epoch: 1, Replicas: c.replicas, Nodes: c.ids, RangeReplicas: split("n0", "n1", "n2"),
			Pending: func(Key) []NodeID { return []NodeID{"n3"} }})
	}
	require.Eventually(t, func() bool { return c.node(3).synced.Load() >= 1 }, 10*time.Second, 20*time.Millisecond)
	epoch2 := &Topology{Epoch: 2, Replicas: func(Key) []NodeID { return []NodeID{"n1", "n2", "n3"} }, Nodes: c.ids, RangeReplicas: split("n1", "n2", "n3")}
	require.Len(t, c.node(1).changedRanges(c.node(1).topos[1], epoch2), 1)
	for i := 0; i < 4; i++ {
		c.node(i).SetTopology(epoch2)
	}
	require.Eventually(t, func() bool {
		for i := 0; i < 4; i++ {
			if c.node(i).synced.Load() < 2 {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond, "every node synchronised epoch 2")
	syncs := c.node(1).Stats().SyncPoints
	epoch3 := &Topology{Epoch: 3, Replicas: epoch2.Replicas, Nodes: c.ids, RangeReplicas: split("n3", "n2", "n1")}
	require.Empty(t, c.node(1).changedRanges(epoch2, epoch3), "the same replicas")
	for i := 0; i < 4; i++ {
		c.node(i).SetTopology(epoch3)
	}
	require.Eventually(t, func() bool { return c.node(0).synced.Load() >= 3 }, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, syncs, c.node(1).Stats().SyncPoints, "no sync point for an epoch without changes")
	// A transaction of epoch 1 (a coordinator that had not learnt epoch
	// 2) is refused (the sync point's barrier, or the synchronised epoch),
	// then recovered and invalidated.
	stale := appendTxn(t, k, 2)
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: c.node(0).clock.Next(0), Node: "n0"}, Kind: Write}
	_, err = c.node(0).coordinate(context.Background(), id, stale)
	require.True(t, errors.Is(err, errStale) || errors.Is(err, errRefused), "%v", err)
	require.NoError(t, c.node(0).recover(context.Background(), id, stale, stale.Keys))
	_, err = c.node(0).awaitOutcome(context.Background(), id, stale.Keys)
	require.ErrorIs(t, err, ErrInvalidated)
	_, err = c.node(0).Coordinate(context.Background(), appendTxn(t, k, 3))
	require.NoError(t, err)
	c.settle()
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		var vals []int
		for _, e := range c.stores[id].data[keyString(k)] {
			vals = append(vals, e.Value)
		}
		require.NotContains(t, vals, 2)
		require.Contains(t, vals, 3)
	}
}

func (s *listStore) ReadRange(r Range, _ *Txn) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]int{}
	for ks, l := range s.data {
		k, _ := parseKeyString(ks)
		if !r.contains(k) {
			continue
		}
		for _, e := range l {
			out[ks] = append(out[ks], e.Value)
		}
	}
	return json.Marshal(out)
}

// TestRangeReads runs range reads during appends (with message loss): each
// key a range read returns is a prefix of its history holding every append
// acknowledged before the read started.
func TestRangeReads(t *testing.T) {
	c := newSimCluster(t, 5, 3, 8)
	split := func(r Range) []RangeShard {
		var out []RangeShard
		for tok := max(r.Lo, 0); tok <= min(r.Hi, int64(len(c.keys)-1)); tok++ {
			out = append(out, RangeShard{Range: Range{Prefix: r.Prefix, Lo: tok, Hi: tok}, Nodes: c.replicas(Key{Token: tok})})
		}
		return out
	}
	for _, id := range c.ids {
		c.net.nodes[id].SetTopology(&Topology{Epoch: 1, Replicas: c.replicas, Nodes: c.ids, RangeReplicas: split})
	}
	c.net.drop = 0.02
	type rangeRead struct {
		start time.Time
		vals  map[string][]int
	}
	var reads []rangeRead
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			shards, out, err := c.node(int(start.UnixNano()%5)).ReadRange(ctx, Range{Lo: 0, Hi: 100}, &Txn{Kind: Read})
			cancel()
			if err != nil {
				continue
			}
			vals := map[string][]int{}
			for _, s := range shards {
				var m map[string][]int
				require.NoError(t, json.Unmarshal(out[s.Range.String()], &m))
				for k, v := range m {
					vals[k] = v
				}
			}
			reads = append(reads, rangeRead{start, vals})
		}
	}()
	records := c.run(6, 200, nil)
	close(stop)
	<-done
	c.settle()
	c.check(records)
	require.NotEmpty(t, reads)
	for _, rr := range reads {
		for ks, vals := range rr.vals {
			var hist []int
			for _, e := range c.stores[c.replicas(func() Key { k, _ := parseKeyString(ks); return k }())[0]].data[ks] {
				hist = append(hist, e.Value)
			}
			require.LessOrEqual(t, len(vals), len(hist))
			require.Equal(t, hist[:len(vals)], vals, "a range read of %s sees a prefix", ks)
		}
		for _, o := range records {
			if o.err == nil && o.end.Before(rr.start) {
				for ks, v := range o.appends {
					require.Contains(t, rr.vals[ks], v, "a range read sees the appends acknowledged before it")
				}
			}
		}
	}
	t.Logf("%d range reads", len(reads))
}

// TestRangeIndex checks that a range's dependencies come from the sorted
// key index: only the keys in the range, whatever the table prefix.
func TestRangeIndex(t *testing.T) {
	n, err := NewNode(Config{ID: "n0", Data: &listStore{data: map[string][]entry{}}, Agent: listAgent{}})
	require.NoError(t, err)
	n.SetTopology(&Topology{Epoch: 1, Replicas: func(Key) []NodeID { return []NodeID{"n0"} }})
	for tok := int64(-50); tok < 50; tok++ {
		for _, prefix := range []string{"a", "b"} {
			_, err := n.s.preAccept(&preAcceptReq{ID: n.newTxnID(Write), Txn: &Txn{Keys: []Key{{Token: tok, ID: prefix + "k"}}}})
			require.NoError(t, err)
		}
	}
	d := n.s.ephemeralDeps(nil, []Range{{Prefix: "a", Lo: -3, Hi: 4}}).Deps
	require.Len(t, d, 8)
	for ks := range d {
		k, ok := parseKeyString(ks)
		require.True(t, ok)
		require.True(t, k.Token >= -3 && k.Token <= 4 && k.ID == "ak", ks)
	}
	require.True(t, slices.IsSortedFunc(n.s.sorted, func(a, b *cfk) int { return a.key.Compare(b.key) }))
}

// TestPrivilegedRestrictions follows cassandra-accord fc4a769: no
// privileged coordinator for multi-key transactions, and no medium path
// with the privileged coordinator's quorums.
func TestPrivilegedRestrictions(t *testing.T) {
	c := newSimCluster(t, 5, 5, 2)
	n0 := c.node(0)
	require.True(t, n0.newTxnID(Write, c.keys[0]).Privileged)
	require.False(t, n0.newTxnID(Write, c.keys[0], c.keys[1]).Privileged, "multi-key: not privileged")
	// RF 5 with two replicas down: a privileged transaction cannot use the
	// medium path, so it takes the slow path.
	c.crash(3)
	c.crash(4)
	before := n0.Stats()
	_, err := n0.Coordinate(context.Background(), appendTxn(t, c.keys[0], 1))
	require.NoError(t, err)
	after := n0.Stats()
	require.Equal(t, before.MediumPath, after.MediumPath)
	require.Equal(t, before.SlowPath+1, after.SlowPath)
}
