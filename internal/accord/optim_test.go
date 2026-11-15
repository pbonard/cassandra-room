package accord

import (
	"context"
	"encoding/json"
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

	epoch2 := &Topology{Epoch: 2, Replicas: func(Key) []NodeID { return []NodeID{"n1", "n2", "n3"} }, Nodes: c.ids}
	c.node(1).SetTopology(epoch2)
	c.node(1).syncEpochs(context.Background())
	require.Equal(t, int64(1), c.node(1).synced.Load(), "a node still on epoch 1 holds synchronisation back")
	for i := 0; i < 4; i++ {
		c.node(i).SetTopology(epoch2)
	}
	require.Eventually(t, func() bool {
		for i := 0; i < 4; i++ {
			c.node(i).syncEpochs(context.Background())
			if c.node(i).synced.Load() < 2 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond, "every node synchronised epoch 2")
	// A transaction of epoch 1 (a coordinator that had not learnt epoch
	// 2) is refused, then recovered and invalidated.
	stale := appendTxn(t, k, 2)
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: c.node(0).clock.Next(0), Node: "n0"}, Kind: Write}
	_, err = c.node(0).coordinate(context.Background(), id, stale)
	require.ErrorIs(t, err, errStale)
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
