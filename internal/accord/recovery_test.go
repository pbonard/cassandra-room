package accord

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func appendTxn(t *testing.T, k Key, v int) *Txn {
	b, err := json.Marshal(listUpdate{Reads: []Key{k}, Appends: map[string]int{keyString(k): v}})
	require.NoError(t, err)
	return &Txn{Kind: Write, Keys: []Key{k}, Update: b}
}

func (c *simCluster) node(i int) *Node {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.nodes[c.ids[i]]
}

func (c *simCluster) status(i int, id TxnID) (Status, Timestamp) {
	n := c.node(i)
	n.s.mu.Lock()
	defer n.s.mu.Unlock()
	cmd := n.s.cmds[id]
	if cmd == nil {
		return NotDefined, Timestamp{}
	}
	return cmd.Status, cmd.ExecuteAt
}

// TestRecoverFastPathCommit: the coordinator took the fast path and
// committed to one replica only before failing. A recovery that does not
// reach that replica must still execute the transaction at its TxnID (it
// could have been acknowledged), and the replica's committed dependencies
// stay valid.
func TestRecoverFastPathCommit(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n0 := c.node(0)
	txn := appendTxn(t, k, 1)
	id := n0.newTxnID(Write)
	ctx := context.Background()
	deps := Deps{}
	for i := 0; i < 3; i++ {
		var r preAcceptReply
		require.NoError(t, n0.send(ctx, c.ids[i], mPreAccept, &preAcceptReq{ID: id, Txn: txn}, &r))
		require.True(t, r.FastVote)
		deps = deps.with(r.Deps)
	}
	require.NoError(t, n0.send(ctx, c.ids[1], mCommit, &commitReq{ID: id, ExecuteAt: id.Timestamp, Deps: deps, Txn: txn, Keys: txn.Keys}, nil))
	c.crash(0)
	c.crash(1)
	c.start(1)
	// Node 1 is back but the recovery quorum is {1, 2} or {2, x}: either
	// way the decision is kept.
	require.NoError(t, c.node(2).recover(ctx, id, txn, txn.Keys))
	c.start(0)
	require.Eventually(t, func() bool {
		for i := 0; i < 3; i++ {
			if s, _ := c.status(i, id); s != Applied {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
	for i := 0; i < 3; i++ {
		_, at := c.status(i, id)
		require.Equal(t, id.Timestamp, at)
	}
}

// TestRecoverInvalidates: a transaction pre-accepted by one replica only
// cannot have been committed; recovery invalidates it, and a later
// transaction on the key that did not witness it is unaffected.
func TestRecoverInvalidates(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n0 := c.node(0)
	ctx := context.Background()
	txn := appendTxn(t, k, 1)
	id := n0.newTxnID(Write)
	var r preAcceptReply
	require.NoError(t, n0.send(ctx, c.ids[0], mPreAccept, &preAcceptReq{ID: id, Txn: txn}, &r))
	c.crash(0)
	// A later transaction commits on nodes 1 and 2 without witnessing it.
	out, err := c.node(1).Coordinate(ctx, appendTxn(t, k, 2))
	require.NoError(t, err)
	var res listResult
	require.NoError(t, json.Unmarshal(out, &res))
	c.start(0)
	// Node 0's progress log recovers it.
	require.Eventually(t, func() bool {
		for i := 0; i < 3; i++ {
			if s, _ := c.status(i, id); s != Invalidated {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
	c.settle()
	for _, id := range c.ids {
		require.Len(t, c.stores[id].data[keyString(k)], 1)
	}
}

// TestAcceptReachesNewEpoch: a transaction started in epoch 1 whose
// execution timestamp falls in epoch 2 is accepted, committed and applied
// by the replicas of both epochs (CASSANDRA-18365: Accept must not miss the
// dependencies of a newer epoch's replicas).
func TestAcceptReachesNewEpoch(t *testing.T) {
	c := newSimCluster(t, 4, 3, 1)
	k := c.keys[0] // replicas n0, n1, n2 in epoch 1
	epoch2 := &Topology{Epoch: 2, Replicas: func(Key) []NodeID { return []NodeID{"n1", "n2", "n3"} }}
	n0 := c.node(0)
	txn := appendTxn(t, k, 7)
	id := n0.newTxnID(Write) // epoch 1
	for i := 0; i < 4; i++ {
		c.node(i).SetTopology(epoch2)
	}
	out, err := n0.coordinate(context.Background(), id, txn)
	require.NoError(t, err)
	var res listResult
	require.NoError(t, json.Unmarshal(out, &res))
	require.Equal(t, int64(2), res.ExecuteAt.Epoch, "replicas proposed an epoch 2 timestamp")
	require.Eventually(t, func() bool {
		s, _ := c.status(3, id)
		return s == Applied
	}, 5*time.Second, 10*time.Millisecond, "the epoch 2 replica took part")
	require.Len(t, c.stores["n3"].data[keyString(k)], 1)
}

// TestUndurableRepersisted: a transaction applied at a quorum whose
// coordinator stopped before its Apply reached the last replica (which
// never heard of it) is still applied there: the home shard's progress
// log sends the outcome to every replica, then marks it durable.
func TestUndurableRepersisted(t *testing.T) {
	simTest(t, func(t *testing.T) {
		c := newSimCluster(t, 3, 3, 1)
		k := c.keys[0]
		n1 := c.node(1)
		id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n1.clock.Next(0), Node: "n1"}, Kind: Write}
		txn := appendTxn(t, k, 7)
		w, err := json.Marshal(7)
		require.NoError(t, err)
		req := &applyReq{ID: id, ExecuteAt: id.Timestamp, Deps: Deps{}, Txn: txn, Writes: Writes{keyString(k): w}}
		for _, i := range []int{0, 1} {
			_, err := c.node(i).s.apply(context.Background(), req)
			require.NoError(t, err)
		}
		// The coordinator stops (with whatever it had in flight) and
		// comes back; n2 knows nothing of the transaction.
		c.crash(1)
		c.start(1)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		for _, r := range c.ids {
			st := c.stores[r]
			st.mu.Lock()
			got := st.data[keyString(k)]
			st.mu.Unlock()
			require.Len(t, got, 1, "%s applied it", r)
			s, _ := c.status(slices.Index(c.ids, r), id)
			require.Equal(t, Applied, s, r)
		}
		for i := range c.ids {
			n := c.node(i)
			n.s.mu.Lock()
			durable := n.s.cmds[id].Durable
			n.s.mu.Unlock()
			require.True(t, durable, "%s knows it durable", c.ids[i])
		}
	})
}

// TestRecoverOnOtherKeysKeepsFastPath: a node recovering a dependency it
// does not know, on keys the transaction does not touch, must not take
// those shards' missing votes for "no fast path". Here n3 decides a
// transaction on k3 alone on the fast path (its own vote and n4's), then
// crashes; n0, whose waiting transaction is on k0 and k3, recovers it on
// both. The k0 shard never saw it: counting its votes invalidated a
// transaction already decided.
func TestRecoverOnOtherKeysKeepsFastPath(t *testing.T) {
	simTest(t, func(t *testing.T) {
		c := newSimCluster(t, 5, 3, 4) // k0 on n0-n2, k3 on n3, n4, n0
		k0, k3 := c.keys[0], c.keys[3]
		n3, n4 := c.node(3), c.node(4)
		id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n3.clock.Next(0), Node: "n3"}, Kind: Write, Privileged: true}
		txn := appendTxn(t, k3, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r, err := n3.s.preAccept(&preAcceptReq{ID: id, Txn: txn})
		require.NoError(t, err)
		require.True(t, r.FastVote)
		r4 := handle[preAcceptReply](t, ctx, n4, "n3", mPreAccept, &preAcceptReq{ID: id, Txn: txn})
		require.True(t, r4.FastVote)
		require.True(t, n3.s.fastCommit(id, txn, r.Deps.with(r4.Deps)))
		c.crash(3)

		require.NoError(t, c.node(0).recover(ctx, id, nil, []Key{k0, k3}))
		s, at := c.status(4, id)
		require.NotEqual(t, Invalidated, s, "a transaction decided on the fast path was invalidated")
		require.True(t, s.decided(), "n4: %s", s)
		require.Equal(t, id.Timestamp, at)
	})
}

// TestDepKeys: a dependency is recovered on the keys it is a dependency
// on, which it touches.
func TestDepKeys(t *testing.T) {
	a := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 1, Node: "n1"}}
	b := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 2, Node: "n1"}}
	k0, k3 := Key{Token: 0, ID: "k0"}, Key{Token: 3, ID: "k3"}
	d := Deps{}
	d.add(k0, a)
	d.add(k3, a)
	d.add(k3, b)
	require.Equal(t, []Key{k0, k3}, depKeys(d, a))
	require.Equal(t, []Key{k3}, depKeys(d, b))
	require.Empty(t, depKeys(d, TxnID{}))
	require.True(t, sameKeys([]Key{k3, k0}, []Key{k0, k3}))
	require.False(t, sameKeys([]Key{k3}, []Key{k0, k3}))
}
