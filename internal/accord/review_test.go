package accord

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRecoverAcrossEpochs: a transaction committed on the fast path in
// epoch 1, recovered once epoch 2 added a replica that never voted, keeps
// its fast-path decision (only the epochs it was pre-accepted in count).
func TestRecoverAcrossEpochs(t *testing.T) {
	c := newSimCluster(t, 4, 3, 1)
	k := c.keys[0] // epoch 1: n0, n1, n2
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
	// Decided on the fast path; the commit reached n1 only.
	require.NoError(t, n0.send(ctx, "n1", mCommit, &commitReq{ID: id, ExecuteAt: id.Timestamp, Deps: deps, Txn: txn, Keys: txn.Keys}, nil))
	epoch2 := &Topology{Epoch: 2, Replicas: func(Key) []NodeID { return []NodeID{"n1", "n2", "n3"} }}
	for i := 0; i < 4; i++ {
		c.node(i).SetTopology(epoch2)
	}
	c.crash(1) // the only replica that knows the decision
	require.NoError(t, c.node(3).recover(ctx, id, txn, txn.Keys))
	require.Eventually(t, func() bool {
		s, at := c.status(2, id)
		return s == Applied && at == id.Timestamp
	}, 10*time.Second, 10*time.Millisecond, "executed at its TxnID, not invalidated")
}

// TestTruncatedOutcome: waiting for the outcome of a transaction below the
// redundancy bound ends at once; an undecided record below it is
// invalidated, then forgotten.
func TestTruncatedOutcome(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	c.setRanges(true, 100*time.Millisecond)
	k := c.keys[0]
	_, err := c.node(0).Coordinate(context.Background(), appendTxn(t, k, 1))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return c.node(1).Stats().Redundant > 0 }, 10*time.Second, 20*time.Millisecond)
	old := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 1, Node: "n0"}, Kind: Write}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err = c.node(0).awaitOutcome(ctx, old, []Key{k})
	require.ErrorIs(t, err, ErrTimeout)
	require.Less(t, time.Since(start), time.Second, "answered at once")

	// A replica holding an undecided record below the bound.
	n2 := c.node(2)
	n2.s.mu.Lock()
	cmd := n2.s.get(old)
	cmd.Txn = appendTxn(t, k, 2)
	n2.s.registerLocked(cmd, cmd.Txn.Keys)
	cmd.Status, cmd.ExecuteAt = PreAccepted, old.Timestamp
	n2.s.forgetLocked()
	require.Equal(t, Invalidated, cmd.Status)
	n2.s.forgetLocked()
	_, present := n2.s.cmds[old]
	n2.s.mu.Unlock()
	require.False(t, present, "forgotten once invalidated")
}

// TestEphemeralReadNewerEpoch: a coordinator still on epoch 1 reads after
// a write of epoch 2 (synchronised, so only epoch 2's replicas have it): a
// replica reports the newer epoch, and the read waits for it.
func TestEphemeralReadNewerEpoch(t *testing.T) {
	c := newSimCluster(t, 4, 3, 1)
	k := c.keys[0]
	epoch2 := &Topology{Epoch: 2, Replicas: func(Key) []NodeID { return []NodeID{"n1", "n2", "n3"} }}
	for i := 1; i < 4; i++ {
		c.node(i).SetTopology(epoch2)
		c.node(i).advanceSynced(2)
	}
	_, err := c.node(1).Coordinate(context.Background(), appendTxn(t, k, 7))
	require.NoError(t, err)
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.node(0).SetTopology(epoch2)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, _ := json.Marshal(listUpdate{Reads: []Key{k}})
	out, err := c.node(0).Coordinate(ctx, &Txn{Kind: Read, Keys: []Key{k}, Update: b})
	require.NoError(t, err)
	var res listResult
	require.NoError(t, json.Unmarshal(out, &res))
	require.Equal(t, []int{7}, res.Reads[keyString(k)], "the epoch 2 write is seen")
	require.Equal(t, int64(2), c.node(0).epoch())
	_ = math.MaxInt64
}
