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
	require.Eventually(t, func() bool { return c.node(1).Stats().Redundant > 0 && c.node(2).Stats().Redundant > 0 }, 10*time.Second, 20*time.Millisecond)
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
	status := cmd.Status
	n2.s.forgetLocked()
	_, present := n2.s.cmds[old]
	n2.s.mu.Unlock()
	require.Equal(t, Invalidated, status)
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

// TestJournalFailureBlocks: while the journal fails, a replica answers
// nothing it has not made durable, not even a retried request whose state
// is already in memory; once writes succeed, both answer.
func TestJournalFailureBlocks(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	n0 := c.node(0)
	k := c.keys[0]
	txn := appendTxn(t, k, 1)
	id := n0.newTxnID(Write)
	c.jours["n0"].SetFail(errors.New("disk failed"))
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := n0.s.preAccept(&preAcceptReq{ID: id, Txn: txn})
			done <- err
		}()
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("answered while the journal fails: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.Positive(t, n0.Stats().JournalErrors)
	c.jours["n0"].SetFail(nil)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("not answered once the journal works")
		}
	}
	cs, err := c.jours["n0"].Load()
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(cs, func(cmd *Command) bool { return cmd.ID == id && cmd.Status >= PreAccepted }))
}

// TestAdoptedBoundSurvivesRestart: a redundancy bound learnt from another
// replica is journaled before the records below it are forgotten.
func TestAdoptedBoundSurvivesRestart(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	n0 := c.node(0)
	full := Range{Lo: math.MinInt64, Hi: math.MaxInt64}
	below := n0.newTxnID(Write)
	at := n0.newTxnID(SyncPoint).Timestamp
	require.True(t, n0.s.adoptRedundant(below, []bound{{R: full, TS: at}}))
	require.Equal(t, at, boundFor(n0.s.redundant, c.keys[0]))
	c.crash(0)
	c.start(0)
	n0 = c.node(0)
	n0.s.mu.Lock()
	defer n0.s.mu.Unlock()
	require.Equal(t, at, boundFor(n0.s.redundant, c.keys[0]))
}

// TestAwaitOutcomeApplies: a coordinator that finds the outcome of
// another's transaction at a single replica applies it at a slow quorum
// before answering, as the coordinator of a transaction does: a QUORUM
// read outside Accord right after the answer must see its writes.
func TestAwaitOutcomeApplies(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n0 := c.node(0)
	txn := appendTxn(t, k, 7)
	id := n0.newTxnID(Write)
	ctx := context.Background()
	deps := Deps{}
	for i := 0; i < 3; i++ {
		var r preAcceptReply
		require.NoError(t, n0.send(ctx, c.ids[i], mPreAccept, &preAcceptReq{ID: id, Txn: txn}, &r))
		deps = deps.with(r.Deps)
	}
	// Decided and applied at n1 only (its coordinator stopped there).
	w, _ := json.Marshal(7)
	var ar applyReply
	require.NoError(t, n0.send(ctx, "n1", mApply, &applyReq{ID: id, ExecuteAt: id.Timestamp, Deps: deps, Txn: txn,
		Writes: Writes{keyString(k): w}, Result: []byte(`"done"`)}, &ar))
	holding := func() int {
		n := 0
		for _, s := range c.stores {
			s.mu.Lock()
			if len(s.data[keyString(k)]) > 0 {
				n++
			}
			s.mu.Unlock()
		}
		return n
	}
	require.Equal(t, 1, holding())
	res, err := c.node(2).awaitOutcome(ctx, id, txn.Keys)
	require.NoError(t, err)
	require.Equal(t, `"done"`, string(res))
	require.GreaterOrEqual(t, holding(), 2, "applied at a quorum before answering")
}

// TestShardsByPending: keys with the same replicas but different pending
// ones are different shards. A node pending on one key (taking over its
// range) owned the other keys of that key's shard too, and waited for
// their transactions, which never reached it.
func TestShardsByPending(t *testing.T) {
	k1, k2 := Key{Token: 1, ID: "a"}, Key{Token: 2, ID: "b"}
	topo := &Topology{Epoch: 1, Nodes: []NodeID{"n0", "n1", "n2"},
		Replicas: func(Key) []NodeID { return []NodeID{"n0", "n1"} },
		Pending: func(k Key) []NodeID {
			if k == k1 {
				return []NodeID{"n2"}
			}
			return nil
		}}
	shards := topo.shards([]Key{k1, k2})
	require.Len(t, shards, 2)
	for _, s := range shards {
		require.Len(t, s.Keys, 1)
		if s.Keys[0] == k1 {
			require.Equal(t, []NodeID{"n2"}, s.Pending)
		} else {
			require.Empty(t, s.Pending)
		}
	}
	c := newSimCluster(t, 3, 3, 1)
	n := c.node(2)
	n.SetTopology(&Topology{Epoch: 2, Nodes: topo.Nodes, Replicas: topo.Replicas, Pending: topo.Pending})
	require.Equal(t, []Key{k1}, n.owned([]Key{k1, k2}, 2), "pending on k1 only")
}
