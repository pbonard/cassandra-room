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

// TestUniqueHLC: when an earlier transaction (by timestamp: an older epoch
// whose slow path chose a higher HLC after the newer transaction took the
// fast path, or the same HLC on an earlier node) applied at HLC >=
// ExecuteAt.HLC, the later transaction executes and persists with a
// UniqueHLC strictly above it so its cells win in storage without changing
// the SSTable format.
func TestUniqueHLC(t *testing.T) {
	for _, tc := range []struct {
		name         string
		early, later Timestamp
	}{
		{
			name:  "across_epochs",
			early: Timestamp{Epoch: 1, HLC: 5_000_000, Node: "n2"},
			later: Timestamp{Epoch: 2, HLC: 2_000_000, Node: "n1"},
		},
		{
			name:  "same_hlc_tie",
			early: Timestamp{Epoch: 1, HLC: 5_000_000, Node: "n0"},
			later: Timestamp{Epoch: 1, HLC: 5_000_000, Node: "n1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSimCluster(t, 3, 3, 1)
			k := c.keys[0]
			for i := 0; i < 3; i++ {
				c.node(i).SetTopology(&Topology{Epoch: 2, Replicas: c.replicas, Nodes: c.ids})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			t0 := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 1_000_000, Node: "n0"}, Kind: Write}
			txn0 := appendTxn(t, k, 10)
			_, err := c.node(0).execute(ctx, Ballot{}, t0, txn0, tc.early, Deps{})
			require.NoError(t, err)

			t1 := TxnID{Timestamp: tc.later, Kind: Write}
			txn1 := appendTxn(t, k, 20)
			raw, err := c.node(1).execute(ctx, Ballot{}, t1, txn1, tc.later, Deps{keyString(k): {t0}})
			require.NoError(t, err)
			var res listResult
			require.NoError(t, json.Unmarshal(raw, &res))
			require.Equal(t, []int{10}, res.Reads[keyString(k)])
			require.Greater(t, res.ExecuteAt.HLC, tc.early.HLC, "UniqueHLC exceeds earlier write HLC")

			c.settle()
			for _, r := range c.replicas(k) {
				got := c.stores[r].data[keyString(k)]
				require.Len(t, got, 2)
				require.Equal(t, 10, got[0].Value)
				require.Equal(t, 20, got[1].Value)
				require.Greater(t, got[1].At.HLC, got[0].At.HLC)
			}
			st := c.node(0).s.checkStatus(t1, []Key{k})
			require.Equal(t, res.ExecuteAt.HLC, st.UniqueHLC)
		})
	}
}

// TestProposeKeepsDeps: propose on the slow path preserves the dependencies
// passed into it (from PreAccept or recovery's LatestDeps), even when the
// replicas answering the Accept quorum did not witness a dependency locally.
func TestProposeKeepsDeps(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Seed dep only on n2, then cut n2 off so the Accept quorum {n0, n1}
	// has never seen dep locally.
	dep := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 100, Node: "n2"}, Kind: Write}
	depTxn := appendTxn(t, k, 1)
	_, err := c.node(2).s.apply(ctx, &applyReq{ID: dep, ExecuteAt: dep.Timestamp, Deps: Deps{}, Txn: depTxn, Writes: Writes{keyString(k): []byte("1")}})
	require.NoError(t, err)
	c.net.mu.Lock()
	c.net.down["n2"] = true
	c.net.mu.Unlock()

	// n0 and n1 apply dep only once the transaction is committed: until
	// then its execution waits on dep, so the commit must have kept it.
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: 200, Node: "n0"}, Kind: Write}
	txn := appendTxn(t, k, 2)
	done := make(chan error, 1)
	go func() {
		_, err := c.node(0).propose(ctx, Ballot{}, id, txn, Timestamp{Epoch: 1, HLC: 300, Node: "n0"}, Deps{keyString(k): {dep}}, false)
		done <- err
	}()
	// Wait until n0 committed id: if propose preserved dep, execution on n0
	// blocks on dep (which n0 has not seen yet).
	require.Eventually(t, func() bool {
		c.node(0).s.mu.Lock()
		defer c.node(0).s.mu.Unlock()
		cmd := c.node(0).s.cmds[id]
		return cmd != nil && cmd.Status >= Committed && cmd.Deps.contains(keyString(k), dep)
	}, 2*time.Second, 10*time.Millisecond, "committed Deps kept input dependency")

	// Unblock execution by applying dep on n0 and n1.
	for _, i := range []int{0, 1} {
		_, err := c.node(i).s.apply(ctx, &applyReq{ID: dep, ExecuteAt: dep.Timestamp, Deps: Deps{}, Txn: depTxn, Writes: Writes{keyString(k): []byte("1")}})
		require.NoError(t, err)
	}
	require.NoError(t, <-done)
}

// TestDurabilityWaitsForPending: a universal sync point (and durability's
// redundancy bound) waits for pending replicas too, so a pending replica
// never adopts a redundancy bound while still missing a transaction below
// it, and receives the bound once caught up.
func TestDurabilityWaitsForPending(t *testing.T) {
	c := newSimCluster(t, 3, 2, 1)
	k := c.keys[0] // read replicas: n0, n1; n2 will be pending
	whole := Range{Lo: math.MinInt64, Hi: math.MaxInt64}
	topo := &Topology{
		Epoch:    2,
		Replicas: c.replicas,
		Nodes:    c.ids,
		Pending:  func(Key) []NodeID { return []NodeID{"n2"} },
		RangeReplicas: func(r Range) []RangeShard {
			return []RangeShard{{Range: r, Nodes: []NodeID{"n0", "n1"}, Pending: []NodeID{"n2"}}}
		},
	}
	for i := 0; i < 3; i++ {
		c.node(i).SetTopology(topo)
		c.node(i).cfg.DurabilityRanges = func() []Range { return []Range{whole} }
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.node(0).Coordinate(ctx, appendTxn(t, k, 1))
	require.NoError(t, err)
	c.settle()

	// While pending n2 is unreachable, a universal sync point must fail
	// rather than succeed off the read replicas {n0, n1} alone.
	c.net.mu.Lock()
	c.net.down["n2"] = true
	c.net.mu.Unlock()
	uctx, ucancel := context.WithTimeout(ctx, 250*time.Millisecond)
	_, err = c.node(0).SyncPoint(uctx, whole, true)
	ucancel()
	require.Error(t, err)

	// Once pending n2 is reachable again, durability advances Redundant on
	// n0, n1, and pending n2.
	c.net.mu.Lock()
	delete(c.net.down, "n2")
	c.net.mu.Unlock()
	c.node(0).durability()
	require.Equal(t, int64(1), c.node(0).Stats().DurabilityRounds)
	for i := 0; i < 3; i++ {
		require.Equal(t, 1, c.node(i).Stats().Redundant, "node n%d learnt the redundancy bound", i)
	}
}

// highBound gives a node a bootstrap bound over a key whose HLC is far
// above the others' (a node that took the range over from streamed data):
// it reads, for a transaction, a minimum write HLC above theirs.
func highBound(t *testing.T, n *Node, k Key, hlc int64) {
	require.NoError(t, n.s.setBootstrapped(BootstrapPoints{{R: Range{Lo: k.Token, Hi: k.Token}, TS: Timestamp{Epoch: 1, HLC: 1, Node: "x"}, MaxHLC: hlc}}))
}

// TestRecoveryAdoptsDecidedHLC: a transaction's write HLC is decided at a
// quorum before any replica applies it, and a recovery executing it again
// adopts the one accepted at the highest ballot instead of its own. Its
// read replica here would give another (n0 holds a higher bound): with
// each executor choosing its own, replicas stored the same transaction at
// different HLCs, and a later transaction, written just above one
// replica's, lost to another's.
func TestRecoveryAdoptsDecidedHLC(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n0, n1 := c.node(0), c.node(1)
	highBound(t, n0, k, n1.clock.Next(0)+10_000_000)
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n1.clock.Next(0), Node: "n1"}, Kind: Write}
	txn := appendTxn(t, k, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts := n1.phaseTopologies(txn.Keys, id.Epoch, id.Epoch)
	// n1 coordinated it up to the decided HLC, then stopped.
	require.NoError(t, n1.quorum(ctx, ts, mCommit, &commitReq{ID: id, ExecuteAt: id.Timestamp, Deps: Deps{}, Txn: txn, Keys: txn.Keys}))
	decided, err := n1.decideHLC(ctx, ts, Ballot{}, id, txn.Keys, id.HLC)
	require.NoError(t, err)
	require.Equal(t, id.HLC, decided)

	require.NoError(t, n0.recover(ctx, id, txn, txn.Keys))
	c.settle()
	for _, r := range c.ids {
		st := c.stores[r]
		st.mu.Lock()
		got := st.data[keyString(k)]
		st.mu.Unlock()
		require.Len(t, got, 1, r)
		require.Equal(t, decided, got[0].At.HLC, "%s writes at the decided HLC", r)
	}
}

// TestDecideHLCPreempted: once a recovery promised a higher ballot, the
// transaction's earlier executor can no longer have its write HLC
// accepted (it then waits for the recovery's outcome).
func TestDecideHLCPreempted(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	k := c.keys[0]
	n0, n1 := c.node(0), c.node(1)
	id := TxnID{Timestamp: Timestamp{Epoch: 1, HLC: n1.clock.Next(0), Node: "n1"}, Kind: Write}
	txn := appendTxn(t, k, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts := n1.phaseTopologies(txn.Keys, id.Epoch, id.Epoch)
	require.NoError(t, n1.quorum(ctx, ts, mCommit, &commitReq{ID: id, ExecuteAt: id.Timestamp, Deps: Deps{}, Txn: txn, Keys: txn.Keys}))
	b := Ballot{Epoch: 1, HLC: n0.clock.Next(0), Node: "n0"}
	for i := 0; i < 3; i++ {
		_, err := c.node(i).s.beginRecovery(&recoverReq{ID: id, Ballot: b, Txn: txn, Keys: txn.Keys})
		require.NoError(t, err)
	}
	_, err := n1.decideHLC(ctx, ts, Ballot{}, id, txn.Keys, id.HLC)
	require.ErrorIs(t, err, errPreempted)
}

// TestBootstrapSyncPointSkipsPending: a bootstrap's sync point applies at
// the replicas, not the pending ones (the nodes about to receive the
// data, which cannot apply it before they have it): its universal attempt
// succeeds without them, instead of timing out on every join.
func TestBootstrapSyncPointSkipsPending(t *testing.T) {
	c := newSimCluster(t, 3, 2, 1)
	whole := Range{Lo: math.MinInt64, Hi: math.MaxInt64}
	topo := &Topology{Epoch: 2, Replicas: c.replicas, Nodes: c.ids,
		Pending: func(Key) []NodeID { return []NodeID{"n2"} },
		RangeReplicas: func(r Range) []RangeShard {
			return []RangeShard{{Range: r, Nodes: []NodeID{"n0", "n1"}, Pending: []NodeID{"n2"}}}
		}}
	for i := 0; i < 3; i++ {
		c.node(i).SetTopology(topo)
	}
	c.net.mu.Lock()
	c.net.down["n2"] = true
	c.net.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, applied, _, err := c.node(0).syncPoint(ctx, []Range{whole}, syncOpts{universal: true})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"n0", "n1"}, applied)
	require.Less(t, time.Since(start), time.Second)
}

// epochWithRanges gives nodes epoch 2, whose one range shard (every key)
// has every node as a replica, and marks it synchronised (no leader runs
// epoch synchronisation here); stuck, if not -1, does not learn it: it
// answers nothing about epoch 2 until its request times out, as a node cut
// off from the metadata log.
func epochWithRanges(c *simCluster, stuck int) {
	epoch2 := &Topology{Epoch: 2, Replicas: c.replicas, Nodes: c.ids,
		RangeReplicas: func(r Range) []RangeShard { return []RangeShard{{Range: r, Nodes: c.ids}} }}
	for i := range c.ids {
		if i != stuck {
			c.node(i).SetTopology(epoch2)
			c.node(i).advanceSynced(2)
		}
	}
}

// TestSyncPointQuorumNoWait: a sync point that needs only a quorum returns
// once a quorum applied it, not after every replica of the round answered:
// a replica that does not answer cost a whole request timeout per sync
// point (per changed range, in turn, when synchronising an epoch a node
// joining waits for).
func TestSyncPointQuorumNoWait(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	epochWithRanges(c, 2)
	whole := Range{Lo: math.MinInt64, Hi: math.MaxInt64}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, applied, _, err := c.node(0).syncPoint(ctx, []Range{whole}, syncOpts{})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"n0", "n1"}, applied)
	require.Less(t, time.Since(start), c.node(0).cfg.RequestTimeout/2, "n2's request timeout not waited out")
}

// TestBootstrapPointsLinger: a bootstrap's sync point waits a moment for
// the replicas beyond a quorum (the more nodes applied it, the more the
// data may be streamed from), but not for one known to be down, nor more
// than a moment for one that does not answer.
func TestBootstrapPointsLinger(t *testing.T) {
	whole := Range{Lo: math.MinInt64, Hi: math.MaxInt64}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := newSimCluster(t, 3, 3, 1)
	epochWithRanges(c, -1)
	_, applied, err := c.node(0).BootstrapPoints(ctx, []Range{whole})
	require.NoError(t, err)
	require.Equal(t, c.ids, applied, "every replica answers: every one is a source")

	c = newSimCluster(t, 3, 3, 1)
	epochWithRanges(c, 2)
	n0 := c.node(0)
	start := time.Now()
	_, applied, err = n0.BootstrapPoints(ctx, []Range{whole})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"n0", "n1"}, applied)
	took := time.Since(start)
	require.Less(t, took, n0.cfg.RequestTimeout, "a replica that does not answer is waited for a moment only")

	n0.cfg.Alive = func(id NodeID) bool { return id != "n2" }
	start = time.Now()
	_, applied, err = n0.BootstrapPoints(ctx, []Range{whole})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"n0", "n1"}, applied)
	require.Less(t, time.Since(start), n0.cfg.RequestTimeout/4, "a replica known down is not waited for")
}

// TestAwaitSyncedAsks: a node waiting for an epoch to be synchronised asks
// the others for theirs, instead of only waiting for the leader's notice,
// which it may have missed (a node joining was not there for it).
func TestAwaitSyncedAsks(t *testing.T) {
	c := newSimCluster(t, 3, 3, 1)
	for i := 0; i < 2; i++ {
		c.node(i).advanceSynced(3)
	}
	n := c.node(2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	n.awaitSynced(ctx, 3)
	require.Equal(t, int64(3), n.synced.Load())
	require.Less(t, time.Since(start), time.Second)
}
