package accord

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"
)

// Exclusive sync points (CoordinateSyncPoint, ExclusiveSyncPoint): a
// transaction over ranges, with no reads or writes, that follows every
// transaction with a lower id on its ranges. Each replica that witnesses it
// records a barrier and refuses any new transaction with a lower id there
// (the coordinator of such a transaction recovers it, which invalidates it,
// and retries with a new id). So once a sync point is applied (its
// dependencies all applied), every transaction with a lower id on its
// ranges has either applied or can never commit.
//
// Sync points serve:
//   - durability (CoordinateShardDurable, RedundantBefore): a sync point
//     applied at every replica of a range makes everything below it
//     redundant there: replicas forget those transactions, and refuse late
//     messages about them;
//   - bootstrap (Bootstrap): a joining node takes a sync point applied at
//     every current replica before it streams the data, which therefore
//     holds every transaction before it.
//
// A sync point is never recovered: one its coordinator abandons only leaves
// barriers (harmless), and is invalidated locally after a while.

type syncReq struct {
	ID  TxnID
	Txn *Txn
}

type syncReply struct {
	Outcome outcome
	Deps    Deps
}

// syncPropose witnesses a sync point: it records the barrier and returns
// every transaction on its ranges with a lower id (all kinds).
func (s *store) syncPropose(r *syncReq) (*syncReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.get(r.ID)
	if c.Status != NotDefined {
		return &syncReply{Outcome: ok, Deps: c.Deps}, nil
	}
	c.Txn, c.Status, c.ExecuteAt = r.Txn, Accepted, r.ID.Timestamp
	deps := Deps{}
	for _, rg := range r.Txn.Ranges {
		s.barriers = addBound(s.barriers, rg, r.ID.Timestamp)
		i := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i].key.Token >= rg.Lo })
		for ; i < len(s.sorted) && s.sorted[i].key.Token <= rg.Hi; i++ {
			f := s.sorted[i]
			if !rg.contains(f.key) {
				continue
			}
			for _, id := range f.ids {
				if !id.Timestamp.Less(r.ID.Timestamp) {
					break
				}
				if o := s.cmds[id]; o != nil && o.Status != NotDefined && o.Status != Invalidated && o.Status != Truncated {
					deps.add(f.key, id)
				}
			}
		}
	}
	c.Deps = deps
	s.n.clock.Observe(r.ID.HLC)
	return &syncReply{Outcome: ok, Deps: deps}, s.saveLocked(c)
}

// syncOwnedLocked sets the keys a sync point waits on here: those of its
// dependencies this replica owns.
func (s *store) syncOwnedLocked(c *command) {
	var keys []Key
	for ks := range c.Deps {
		if k, ok := parseKeyString(ks); ok {
			keys = append(keys, k)
		}
	}
	c.owned = s.n.owned(keys, 0)
	if c.owned == nil {
		c.owned = []Key{}
	}
}

// markRedundant records that a sync point is applied at every replica of
// its ranges (RedundantBefore).
func (s *store) markRedundant(id TxnID, ranges []Range) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range ranges {
		s.redundant = addBound(s.redundant, r, id.Timestamp)
		s.barriers = addBound(s.barriers, r, id.Timestamp)
	}
	c := s.get(id)
	if c.Txn == nil {
		c.Txn = &Txn{Kind: SyncPoint, Ranges: ranges}
	}
	if c.Status != Applied {
		c.Status, c.ExecuteAt = Applied, id.Timestamp
	}
	c.Redundant = true
	return s.saveLocked(c)
}

// rangeTopologies returns the shards of a range in every epoch not yet
// synchronised.
func (n *Node) rangeTopologies(r Range) *Topologies {
	latest := n.epoch()
	ts := &Topologies{}
	n.topoMu.RLock()
	defer n.topoMu.RUnlock()
	for e := min(n.synced.Load(), latest); e <= latest; e++ {
		t := n.topos[e]
		if t == nil || t.RangeReplicas == nil {
			continue
		}
		for _, rs := range t.RangeReplicas(r) {
			nodes := slices.Clone(rs.Nodes)
			slices.Sort(nodes)
			ts.Shards = append(ts.Shards, &Shard{Epoch: e, Nodes: slices.Compact(nodes)})
		}
	}
	return ts
}

// SyncPoint coordinates an exclusive sync point over a range and returns
// its id once it applied at a quorum of every shard (at every replica if
// universal).
func (n *Node) SyncPoint(ctx context.Context, r Range, universal bool) (TxnID, error) {
	ts := n.rangeTopologies(r)
	if len(ts.Shards) == 0 {
		return TxnID{}, errors.New("accord: no topology for the range")
	}
	id := n.newTxnID(SyncPoint)
	txn := &Txn{Kind: SyncPoint, Ranges: []Range{r}}
	nodes := ts.nodes()
	ch := broadcast[syncReply](ctx, n, nodes, mSyncPropose, func(NodeID) any { return &syncReq{ID: id, Txn: txn} })
	tr := newTracker(ts, (*Shard).slowQuorum)
	deps := Deps{}
	for i := 0; i < len(nodes) && !tr.done(); i++ {
		var rep response[syncReply]
		select {
		case rep = <-ch:
		case <-ctx.Done():
			return TxnID{}, ErrTimeout
		}
		if rep.err != nil || rep.reply.Outcome != ok {
			tr.failure(rep.from)
		} else {
			tr.success(rep.from)
			deps = deps.with(rep.reply.Deps)
		}
		if tr.impossible() {
			return TxnID{}, ErrTimeout
		}
	}
	if !tr.done() {
		return TxnID{}, ErrTimeout
	}
	n.metrics.syncPoints.Add(1)
	// Applying it means every dependency applied at that replica.
	req := &applyReq{ID: id, ExecuteAt: id.Timestamp, Deps: deps, Txn: txn, Writes: Writes{}}
	applied := map[NodeID]bool{}
	tr = newTracker(ts, (*Shard).slowQuorum)
	backoff := 50 * time.Millisecond
	for {
		pending := slices.DeleteFunc(slices.Clone(nodes), func(x NodeID) bool { return applied[x] })
		ach := broadcast[applyReply](ctx, n, pending, mApply, func(NodeID) any { return req })
		for range pending {
			var rep response[applyReply]
			select {
			case rep = <-ach:
			case <-ctx.Done():
				return TxnID{}, ErrTimeout
			}
			if rep.err == nil && rep.reply.Outcome == ok {
				applied[rep.from] = true
				tr.success(rep.from)
			}
		}
		if universal && len(applied) == len(nodes) || !universal && tr.done() {
			return id, nil
		}
		select {
		case <-ctx.Done():
			return TxnID{}, ErrTimeout
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// durability runs a universal sync point over each durability range and,
// once applied everywhere, makes it the range's redundancy bound on every
// replica (CoordinateShardDurable, then SetShardDurable).
func (n *Node) durability() {
	for _, r := range n.cfg.DurabilityRanges() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*n.recoveryTimeout())
		go func() {
			select {
			case <-n.stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		id, err := n.SyncPoint(ctx, r, true)
		if err == nil {
			for _, to := range n.rangeTopologies(r).nodes() {
				_ = n.send(ctx, to, mRedundant, &syncReq{ID: id, Txn: &Txn{Kind: SyncPoint, Ranges: []Range{r}}}, nil)
			}
			n.metrics.durableRounds.Add(1)
		}
		cancel()
	}
}

// Bootstrap takes a sync point applied at every replica of each range, for
// a node about to stream the ranges' data: the data then holds every
// transaction before the sync points, and this node does not wait for
// those it does not know (reads still fetch them first).
func (n *Node) Bootstrap(ctx context.Context, ranges []Range) error {
	var at Timestamp
	for _, r := range ranges {
		// Applied at every replica if they all answer in time; otherwise
		// (a replica is down, as when replacing it) at a quorum: the
		// transactions some streaming source may lack are then fetched
		// before this node reads them.
		uctx, cancel := context.WithTimeout(ctx, 4*n.cfg.RequestTimeout)
		id, err := n.SyncPoint(uctx, r, true)
		cancel()
		if err != nil {
			id, err = n.SyncPoint(ctx, r, false)
		}
		if err != nil {
			return err
		}
		at = maxTimestamp(at, id.Timestamp)
	}
	if prev := n.bootstrapped.Load(); prev == nil || prev.Less(at) {
		n.bootstrapped.Store(&at)
	}
	return nil
}
