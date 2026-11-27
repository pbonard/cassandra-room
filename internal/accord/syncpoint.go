package accord

import (
	"context"
	"encoding/json"
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

func (s *store) syncPropose(r *syncReq) (*syncReply, error) {
	s.mu.Lock()
	rep, err := s.syncProposeLocked2(r)
	if uerr := s.unlock(); err == nil {
		err = uerr
	}
	return rep, err
}

// syncPropose witnesses a sync point: it records the barrier and returns
// every transaction on its ranges with a lower id (all kinds).
func (s *store) syncProposeLocked2(r *syncReq) (*syncReply, error) {
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

func (s *store) markRedundant(id TxnID, ranges []Range) error {
	s.mu.Lock()
	err := s.markRedundantLocked2(id, ranges)
	if uerr := s.unlock(); err == nil {
		err = uerr
	}
	return err
}

// markRedundant records that a sync point is applied at every replica of
// its ranges (RedundantBefore).
func (s *store) markRedundantLocked2(id TxnID, ranges []Range) error {
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

// markMajority records that a sync point applied at a majority of every
// shard of its ranges: the transactions below it need no local dependencies
// any more (a recovery finds them decided), so their records shrink.
func (s *store) markMajority(id TxnID, ranges []Range) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range ranges {
		s.majority = addBound(s.majority, r, id.Timestamp)
		s.barriers = addBound(s.barriers, r, id.Timestamp)
	}
}

// compactMajorityLocked drops the local dependencies of applied commands
// below a majority bound (TruncatedApplyWithOutcome keeps the outcome).
func (s *store) compactMajorityLocked() {
	if len(s.majority) == 0 {
		return
	}
	for id, c := range s.cmds {
		if c.Status != Applied || c.LocalDeps == nil || len(c.Keys) == 0 {
			continue
		}
		below := true
		for _, k := range c.Keys {
			below = below && id.Timestamp.Less(boundFor(s.majority, k))
		}
		if below {
			c.LocalDeps = nil
			_ = s.saveLocked(c)
		}
	}
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
			pending := slices.Clone(rs.Pending)
			slices.Sort(pending)
			ts.Shards = append(ts.Shards, &Shard{Epoch: e, Nodes: slices.Compact(nodes), Pending: slices.Compact(pending)})
		}
	}
	return ts
}

// SyncPoint coordinates an exclusive sync point over a range and returns
// its id once it applied at a quorum of every shard (at every replica if
// universal).
func (n *Node) SyncPoint(ctx context.Context, r Range, universal bool) (TxnID, error) {
	id, _, _, err := n.syncPoint(ctx, []Range{r}, syncOpts{universal: universal, pending: universal})
	return id, err
}

// syncOpts say when a sync point is done applying: at a quorum of every
// shard, or at every replica if universal (pending ones too if pending);
// linger, after a quorum, also waits that long at most for the other
// replicas not known to be down, to be among the nodes it applied at.
//
// Durability rounds wait for pending replicas, so that a redundancy bound
// is never set before they applied the transactions it covers. A
// bootstrap's sync point does not: the pending replicas are the nodes
// about to receive the data, which cannot apply it before they have it.
type syncOpts struct {
	universal, pending bool
	linger             time.Duration
}

// syncPoint coordinates one exclusive sync point over ranges: every
// transaction on any of them with a lower id is one of its dependencies.
// It returns once it applied as opts say, with the nodes it applied at by
// then and the largest HLC applied on their ranges.
func (n *Node) syncPoint(ctx context.Context, ranges []Range, opts syncOpts) (TxnID, []NodeID, int64, error) {
	ts := &Topologies{}
	for _, r := range ranges {
		ts.Shards = append(ts.Shards, n.rangeTopologies(r).Shards...)
	}
	if len(ts.Shards) == 0 {
		return TxnID{}, nil, 0, errors.New("accord: no topology for the range")
	}
	id := n.newTxnID(SyncPoint)
	txn := &Txn{Kind: SyncPoint, Ranges: slices.Clone(ranges)}
	nodes := ts.nodes()
	ch := broadcast[syncReply](ctx, n, nodes, mSyncPropose, func(NodeID) any { return &syncReq{ID: id, Txn: txn} })
	tr := newTracker(ts, (*Shard).slowQuorum)
	deps := Deps{}
	for i := 0; i < len(nodes) && !tr.done(); i++ {
		var rep response[syncReply]
		select {
		case rep = <-ch:
		case <-ctx.Done():
			return TxnID{}, nil, 0, ErrTimeout
		}
		if rep.err != nil || rep.reply.Outcome != ok {
			tr.failure(rep.from)
		} else {
			tr.success(rep.from)
			deps = deps.with(rep.reply.Deps)
		}
		if tr.impossible() {
			return TxnID{}, nil, 0, ErrUnavailable
		}
	}
	if !tr.done() {
		return TxnID{}, nil, 0, ErrTimeout
	}
	n.metrics.syncPoints.Add(1)
	// Applying it means every dependency applied at that replica.
	applyTargets := nodes
	if opts.universal && opts.pending {
		applyTargets = ts.applyNodes()
	}
	req := &applyReq{ID: id, ExecuteAt: id.Timestamp, Deps: deps, Txn: txn, Writes: Writes{}}
	applied := map[NodeID]bool{}
	maxHLC := id.HLC
	tr = newTracker(ts, (*Shard).slowQuorum)
	// done: as soon as it is (a replica that does not answer, cut off or
	// behind the epoch, is not waited for once a quorum applied).
	var lingerUntil time.Time
	done := func() bool {
		if opts.universal {
			return len(applied) == len(applyTargets)
		}
		if !tr.done() {
			return false
		}
		if opts.linger <= 0 {
			return true
		}
		if lingerUntil.IsZero() {
			lingerUntil = time.Now().Add(opts.linger)
		}
		for _, x := range applyTargets {
			if !applied[x] && n.alive(x) && time.Now().Before(lingerUntil) {
				return false
			}
		}
		return true
	}
	result := func() (TxnID, []NodeID, int64, error) {
		var at []NodeID
		for x := range applied {
			at = append(at, x)
		}
		slices.Sort(at)
		return id, at, maxHLC, nil
	}
	backoff := 50 * time.Millisecond
	for {
		pending := slices.DeleteFunc(slices.Clone(applyTargets), func(x NodeID) bool { return applied[x] })
		ach := broadcast[applyReply](ctx, n, pending, mApply, func(NodeID) any { return req })
		for range pending {
			var wait <-chan time.Time
			if !lingerUntil.IsZero() {
				wait = time.After(time.Until(lingerUntil))
			}
			var rep response[applyReply]
			select {
			case rep = <-ach:
			case <-wait:
				if done() {
					return result()
				}
				continue
			case <-ctx.Done():
				return TxnID{}, nil, 0, ErrTimeout
			}
			if rep.err == nil && rep.reply.Outcome == ok {
				applied[rep.from] = true
				maxHLC = max(maxHLC, rep.reply.MaxHLC)
				tr.success(rep.from)
			}
			if done() {
				return result()
			}
		}
		if done() {
			return result()
		}
		select {
		case <-ctx.Done():
			return TxnID{}, nil, 0, ErrTimeout
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
		if !n.s.dirty(r) {
			continue // nothing above its redundancy bound
		}
		ctx, cancel := context.WithTimeout(n.ctx, 2*n.recoveryTimeout())
		uctx, ucancel := context.WithTimeout(ctx, n.recoveryTimeout())
		id, err := n.SyncPoint(uctx, r, true)
		ucancel()
		msg := mRedundant
		if err != nil {
			// A replica is down or slow: a sync point applied at a majority
			// still bounds what the others must keep (MajorityDurable).
			msg = mMajority
			id, err = n.SyncPoint(ctx, r, false)
		}
		if err == nil {
			rts := n.rangeTopologies(r)
			targets := rts.nodes()
			if msg == mRedundant {
				targets = rts.applyNodes()
			}
			for _, to := range targets {
				_ = n.send(ctx, to, msg, &syncReq{ID: id, Txn: &Txn{Kind: SyncPoint, Ranges: []Range{r}}}, nil)
			}
			if msg == mRedundant {
				n.metrics.durableRounds.Add(1)
			} else {
				n.metrics.majorityRounds.Add(1)
			}
		}
		cancel()
	}
}

// Bootstrap takes a sync point over ranges, for this node about to stream
// their data from some of the nodes it applied at (returned): the data then
// holds every transaction on a range before its point, and this node does
// not wait for those it does not know (reads still fetch them first).
func (n *Node) Bootstrap(ctx context.Context, ranges []Range) ([]NodeID, error) {
	pts, applied, err := n.BootstrapPoints(ctx, ranges)
	if err != nil {
		return nil, err
	}
	return applied, n.s.setBootstrapped(pts)
}

// BootstrapPoints are sync points over ranges, one per range.
type BootstrapPoints []bound

// For returns the points of ranges (each one of those taken).
func (b BootstrapPoints) For(ranges []Range) BootstrapPoints {
	var out BootstrapPoints
	for _, p := range b {
		if slices.Contains(ranges, p.R) {
			out = append(out, p)
		}
	}
	return out
}

// BootstrapPoints takes one sync point over all the ranges, applied at a
// quorum of every shard and at the other replicas that apply it in a
// moment (not those down, as when replacing or removing one, or cut off,
// or behind the epoch), and returns the nodes it applied at. The ranges'
// data then holds every transaction on them before it only where it is
// streamed from one of those: a node that had not applied it may still
// apply a transaction before it after the data left. The nodes receiving the data learn the
// point with SendBootstrapped (or Bootstrap for this node). One sync point,
// not one per range: a transaction on a range with an id between that
// range's point and a later one's would be covered by neither, though a
// node-wide point skipped it.
func (n *Node) BootstrapPoints(ctx context.Context, ranges []Range) (BootstrapPoints, []NodeID, error) {
	if len(ranges) == 0 {
		return nil, nil, nil
	}
	// First, the epoch this node learnt the movement in must be
	// synchronised: until then, a transaction of an earlier epoch (whose
	// replicas do not include the nodes receiving the ranges) may still
	// commit with an id above the sync point, which would neither be one of
	// its dependencies nor reach them. Once it is, replicas refuse those,
	// and they are coordinated again in an epoch that includes them.
	n.awaitSynced(ctx, n.epoch())
	// One sync point, kept once a quorum applied it: the replicas that do
	// not apply it in a moment (down, cut off, behind the epoch) are not
	// waited for, only left out of the nodes the data may be streamed from.
	id, applied, maxHLC, err := n.syncPoint(ctx, ranges, syncOpts{linger: n.cfg.RequestTimeout / 2})
	if err != nil {
		return nil, nil, err
	}
	out := make(BootstrapPoints, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, bound{R: r, TS: id.Timestamp, MaxHLC: maxHLC})
	}
	return out, applied, nil
}

// awaitSynced waits until epoch is synchronised (every transaction of an
// earlier epoch on the ranges that changed is done, and the later ones are
// refused), for at most half of ctx's time: the epoch synchronisation runs
// on the leader, and the caller goes on without it if it lags. Besides the
// leader's notice (sent once, which a node joining may not have been there
// for, or may miss), it asks the other nodes for their synchronised epoch
// every so often.
func (n *Node) awaitSynced(ctx context.Context, epoch int64) {
	limit := 10 * n.cfg.RequestTimeout
	if d, ok := ctx.Deadline(); ok {
		limit = min(limit, time.Until(d)/2)
	}
	deadline := time.Now().Add(limit)
	var asked time.Time
	for n.synced.Load() < epoch && time.Now().Before(deadline) {
		if time.Since(asked) >= 200*time.Millisecond {
			asked = time.Now()
			n.askSynced(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// askSynced adopts the highest synchronised epoch the other nodes of the
// latest epoch report (each learnt it from the leader once the epoch's sync
// points applied).
func (n *Node) askSynced(ctx context.Context) {
	n.topoMu.RLock()
	var nodes []NodeID
	if t := n.topos[n.latest]; t != nil {
		nodes = t.Nodes
	}
	n.topoMu.RUnlock()
	for _, to := range nodes {
		if to == n.cfg.ID || !n.alive(to) {
			continue
		}
		n.background(func(context.Context) {
			ctx, cancel := n.bound(ctx)
			defer cancel()
			var r epochSynced
			if err := n.send(ctx, to, mGetSynced, struct{}{}, &r); err == nil {
				n.advanceSynced(r.Epoch)
			}
		})
	}
}

type bootstrappedReq struct {
	Points BootstrapPoints
}

// SendBootstrapped tells a node it received the data of ranges as of their
// bootstrap points (a node taking over ranges a leaving or removed node
// held, streamed to it by another).
func (n *Node) SendBootstrapped(ctx context.Context, to NodeID, pts BootstrapPoints) error {
	if len(pts) == 0 {
		return nil
	}
	return n.send(ctx, to, mBootstrapped, &bootstrappedReq{Points: pts}, nil)
}

var bootstrapRecordID = TxnID{Timestamp: Timestamp{Node: "~bootstrap"}, Kind: SyncPoint}

// setBootstrapped records bootstrap points, durably.
func (s *store) setBootstrapped(pts BootstrapPoints) error {
	s.mu.Lock()
	changed := false
	for _, p := range pts {
		s.n.clock.Observe(max(p.TS.HLC, p.MaxHLC))
		if !slices.ContainsFunc(s.bootstrapped, func(b bound) bool { return b.R == p.R && !b.TS.Less(p.TS) && b.MaxHLC >= p.MaxHLC }) {
			s.bootstrapped = addBootstrapBound(s.bootstrapped, p)
			changed = true
		}
	}
	if !changed {
		s.mu.Unlock()
		return nil
	}
	// Commands waiting on dependencies the point makes unnecessary (from
	// before the data this node received) can execute now.
	for _, c := range s.cmds {
		s.maybeExecuteLocked(c)
	}
	// One fixed id (no real command has it), so the record replaces any
	// earlier bootstrap record.
	rec := &Command{ID: bootstrapRecordID, Bootstrap: true, Bounds: slices.Clone(s.bootstrapped)}
	s.bootstrapRec = rec
	if s.gc != nil {
		b, err := json.Marshal(rec)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.lastSeq = s.gc.enqueue(Record{ID: rec.ID, Data: b})
		return s.unlock()
	}
	err := s.n.cfg.Journal.Append(rec)
	s.mu.Unlock()
	return err
}

// dirty reports whether a range has had transactions here since its
// redundancy bound: only those need a durability round.
func (s *store) dirty(r Range) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i].key.Token >= r.Lo })
	for ; i < len(s.sorted) && s.sorted[i].key.Token <= r.Hi; i++ {
		f := s.sorted[i]
		// Any timestamp above the bound, even with no transaction left in
		// the index (truncated ones still need a round to be forgotten).
		if r.contains(f.key) && boundFor(s.redundant, f.key).Less(f.maxTS) {
			return true
		}
	}
	return false
}

// adoptRedundant records redundancy bounds another replica of the ranges
// has (a SetShardDurable this replica missed: the sync point applied here
// too), and wakes the commands waiting on a transaction below them; only
// for a transaction this replica has not seen decided (it reports whether).
func (s *store) adoptRedundant(id TxnID, bs []bound) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A decided transaction still needs its outcome; an undecided one
	// below a universal sync point never committed (it would have been a
	// dependency the sync point applied here first), and is invalidated by
	// forgetLocked once the bound is known.
	if c := s.cmds[id]; c != nil && c.Status.decided() {
		return false
	}
	// Journaled as markRedundant does (the bound's sync point, applied and
	// redundant), ahead of the deletions forgetLocked queues: a restart
	// restores the bound before anything below it is gone.
	for _, b := range bs {
		if !slices.ContainsFunc(s.redundant, func(o bound) bool { return o.R == b.R && !o.TS.Less(b.TS) }) {
			if err := s.markRedundantLocked2(TxnID{Timestamp: b.TS, Kind: SyncPoint}, []Range{b.R}); err != nil {
				return false
			}
		}
	}
	s.forgetLocked()
	if c := s.cmds[id]; c != nil && !c.Status.decided() && c.Status != Invalidated && c.Status != Truncated && len(s.n.owned(c.Keys, c.ID.Epoch)) == 0 {
		// Known here without owning its keys (seen as a dependency or
		// through a recovery): nothing to keep; it either never committed
		// or is durable at its replicas.
		c.Status = Truncated
		_ = s.saveLocked(c)
	}
	for _, w := range s.waiters[id] {
		if wc := s.cmds[w]; wc != nil {
			s.maybeExecuteLocked(wc)
		}
	}
	return true
}
