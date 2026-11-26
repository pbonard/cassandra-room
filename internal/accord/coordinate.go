package accord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// errPreempted: another coordinator (a recovery) took over the
// transaction; its outcome must be awaited. errStale: a replica refused the
// transaction's epoch.
var (
	errPreempted = errors.New("accord: preempted")
	errStale     = errors.New("accord: stale epoch")
	errRefused   = errors.New("accord: refused by a quorum")
)

// Coordinate runs a transaction and returns its result
// (CoordinateTransaction). Read-only transactions are ephemeral reads
// unless disabled.
func (n *Node) Coordinate(ctx context.Context, txn *Txn) ([]byte, error) {
	if len(txn.Keys) == 0 {
		return nil, errors.New("accord: a transaction needs keys")
	}
	if txn.Kind == Read && len(txn.Keys) == 1 && !n.cfg.NoEphemeralReads {
		return n.ephemeralRead(ctx, txn)
	}
	for {
		id := n.newTxnID(txn.Kind, txn.Keys...)
		res, err := n.coordinate(ctx, id, txn)
		switch {
		case errors.Is(err, errRefused):
			// A quorum of a shard refused it for good: it never commits.
			n.metrics.stale.Add(1)
			select {
			case <-ctx.Done():
				return nil, ErrTimeout
			case <-time.After(10 * time.Millisecond):
			}
			continue
		case errors.Is(err, errStale):
			n.metrics.stale.Add(1)
			// Make sure the stale attempt never executes, then retry in
			// the newer epoch.
			_ = n.recover(ctx, id, txn, txn.Keys)
			res, err = n.awaitOutcome(ctx, id, txn.Keys)
			if errors.Is(err, ErrInvalidated) {
				select {
				case <-ctx.Done():
					return nil, ErrTimeout
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
		case errors.Is(err, errPreempted):
			n.metrics.preempted.Add(1)
			return n.awaitOutcome(ctx, id, txn.Keys)
		case errors.Is(err, ErrTimeout):
			n.metrics.timeouts.Add(1)
		}
		return res, err
	}
}

// coordinate runs PreAccept, then the fast path or Propose.
func (n *Node) coordinate(ctx context.Context, id TxnID, txn *Txn) ([]byte, error) {
	ts := n.phaseTopologies(txn.Keys, id.Epoch, id.Epoch)
	if len(ts.Shards) == 0 {
		return nil, errors.New("accord: no topology")
	}
	fq := n.fastQuorumOf(id)
	req := &preAcceptReq{ID: id, Txn: txn}
	slow := newTracker(ts, (*Shard).slowQuorum)
	fast := newTracker(ts, fq)
	medium := newTracker(ts, (*Shard).slowQuorum)
	noFast := newTracker(ts, func(s *Shard) int { return len(s.Nodes) - fq(s) + 1 })
	oks := map[NodeID]*preAcceptReply{}
	refused := newTracker(ts, (*Shard).slowQuorum)
	sawStale := false
	targets := ts.nodes()
	if id.Privileged {
		// The privileged coordinator votes first, locally.
		r, err := n.s.preAccept(req)
		if err != nil {
			return nil, err
		}
		if r.Outcome != ok {
			return nil, errPreempted
		}
		oks[n.cfg.ID] = r
		slow.success(n.cfg.ID)
		if r.FastVote {
			fast.success(n.cfg.ID)
			medium.success(n.cfg.ID)
		} else {
			noFast.success(n.cfg.ID)
		}
		targets = slices.DeleteFunc(slices.Clone(targets), func(x NodeID) bool { return x == n.cfg.ID })
	}
	ch := broadcast[preAcceptReply](ctx, n, targets, mPreAccept, func(NodeID) any { return req })
	var grace <-chan time.Time
	for pending := len(targets); pending > 0 && !fast.done(); {
		var r response[preAcceptReply]
		select {
		case r = <-ch:
		case <-grace:
			pending = 0
			continue
		case <-ctx.Done():
			return nil, ErrTimeout
		}
		pending--
		switch {
		case r.err != nil:
			slow.failure(r.from)
			fast.failure(r.from)
			noFast.success(r.from)
		case r.reply.Outcome == stale:
			// Refused (a sync point's barrier, or a synchronised epoch). A
			// quorum of a shard refusing makes it certain the transaction
			// never commits: it can be retried at once.
			refused.success(r.from)
			if refused.anyDone() {
				return nil, errRefused
			}
			sawStale = true
		case r.reply.Outcome != ok:
			return nil, errPreempted
		default:
			oks[r.from] = &r.reply
			slow.success(r.from)
			if r.reply.FastVote {
				fast.success(r.from)
				medium.success(r.from)
			} else {
				noFast.success(r.from)
			}
		}
		if slow.impossible() {
			return nil, ErrTimeout
		}
		if fast.done() {
			break
		}
		// With a slow quorum, wait a little for a fast one unless it can
		// no longer form.
		if slow.done() && noFast.done() {
			break
		}
		if slow.done() && grace == nil {
			grace = time.After(20 * time.Millisecond)
		}
	}
	if sawStale {
		return nil, errStale
	}
	if !slow.done() {
		return nil, ErrTimeout
	}
	fastDeps := func() Deps {
		deps := Deps{}
		for _, r := range oks {
			if r.FastVote {
				deps = deps.with(r.Deps)
			}
		}
		return deps
	}
	if fast.done() {
		// The fast path: execute at the TxnID with the dependencies of the
		// fast votes. A privileged coordinator decides it locally, unless a
		// recovery got there first.
		deps := fastDeps()
		if id.Privileged && !n.s.fastCommit(id, txn, deps) {
			return nil, errPreempted
		}
		n.metrics.fastPath.Add(1)
		return n.execute(ctx, Ballot{}, id, txn, id.Timestamp, deps)
	}
	if medium.done() && !id.Privileged {
		// The medium path: a slow quorum of every shard voted for the
		// TxnID; one Accept round of that proposal, without new
		// dependencies, commits it. Not with the privileged coordinator's
		// quorums (cassandra-accord fc4a769: not correctly implemented with
		// coordinator optimisation quorums).
		n.metrics.mediumPath.Add(1)
		return n.propose(ctx, Ballot{}, id, txn, id.Timestamp, fastDeps(), true)
	}
	executeAt := id.Timestamp
	deps := Deps{}
	for _, r := range oks {
		executeAt = mergeMax(executeAt, r.ExecuteAt)
		deps = deps.with(r.Deps)
	}
	n.metrics.slowPath.Add(1)
	return n.propose(ctx, Ballot{}, id, txn, executeAt, deps, false)
}

// fastQuorumOf returns the fast quorum size of each shard for a
// transaction: privileged where its coordinator is a replica.
func (n *Node) fastQuorumOf(id TxnID) func(*Shard) int {
	return func(s *Shard) int {
		if id.Privileged && slices.Contains(s.Nodes, id.Node) {
			return s.privilegedFastQuorum()
		}
		return s.fastQuorum()
	}
}

// propose runs the Accept round (Propose), then executes.
func (n *Node) propose(ctx context.Context, ballot Ballot, id TxnID, txn *Txn, executeAt Timestamp, deps Deps, medium bool) ([]byte, error) {
	n.clock.Observe(executeAt.HLC)
	// A replica may have proposed an epoch this node does not know yet:
	// its replicas would be missed.
	if err := n.awaitEpoch(ctx, executeAt.Epoch); err != nil {
		return nil, err
	}
	ts := n.phaseTopologies(txn.Keys, id.Epoch, executeAt.Epoch)
	req := &acceptReq{ID: id, Ballot: ballot, ExecuteAt: executeAt, Deps: deps, Txn: txn, Keys: txn.Keys, NoDeps: medium}
	ch := broadcast[acceptReply](ctx, n, ts.nodes(), mAccept, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	merged := Deps{}
	for pending := len(ts.nodes()); pending > 0 && !tr.done(); pending-- {
		var r response[acceptReply]
		select {
		case r = <-ch:
		case <-ctx.Done():
			return nil, ErrTimeout
		}
		switch {
		case r.err != nil:
			tr.failure(r.from)
		case r.reply.Outcome == stale && ballot.IsZero():
			return nil, errStale
		case r.reply.Outcome != ok:
			return nil, errPreempted
		default:
			tr.success(r.from)
			merged = merged.with(r.reply.Deps)
		}
		if tr.impossible() {
			return nil, ErrTimeout
		}
	}
	if !tr.done() {
		return nil, ErrTimeout
	}
	if medium {
		merged = deps
	}
	return n.execute(ctx, ballot, id, txn, executeAt, merged)
}

// execute commits a decision to a quorum, marks it stable and reads
// (Stabilise, then ExecuteTxn), computes the writes and result and applies
// them (Persist).
func (n *Node) execute(ctx context.Context, ballot Ballot, id TxnID, txn *Txn, executeAt Timestamp, deps Deps) ([]byte, error) {
	if txn == nil {
		return nil, errors.New("accord: executing a transaction without its definition")
	}
	if err := n.awaitEpoch(ctx, executeAt.Epoch); err != nil {
		return nil, err
	}
	ts := n.phaseTopologies(txn.Keys, id.Epoch, executeAt.Epoch)
	commit := func(stable bool) *commitReq {
		return &commitReq{ID: id, Ballot: ballot, ExecuteAt: executeAt, Deps: deps, Txn: txn, Keys: txn.Keys, Stable: stable}
	}
	// Committed at a quorum of every shard first, so any recovery finds the
	// decision (and its dependencies) before a replica executes it.
	if err := n.quorum(ctx, ts, mCommit, commit(false)); err != nil {
		return nil, err
	}
	// Stable everywhere; read from one replica per shard.
	stable := commit(true)
	reads := map[string][]byte{}
	readers := map[NodeID]bool{}
	var applied *commitReply
	// A reply's reads count only for keys its replica reads for (a
	// replica of several shards answers them all at once).
	readsFor := map[string][]NodeID{}
	for _, s := range ts.latest() {
		for _, k := range s.Keys {
			readsFor[keyString(k)] = s.Nodes
		}
	}
	for _, s := range ts.latest() {
		covered := true
		for _, k := range s.Keys {
			if _, ok := reads[keyString(k)]; !ok {
				covered = false
			}
		}
		if covered {
			continue
		}
		order := n.readOrder(s.Nodes)
		done := false
		for _, to := range order {
			req := *stable
			req.Read = true
			var rep commitReply
			if err := n.send(ctx, to, mCommit, &req, &rep); err != nil || rep.Outcome != ok {
				if ctx.Err() != nil {
					return nil, ErrTimeout
				}
				continue
			}
			readers[to] = true
			if rep.Applied {
				applied = &rep
			}
			for k, v := range rep.Reads {
				if slices.Contains(readsFor[k], to) {
					reads[k] = v
				}
			}
			done = true
			break
		}
		if !done {
			return nil, ErrTimeout
		}
	}
	for _, to := range ts.nodes() {
		if !readers[to] {
			go func() { _ = n.send(context.Background(), to, mCommit, stable, nil) }()
		}
	}
	var writes Writes
	var result []byte
	if applied != nil {
		// A replica already applied it (a recovery completed it first).
		writes, result = applied.Writes, applied.Result
	} else {
		var err error
		writes, result, err = n.cfg.Agent.Execute(txn, executeAt, reads)
		if err != nil {
			return nil, err
		}
	}
	ats, err := n.applyTopologies(ctx, txn.Keys, id, executeAt)
	if err != nil {
		return nil, err
	}
	return result, n.persist(ctx, ats, id, txn, executeAt, deps, writes, result)
}

// applyTopologies are the replicas a transaction's writes go to: those of
// every epoch up to the current one, and pending replicas, so that nodes
// joining since it was decided receive them too.
func (n *Node) applyTopologies(ctx context.Context, keys []Key, id TxnID, executeAt Timestamp) (*Topologies, error) {
	if err := n.awaitEpoch(ctx, executeAt.Epoch); err != nil {
		return nil, err
	}
	return n.phaseTopologies(keys, id.Epoch, max(executeAt.Epoch, n.epoch())), nil
}

// persist applies the outcome at every replica, returning once a quorum
// of every shard has applied it; the rest continues in the background, and
// once every replica has applied it they are told it is durable.
func (n *Node) persist(ctx context.Context, ts *Topologies, id TxnID, txn *Txn, executeAt Timestamp, deps Deps, writes Writes, result []byte) error {
	req := &applyReq{ID: id, ExecuteAt: executeAt, Deps: deps, Txn: txn, Writes: writes, Result: result}
	nodes := ts.applyNodes()
	ch := broadcast[applyReply](context.Background(), n, nodes, mApply, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	got := 0
	for ; got < len(nodes) && !tr.done(); got++ {
		var r response[applyReply]
		select {
		case r = <-ch:
		case <-ctx.Done():
			return ErrTimeout
		}
		if r.err == nil && r.reply.Outcome == ok {
			tr.success(r.from)
		} else {
			tr.failure(r.from)
		}
	}
	if !tr.done() {
		return ErrTimeout
	}
	select {
	case n.persisting <- struct{}{}:
	default:
		// Too many transactions are retrying (a replica is down): leave
		// the rest to the replicas' own progress (fetching what they miss)
		// and to durability rounds.
		n.metrics.persistShed.Add(1)
		return nil
	}
	go func() {
		defer func() { <-n.persisting }()
		// Every replica must apply it: retry those that failed (a replica
		// that was down learns the outcome when it is back), then tell
		// them all it is durable.
		applied := map[NodeID]bool{}
		for i := range tr.shards {
			for node := range tr.ok[i] {
				applied[node] = true
			}
		}
		for ; got < len(nodes); got++ {
			if r := <-ch; r.err == nil && r.reply.Outcome == ok {
				applied[r.from] = true
			}
		}
		backoff := 100 * time.Millisecond
		for attempt := 0; len(applied) < len(nodes) && attempt < 40; attempt++ {
			select {
			case <-n.stop:
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, 2*time.Second)
			for _, to := range nodes {
				if !applied[to] {
					var rep applyReply
					if err := n.send(context.Background(), to, mApply, req, &rep); err == nil && rep.Outcome == ok {
						applied[to] = true
					}
				}
			}
		}
		if len(applied) == len(nodes) {
			for _, to := range nodes {
				_ = n.send(context.Background(), to, mInformDurable, &checkStatusReq{ID: id}, nil)
			}
		}
	}()
	return nil
}

// quorum sends a request to every replica and waits for a quorum of every
// shard to accept it.
func (n *Node) quorum(ctx context.Context, ts *Topologies, t msgType, req any) error {
	nodes := ts.nodes()
	ch := broadcast[commitReply](ctx, n, nodes, t, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	for i := 0; i < len(nodes) && !tr.done(); i++ {
		var r response[commitReply]
		select {
		case r = <-ch:
		case <-ctx.Done():
			return ErrTimeout
		}
		if r.err == nil && r.reply.Outcome == ok {
			tr.success(r.from)
		} else {
			tr.failure(r.from)
		}
		if tr.impossible() {
			return ErrTimeout
		}
	}
	if !tr.done() {
		return ErrTimeout
	}
	return nil
}

// awaitOutcome waits for a transaction another coordinator completes,
// asking its replicas.
func (n *Node) awaitOutcome(ctx context.Context, id TxnID, keys []Key) ([]byte, error) {
	for {
		ts := n.topologies(keys, id.Epoch, n.epoch())
		truncated, answered := 0, 0
		for _, to := range ts.nodes() {
			var rep checkStatusReply
			if err := n.send(ctx, to, mCheckStatus, &checkStatusReq{ID: id, Keys: keys}, &rep); err != nil {
				continue
			}
			answered++
			switch rep.Status {
			case Applied, PreApplied:
				if rep.Txn == nil {
					continue
				}
				// The outcome is decided, but this replica may be the only
				// one to have it: apply it at a slow quorum of each shard
				// first, as persist does for the coordinator's own
				// transactions, so that a QUORUM read outside Accord
				// right after the answer sees it (on tables whose
				// conditional statements or writes run on Accord and
				// reads do not).
				ats, err := n.applyTopologies(ctx, keys, id, rep.ExecuteAt)
				if err == nil {
					err = n.persist(ctx, ats, id, rep.Txn, rep.ExecuteAt, rep.Deps, rep.Writes, rep.Result)
				}
				if err != nil {
					return nil, err
				}
				return rep.Result, nil
			case Invalidated:
				return nil, ErrInvalidated
			case Truncated:
				// Erased here: applied everywhere, or below the redundancy
				// bound. Another replica may still hold the result.
				truncated++
			}
		}
		if answered > 0 && truncated == answered {
			return nil, ErrTimeout // gone from every replica that answers
		}
		select {
		case <-ctx.Done():
			return nil, ErrTimeout
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// ---- recovery ----

// recover completes or invalidates a transaction whose coordinator may
// have failed (Recover). txn may be nil when only its keys are known.
func (n *Node) recover(ctx context.Context, id TxnID, txn *Txn, keys []Key) error {
	if txn != nil {
		keys = txn.Keys
	}
	n.metrics.recoveries.Add(1)
	ballot := Ballot{Epoch: n.epoch(), HLC: n.clock.Next(0), Node: n.cfg.ID}
	ts := n.phaseTopologies(keys, id.Epoch, n.epoch())
	if len(ts.Shards) == 0 {
		return errors.New("accord: no topology")
	}
	req := &recoverReq{ID: id, Ballot: ballot, Txn: txn, Keys: keys}
	ch := broadcast[recoverReply](ctx, n, ts.nodes(), mRecover, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	oks := map[NodeID]*recoverReply{}
	for i := 0; i < len(ts.nodes()) && !tr.done(); i++ {
		var r response[recoverReply]
		select {
		case r = <-ch:
		case <-ctx.Done():
			return ErrTimeout
		}
		switch {
		case r.err != nil:
			tr.failure(r.from)
		case r.reply.Outcome != ok:
			return errPreempted
		default:
			tr.success(r.from)
			oks[r.from] = &r.reply
			if txn == nil && r.reply.Txn != nil {
				txn = r.reply.Txn
			}
		}
		if tr.impossible() {
			return ErrTimeout
		}
	}
	if !tr.done() {
		return ErrTimeout
	}
	// The most advanced state found (by phase, then ballot).
	var best *recoverReply
	for _, r := range oks {
		if r.Status == Truncated {
			return nil // applied at every replica
		}
		if best == nil || phase(r) > phase(best) || phase(r) == phase(best) && best.Accepted.Less(r.Accepted) {
			best = r
		}
	}
	switch {
	case best.Status == Invalidated:
		return n.commitInvalidate(ts, id, keys)
	case best.Status == Applied || best.Status == PreApplied:
		ats, err := n.applyTopologies(ctx, keys, id, best.ExecuteAt)
		if err != nil {
			return err
		}
		return n.persist(ctx, ats, id, txn, best.ExecuteAt, best.Deps, best.Writes, best.Result)
	case best.Status == Stable || best.Status == Committed:
		_, err := n.execute(ctx, ballot, id, txn, best.ExecuteAt, best.Deps)
		return err
	case best.Status == Accepted:
		// The latest proposal's dependencies, with what the replicas
		// witnessed (LatestDeps).
		deps := best.Deps.with(nil)
		for _, r := range oks {
			deps = deps.with(r.LocalDeps)
		}
		_, err := n.propose(ctx, ballot, id, txn, best.ExecuteAt, deps, false)
		return err
	case best.Status == AcceptedInvalidate || txn == nil:
		return n.invalidate(ctx, ballot, ts, id, keys)
	}
	// Nothing accepted: was the fast path possible?
	rejects := false
	var wait []TxnID
	for _, r := range oks {
		rejects = rejects || r.Rejects
		for _, w := range r.Wait {
			if !slices.Contains(wait, w) {
				wait = append(wait, w)
			}
		}
	}
	if r := oks[id.Node]; id.Privileged && r != nil {
		// The privileged coordinator answered without having decided: it
		// promised our ballot, so it can no longer take the fast path.
		rejects = true
	}
	fq := n.fastQuorumOf(id)
	for _, s := range ts.Shards {
		if s.Epoch > id.Epoch {
			// Only the epochs it was pre-accepted in decide whether a fast
			// quorum could have formed: replicas of later epochs never
			// voted.
			continue
		}
		votesAgainst := 0
		for _, node := range s.Nodes {
			if r := oks[node]; r != nil && !r.FastVote {
				votesAgainst++
			}
		}
		// A privileged coordinator outside the recovery quorum is assumed
		// to have voted for the fast path.
		if votesAgainst > len(s.Nodes)-fq(s) {
			rejects = true // no fast quorum can have formed in this shard
		}
	}
	if rejects {
		return n.invalidate(ctx, ballot, ts, id, keys)
	}
	if len(wait) > 0 {
		// Earlier transactions that may execute after this one have not
		// decided their dependencies: wait for them, then start again.
		for _, w := range wait {
			if err := n.awaitDecided(ctx, w, keys); err != nil {
				return err
			}
		}
		return n.recover(ctx, id, txn, keys)
	}
	deps := Deps{}
	for _, r := range oks {
		deps = deps.with(r.LocalDeps)
	}
	_, err := n.propose(ctx, ballot, id, txn, id.Timestamp, deps, false)
	return err
}

// phase orders recovery replies: decided states first, then accepted.
func phase(r *recoverReply) int {
	switch r.Status {
	case Invalidated:
		return 6
	case Applied, PreApplied:
		return 5
	case Stable:
		return 4
	case Committed:
		return 3
	case Accepted, AcceptedInvalidate:
		return 2
	}
	return 1
}

// invalidate makes sure a transaction never executes (Invalidate).
func (n *Node) invalidate(ctx context.Context, ballot Ballot, ts *Topologies, id TxnID, keys []Key) error {
	n.metrics.invalidations.Add(1)
	req := &acceptReq{ID: id, Ballot: ballot, Keys: keys, Invalidate: true}
	ch := broadcast[acceptReply](ctx, n, ts.nodes(), mAccept, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	for i := 0; i < len(ts.nodes()) && !tr.done(); i++ {
		var r response[acceptReply]
		select {
		case r = <-ch:
		case <-ctx.Done():
			return ErrTimeout
		}
		switch {
		case r.err != nil:
			tr.failure(r.from)
		case r.reply.Outcome != ok:
			return errPreempted
		default:
			tr.success(r.from)
		}
		if tr.impossible() {
			return ErrTimeout
		}
	}
	if !tr.done() {
		return ErrTimeout
	}
	return n.commitInvalidate(ts, id, keys)
}

func (n *Node) commitInvalidate(ts *Topologies, id TxnID, keys []Key) error {
	req := &commitReq{ID: id, Keys: keys, Invalidate: true}
	for _, to := range ts.nodes() {
		go func() { _ = n.send(context.Background(), to, mCommit, req, nil) }()
	}
	return nil
}

// awaitDecided waits until a transaction's execution timestamp is decided
// (or it is invalidated), recovering it if it stalls.
func (n *Node) awaitDecided(ctx context.Context, id TxnID, keys []Key) error {
	start := n.cfg.Now()
	for {
		ts := n.topologies(keys, id.Epoch, n.epoch())
		var txn *Txn
		var known []Key
		for _, to := range ts.nodes() {
			var rep checkStatusReply
			if err := n.send(ctx, to, mCheckStatus, &checkStatusReq{ID: id}, &rep); err != nil {
				continue
			}
			if rep.Status.decided() || rep.Status == Invalidated {
				return nil
			}
			if rep.Txn != nil {
				txn = rep.Txn
			}
			if len(rep.Keys) > 0 {
				known = rep.Keys
			}
		}
		if n.cfg.Now().Sub(start) > n.cfg.RecoverAfter {
			if known == nil {
				known = keys
			}
			if err := n.recover(ctx, id, txn, known); err != nil && !errors.Is(err, errPreempted) {
				return err
			}
			start = n.cfg.Now()
		}
		select {
		case <-ctx.Done():
			return ErrTimeout
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// ---- progress log ----

// progressLog looks for stalled transactions (SimpleProgressLog): commands
// not decided long after they were last changed (their coordinator may have
// failed), and stable commands waiting long on a dependency or for their
// writes. It fetches what other replicas know, and recovers what nobody
// completes.
func (n *Node) progressLog() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.ProgressInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-t.C:
		}
		type stalled struct {
			id   TxnID
			txn  *Txn
			keys []Key
		}
		var todo []stalled
		now := n.cfg.Now()
		after := jitter(n.cfg.RecoverAfter)
		n.s.mu.Lock()
		for _, c := range n.s.cmds {
			switch {
			case c.Status == Applied || c.Status == Invalidated || c.Status == Truncated:
			case c.ID.Kind == SyncPoint && c.Status < PreApplied:
				// Never recovered: one its coordinator abandoned is
				// invalidated here after a while (its barrier stays).
				if now.Sub(c.lastChange) > 10*n.cfg.RecoverAfter {
					c.Status = Invalidated
					_ = n.s.saveLocked(c)
				}
			case c.blockedOn != nil && now.Sub(c.blockedSince) > after:
				dep := n.s.cmds[*c.blockedOn]
				var txn *Txn
				keys := c.owned
				if dep != nil {
					txn = dep.Txn
					if len(dep.Keys) > 0 {
						keys = dep.Keys
					}
				}
				todo = append(todo, stalled{*c.blockedOn, txn, keys})
				c.blockedSince = now
			case now.Sub(c.lastChange) > n.homeDelay(c.Keys, after)<<min(c.recoveries, 4) && (c.Status != NotDefined || !c.Promised.IsZero()) && len(c.Keys) > 0:
				todo = append(todo, stalled{c.ID, c.Txn, c.Keys})
				c.lastChange = now
				c.recoveries++
			}
		}
		n.s.truncateLocked()
		n.s.mu.Unlock()
		if n.cfg.DurabilityRanges != nil && now.Sub(n.lastDurable) > jitter(n.cfg.DurabilityInterval) {
			n.lastDurable = now
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				n.durability()
			}()
		}
		if n.synced.Load() < n.epoch() && n.cfg.IsLeader != nil && n.cfg.IsLeader() && n.syncing.CompareAndSwap(false, true) {
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				defer n.syncing.Store(false)
				n.syncEpochs()
			}()
		} else if e := n.synced.Load(); e > 0 && e == n.epoch() && n.cfg.IsLeader != nil && n.cfg.IsLeader() && now.Sub(n.lastSyncedSent) > jitter(n.cfg.DurabilityInterval) {
			// Re-sent now and then: a node that restarted without it (or
			// joined since) learns which epochs it may refuse.
			n.lastSyncedSent = now
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				n.broadcastSynced(e)
			}()
		}
		for _, s := range todo {
			if _, busy := n.recovering.LoadOrStore(s.id, true); busy {
				continue
			}
			go func() {
				defer n.recovering.Delete(s.id)
				ctx, cancel := context.WithTimeout(context.Background(), n.recoveryTimeout())
				defer cancel()
				if n.fetch(ctx, s.id, s.keys) {
					return
				}
				_ = n.recover(ctx, s.id, s.txn, s.keys)
			}()
		}
	}
}

// fetch learns a transaction's decision from other replicas and records it
// here (FetchData / Propagate); it reports whether one was found.
func (n *Node) fetch(ctx context.Context, id TxnID, keys []Key) bool {
	ts := n.topologies(keys, id.Epoch, n.epoch())
	var truncated []bound
	for _, to := range ts.nodes() {
		if to == n.cfg.ID {
			continue
		}
		var rep checkStatusReply
		if err := n.send(ctx, to, mCheckStatus, &checkStatusReq{ID: id, Keys: keys}, &rep); err != nil {
			continue
		}
		switch {
		case rep.Status == Truncated && len(rep.Redundant) > 0:
			// Forgotten there below a universal sync point; another replica
			// may still have the outcome, so it is only adopted after.
			truncated = append(truncated, rep.Redundant...)
		case rep.Status == Invalidated:
			_, err := n.s.commit(ctx, &commitReq{ID: id, Keys: rep.Keys, Invalidate: true})
			return err == nil
		case (rep.Status == Applied || rep.Status == PreApplied) && rep.Txn != nil:
			go func() {
				actx, cancel := context.WithTimeout(context.Background(), n.recoveryTimeout())
				defer cancel()
				_, _ = n.s.apply(actx, &applyReq{ID: id, ExecuteAt: rep.ExecuteAt, Deps: rep.Deps, Txn: rep.Txn, Writes: rep.Writes, Result: rep.Result})
			}()
			return true
		case rep.Status == Stable && rep.Txn != nil:
			// Decided and stable elsewhere: stable here too; the writes
			// still need a coordinator (the recovery that follows, unless
			// one is under way).
			_, err := n.s.commit(ctx, &commitReq{ID: id, Ballot: rep.Accepted, ExecuteAt: rep.ExecuteAt, Deps: rep.Deps, Txn: rep.Txn, Keys: rep.Keys, Stable: true})
			_ = err
		}
	}
	// Nobody has it any more, and it is below a universal sync point,
	// which applied here too: this replica missed the redundancy bound. A
	// transaction it knows (beyond a placeholder) still needs recovering.
	if len(truncated) > 0 && n.s.adoptRedundant(id, truncated) {
		return true
	}
	return false
}

func (n *Node) String() string { return fmt.Sprintf("accord node %s", n.cfg.ID) }

// ---- ephemeral reads ----

// ephemeralRead runs a read-only transaction without committing it
// (CoordinateEphemeralRead): a quorum of every shard reports the writes
// not yet applied everywhere; one replica per shard reads once those have
// applied there. Every write acknowledged before the read started is
// applied at a quorum, which the dependency quorum intersects, so the read
// sees it; nothing is journaled.
func (n *Node) ephemeralRead(ctx context.Context, txn *Txn) ([]byte, error) {
	n.metrics.ephemeralReads.Add(1)
	for {
		res, newer, err := n.ephemeralReadAt(ctx, txn, n.epoch())
		if newer == 0 {
			return res, err
		}
		// A replica knows a newer epoch: read again once this node does.
		if err := n.awaitEpoch(ctx, newer); err != nil {
			return nil, err
		}
	}
}

// awaitEpoch waits until this node knows an epoch, asking for it (from
// the node that knows it, if any) when it does not.
func (n *Node) awaitEpoch(ctx context.Context, e int64) error {
	return n.awaitEpochFrom(ctx, "", e)
}

func (n *Node) awaitEpochFrom(ctx context.Context, from NodeID, e int64) error {
	if n.epoch() >= e {
		return nil
	}
	if f := n.cfg.FetchEpoch; f != nil {
		f(from, e)
	}
	for n.epoch() < e {
		select {
		case <-ctx.Done():
			return ErrTimeout
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}

// ephemeralReadAt runs an ephemeral read in an epoch; it returns a newer
// epoch a replica knows, if any (and then nothing else).
func (n *Node) ephemeralReadAt(ctx context.Context, txn *Txn, epoch int64) ([]byte, int64, error) {
	ts := n.phaseTopologies(txn.Keys, epoch, epoch)
	if len(ts.Shards) == 0 {
		return nil, 0, errors.New("accord: no topology")
	}
	req := &ephemeralReq{Keys: txn.Keys}
	ch := broadcast[ephemeralReply](ctx, n, ts.nodes(), mEphemeralDeps, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	deps := Deps{}
	newer := int64(0)
	for i := 0; i < len(ts.nodes()) && !tr.done(); i++ {
		var r response[ephemeralReply]
		select {
		case r = <-ch:
		case <-ctx.Done():
			return nil, 0, ErrTimeout
		}
		if r.err != nil || r.reply.Outcome != ok {
			tr.failure(r.from)
		} else {
			tr.success(r.from)
			deps = deps.with(r.reply.Deps)
			if r.reply.Epoch > epoch {
				newer = max(newer, r.reply.Epoch)
			}
		}
		if tr.impossible() {
			return nil, 0, ErrTimeout
		}
	}
	if !tr.done() {
		return nil, 0, ErrTimeout
	}
	if newer > 0 {
		return nil, newer, nil
	}
	reads := map[string][]byte{}
	for _, s := range ts.latest() {
		order := n.readOrder(s.Nodes)
		done := false
		for _, to := range order {
			var rep ephemeralReply
			if err := n.send(ctx, to, mEphemeralRead, &ephemeralReq{Keys: s.Keys, Deps: deps, Txn: txn}, &rep); err != nil || rep.Outcome != ok {
				if ctx.Err() != nil {
					return nil, 0, ErrTimeout
				}
				continue
			}
			for k, v := range rep.Reads {
				reads[k] = v
			}
			done = true
			break
		}
		if !done {
			return nil, 0, ErrTimeout
		}
	}
	_, result, err := n.cfg.Agent.Execute(txn, Timestamp{Epoch: epoch, HLC: n.clock.Next(0), Node: n.cfg.ID}, reads)
	return result, 0, err
}

// ---- epoch synchronisation ----

// syncEpochs synchronises the epochs after the synchronised one, in order
// (the leader does): for each, a sync point over every range whose
// replicas changed from the epoch before, with quorums in both. Its
// barrier refuses any transaction of an earlier epoch it does not follow,
// so once it is applied no earlier-epoch transaction is in progress on
// those ranges, and every node is told the epoch is synchronised: phases
// stop including the earlier epochs.
func (n *Node) syncEpochs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-n.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	for {
		n.topoMu.RLock()
		synced, latest := n.synced.Load(), n.latest
		var prev, cur *Topology
		e := synced + 1
		for ; e <= latest && n.topos[e] == nil; e++ {
		}
		if e <= latest {
			cur, prev = n.topos[e], n.topos[e-1]
		}
		n.topoMu.RUnlock()
		if cur == nil {
			return
		}
		ranges := n.changedRanges(prev, cur)
		for _, r := range ranges {
			sctx, scancel := context.WithTimeout(ctx, 2*n.recoveryTimeout())
			_, err := n.SyncPoint(sctx, r, false)
			scancel()
			if err != nil {
				return // retried at the next opportunity
			}
		}
		n.metrics.epochSyncs.Add(1)
		for _, to := range cur.Nodes {
			_ = n.send(ctx, to, mEpochSynced, &epochSynced{Epoch: e}, nil)
		}
		n.advanceSynced(e)
	}
}

// changedRanges returns the parts of the epoch ranges whose replicas or
// pending replicas differ between two topologies (all of them without the
// earlier one): a range starting to move to a node must be synchronised
// too, or transactions of the epoch before, which do not reach the new
// node, could still be executing when its data is streamed.
func (n *Node) changedRanges(prev, cur *Topology) []Range {
	if n.cfg.EpochRanges == nil || cur.RangeReplicas == nil {
		return nil
	}
	var out []Range
	for _, r := range n.cfg.EpochRanges() {
		if prev == nil || prev.RangeReplicas == nil {
			out = append(out, r)
			continue
		}
		a, b := prev.RangeReplicas(r), cur.RangeReplicas(r)
		owner := func(shards []RangeShard, tok int64) string {
			for _, s := range shards {
				if tok >= s.Lo && tok <= s.Hi {
					nodes, pending := slices.Clone(s.Nodes), slices.Clone(s.Pending)
					slices.Sort(nodes)
					slices.Sort(pending)
					return strings.Join(nodes, ",") + "|" + strings.Join(pending, ",")
				}
			}
			return ""
		}
		// The boundaries of either split, walked in order.
		var starts []int64
		for _, s := range append(slices.Clone(a), b...) {
			starts = append(starts, s.Lo)
			if s.Hi < r.Hi {
				starts = append(starts, s.Hi+1)
			}
		}
		starts = append(starts, r.Lo)
		slices.Sort(starts)
		starts = slices.Compact(starts)
		for i, lo := range starts {
			if lo < r.Lo || lo > r.Hi {
				continue
			}
			hi := r.Hi
			if i+1 < len(starts) {
				hi = starts[i+1] - 1
			}
			if owner(a, lo) == owner(b, lo) {
				continue
			}
			if k := len(out); k > 0 && out[k-1].Prefix == r.Prefix && out[k-1].Hi+1 == lo {
				out[k-1].Hi = hi // merge adjacent
				continue
			}
			out = append(out, Range{Prefix: r.Prefix, Lo: lo, Hi: hi})
		}
	}
	return out
}

// epochSynced tells a node an epoch is synchronised.
type epochSynced struct{ Epoch int64 }

func (n *Node) advanceSynced(e int64) {
	for {
		cur := n.synced.Load()
		if e <= cur {
			return
		}
		if n.synced.CompareAndSwap(cur, e) {
			break
		}
	}
	if n.s != nil {
		_ = n.s.saveSynced()
	}
}

// broadcastSynced tells every node of the latest epoch that epoch e is
// synchronised.
func (n *Node) broadcastSynced(e int64) {
	n.topoMu.RLock()
	var nodes []NodeID
	if t := n.topos[n.latest]; t != nil {
		nodes = t.Nodes
	}
	n.topoMu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RequestTimeout)
	defer cancel()
	for _, to := range nodes {
		if to != n.cfg.ID {
			_ = n.send(ctx, to, mEpochSynced, &epochSynced{Epoch: e}, nil)
		}
	}
}

var syncedRecordID = TxnID{Timestamp: Timestamp{Node: "~synced"}, Kind: SyncPoint}

// saveSynced journals the node's synced epoch (one record, replaced), so a
// restarted node keeps refusing transactions of older epochs.
func (s *store) saveSynced() error {
	s.mu.Lock()
	e := s.n.synced.Load()
	if s.syncedRec != nil && s.syncedRec.SyncedEpoch >= e {
		s.mu.Unlock()
		return nil
	}
	rec := &Command{ID: syncedRecordID, SyncedEpoch: e}
	s.syncedRec = rec
	if s.gc == nil {
		err := s.n.cfg.Journal.Append(rec)
		s.mu.Unlock()
		return err
	}
	b, err := json.Marshal(rec)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.lastSeq = s.gc.enqueue(Record{ID: rec.ID, Data: b})
	return s.unlock()
}

// fetchOnce fetches a transaction's outcome, or recovers it, unless that
// is already under way.
func (n *Node) fetchOnce(id TxnID, keys []Key) {
	if _, busy := n.recovering.LoadOrStore(id, true); busy {
		return
	}
	defer n.recovering.Delete(id)
	ctx, cancel := context.WithTimeout(context.Background(), n.recoveryTimeout())
	defer cancel()
	if !n.fetch(ctx, id, keys) {
		_ = n.recover(ctx, id, nil, keys)
	}
}

// recoveryTimeout bounds a recovery: it may take several rounds of
// requests, and a stable transaction's execution.
func (n *Node) recoveryTimeout() time.Duration {
	return max(4*n.cfg.RecoverAfter, 8*n.cfg.RequestTimeout)
}

// ---- range reads ----

// RangeShards splits a range by the replica sets of the newest epoch.
func (n *Node) RangeShards(r Range) []RangeShard {
	n.topoMu.RLock()
	t := n.topos[n.latest]
	n.topoMu.RUnlock()
	if t == nil || t.RangeReplicas == nil {
		return nil
	}
	return t.RangeReplicas(r)
}

// ReadRange runs an ephemeral read of a range (TxnRangeRead, read
// ephemerally): a quorum of every replica set owning part of it, in every
// epoch not yet synchronised, reports the writes in the range not yet
// applied everywhere; then, per sub-range, one replica reads once those
// have applied there. The result maps each sub-range (Range.String) to
// what the DataStore read, in token order.
func (n *Node) ReadRange(ctx context.Context, r Range, txn *Txn) ([]RangeShard, map[string][]byte, error) {
	n.metrics.rangeReads.Add(1)
	for {
		shards, reads, newer, err := n.readRangeOnce(ctx, r, txn)
		if newer == 0 {
			return shards, reads, err
		}
		if err := n.awaitEpoch(ctx, newer); err != nil {
			return nil, nil, err
		}
	}
}

func (n *Node) readRangeOnce(ctx context.Context, r Range, txn *Txn) ([]RangeShard, map[string][]byte, int64, error) {
	latest := n.epoch()
	newer := int64(0)
	ts := &Topologies{}
	var readShards []RangeShard
	n.topoMu.RLock()
	for e := min(n.synced.Load(), latest); e <= latest; e++ {
		t := n.topos[e]
		if t == nil || t.RangeReplicas == nil {
			continue
		}
		for _, rs := range t.RangeReplicas(r) {
			nodes := slices.Clone(rs.Nodes)
			slices.Sort(nodes)
			ts.Shards = append(ts.Shards, &Shard{Epoch: e, Nodes: slices.Compact(nodes)})
			if e == latest {
				readShards = append(readShards, rs)
			}
		}
	}
	n.topoMu.RUnlock()
	if len(readShards) == 0 {
		return nil, nil, 0, errors.New("accord: no topology for the range")
	}
	req := &ephemeralReq{Ranges: []Range{r}}
	nodes := ts.nodes()
	ch := broadcast[ephemeralReply](ctx, n, nodes, mEphemeralDeps, func(NodeID) any { return req })
	tr := newTracker(ts, (*Shard).slowQuorum)
	deps := Deps{}
	for i := 0; i < len(nodes) && !tr.done(); i++ {
		var rep response[ephemeralReply]
		select {
		case rep = <-ch:
		case <-ctx.Done():
			return nil, nil, 0, ErrTimeout
		}
		if rep.err != nil || rep.reply.Outcome != ok {
			tr.failure(rep.from)
		} else {
			tr.success(rep.from)
			deps = deps.with(rep.reply.Deps)
			newer = max(newer, rep.reply.Epoch)
		}
		if tr.impossible() {
			return nil, nil, 0, ErrTimeout
		}
	}
	if !tr.done() {
		return nil, nil, 0, ErrTimeout
	}
	if newer > latest {
		return nil, nil, newer, nil
	}
	reads := map[string][]byte{}
	for _, rs := range readShards {
		order := n.readOrder(rs.Nodes)
		sub := Deps{}
		for ks, ids := range deps {
			if k, ok := parseKeyString(ks); ok && rs.Range.contains(k) {
				sub[ks] = ids
			}
		}
		done := false
		for _, to := range order {
			var rep ephemeralReply
			if err := n.send(ctx, to, mEphemeralRead, &ephemeralReq{Ranges: []Range{rs.Range}, Deps: sub, Txn: txn}, &rep); err != nil || rep.Outcome != ok {
				if ctx.Err() != nil {
					return nil, nil, 0, ErrTimeout
				}
				continue
			}
			reads[rs.Range.String()] = rep.Reads[rs.Range.String()]
			done = true
			break
		}
		if !done {
			return nil, nil, 0, ErrTimeout
		}
	}
	return readShards, reads, 0, nil
}

// readOrder orders replicas to read from: this node first, then the
// nearest (Config.Proximity), as cassandra-accord picks read candidates.
func (n *Node) readOrder(nodes []NodeID) []NodeID {
	order := slices.Clone(nodes)
	slices.Sort(order)
	rank := func(x NodeID) int {
		switch {
		case x == n.cfg.ID:
			return -1
		case n.cfg.Proximity != nil:
			return n.cfg.Proximity(x)
		}
		return 0
	}
	slices.SortStableFunc(order, func(a, b NodeID) int { return rank(a) - rank(b) })
	return order
}

// homeDelay is how long a stalled transaction waits before this replica
// recovers it: the replicas of its home key (the first) recover first
// (accord's home shard progress log); the others only if those fail.
func (n *Node) homeDelay(keys []Key, after time.Duration) time.Duration {
	if len(keys) == 0 || len(n.owned(keys[:1], 0)) > 0 {
		return after
	}
	return 3 * after
}
