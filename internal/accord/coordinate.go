package accord

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// errPreempted: another coordinator (a recovery) took over the
// transaction; its outcome must be awaited.
var errPreempted = errors.New("accord: preempted")

// Coordinate runs a transaction and returns its result
// (CoordinateTransaction).
func (n *Node) Coordinate(ctx context.Context, txn *Txn) ([]byte, error) {
	if len(txn.Keys) == 0 {
		return nil, errors.New("accord: a transaction needs keys")
	}
	id := n.newTxnID(txn.Kind)
	res, err := n.coordinate(ctx, id, txn)
	if errors.Is(err, errPreempted) {
		return n.awaitOutcome(ctx, id, txn.Keys)
	}
	return res, err
}

// coordinate runs PreAccept, then the fast path or Propose.
func (n *Node) coordinate(ctx context.Context, id TxnID, txn *Txn) ([]byte, error) {
	ts := n.topologies(txn.Keys, id.Epoch, id.Epoch)
	if len(ts.Shards) == 0 {
		return nil, errors.New("accord: no topology")
	}
	req := &preAcceptReq{ID: id, Txn: txn}
	ch := broadcast[preAcceptReply](ctx, n, ts.nodes(), mPreAccept, func(NodeID) any { return req })
	slow := newTracker(ts, (*Shard).slowQuorum)
	fast := newTracker(ts, (*Shard).fastQuorum)
	noFast := newTracker(ts, func(s *Shard) int { return len(s.Nodes) - s.fastQuorum() + 1 })
	oks := map[NodeID]*preAcceptReply{}
	var grace <-chan time.Time
	for pending := len(ts.nodes()); pending > 0; {
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
		case r.reply.Outcome != ok:
			return nil, errPreempted
		default:
			oks[r.from] = &r.reply
			slow.success(r.from)
			if r.reply.FastVote {
				fast.success(r.from)
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
	if !slow.done() {
		return nil, ErrTimeout
	}
	if fast.done() {
		// The fast path: execute at the TxnID with the dependencies of the
		// fast votes.
		deps := Deps{}
		for _, r := range oks {
			if r.FastVote {
				deps = deps.with(r.Deps)
			}
		}
		return n.execute(ctx, Ballot{}, id, txn, id.Timestamp, deps)
	}
	executeAt := id.Timestamp
	deps := Deps{}
	for _, r := range oks {
		executeAt = maxTimestamp(executeAt, r.ExecuteAt)
		deps = deps.with(r.Deps)
	}
	return n.propose(ctx, Ballot{}, id, txn, executeAt, deps)
}

// propose runs the Accept round (Propose), then executes.
func (n *Node) propose(ctx context.Context, ballot Ballot, id TxnID, txn *Txn, executeAt Timestamp, deps Deps) ([]byte, error) {
	n.clock.Observe(executeAt.HLC)
	ts := n.topologies(txn.Keys, id.Epoch, executeAt.Epoch)
	req := &acceptReq{ID: id, Ballot: ballot, ExecuteAt: executeAt, Deps: deps, Txn: txn, Keys: txn.Keys}
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
	return n.execute(ctx, ballot, id, txn, executeAt, merged)
}

// execute commits a decision to a quorum, marks it stable and reads
// (Stabilise, then ExecuteTxn), computes the writes and result and applies
// them (Persist).
func (n *Node) execute(ctx context.Context, ballot Ballot, id TxnID, txn *Txn, executeAt Timestamp, deps Deps) ([]byte, error) {
	if txn == nil {
		return nil, errors.New("accord: executing a transaction without its definition")
	}
	ts := n.topologies(txn.Keys, id.Epoch, executeAt.Epoch)
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
		order := slices.Clone(s.Nodes)
		if i := slices.Index(order, n.cfg.ID); i > 0 {
			order[0], order[i] = order[i], order[0]
		}
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
				reads[k] = v
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
	return result, n.persist(ctx, ts, id, txn, executeAt, deps, writes, result)
}

// persist applies the outcome at every replica, returning once a quorum
// of every shard has applied it; the rest continues in the background, and
// once every replica has applied it they are told it is durable.
func (n *Node) persist(ctx context.Context, ts *Topologies, id TxnID, txn *Txn, executeAt Timestamp, deps Deps, writes Writes, result []byte) error {
	req := &applyReq{ID: id, ExecuteAt: executeAt, Deps: deps, Txn: txn, Writes: writes, Result: result}
	nodes := ts.nodes()
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
	go func() {
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
		for _, to := range ts.nodes() {
			var rep checkStatusReply
			if err := n.send(ctx, to, mCheckStatus, &checkStatusReq{ID: id}, &rep); err != nil {
				continue
			}
			switch rep.Status {
			case Applied, PreApplied:
				return rep.Result, nil
			case Invalidated:
				return nil, ErrInvalidated
			}
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
	ballot := Ballot{Epoch: n.epoch(), HLC: n.clock.Next(0), Node: n.cfg.ID}
	ts := n.topologies(keys, id.Epoch, n.epoch())
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
		return n.persist(ctx, ts, id, txn, best.ExecuteAt, best.Deps, best.Writes, best.Result)
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
		_, err := n.propose(ctx, ballot, id, txn, best.ExecuteAt, deps)
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
	for _, s := range ts.Shards {
		votesAgainst := 0
		for _, node := range s.Nodes {
			if r := oks[node]; r != nil && !r.FastVote {
				votesAgainst++
			}
		}
		if votesAgainst > len(s.Nodes)-s.fastQuorum() {
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
	_, err := n.propose(ctx, ballot, id, txn, id.Timestamp, deps)
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
			case now.Sub(c.lastChange) > after && (c.Status != NotDefined || !c.Promised.IsZero()) && len(c.Keys) > 0:
				todo = append(todo, stalled{c.ID, c.Txn, c.Keys})
				c.lastChange = now
			}
		}
		n.s.truncateLocked()
		n.s.mu.Unlock()
		for _, s := range todo {
			if _, busy := n.recovering.LoadOrStore(s.id, true); busy {
				continue
			}
			go func() {
				defer n.recovering.Delete(s.id)
				ctx, cancel := context.WithTimeout(context.Background(), 4*n.cfg.RecoverAfter)
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
	for _, to := range ts.nodes() {
		if to == n.cfg.ID {
			continue
		}
		var rep checkStatusReply
		if err := n.send(ctx, to, mCheckStatus, &checkStatusReq{ID: id}, &rep); err != nil {
			continue
		}
		switch {
		case rep.Status == Invalidated:
			_, err := n.s.commit(ctx, &commitReq{ID: id, Keys: rep.Keys, Invalidate: true})
			return err == nil
		case (rep.Status == Applied || rep.Status == PreApplied) && rep.Txn != nil:
			go func() {
				actx, cancel := context.WithTimeout(context.Background(), 4*n.cfg.RecoverAfter)
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
	return false
}

func (n *Node) String() string { return fmt.Sprintf("accord node %s", n.cfg.ID) }
