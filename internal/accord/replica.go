package accord

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Status is a command's progress on a replica (accord.local.SaveStatus,
// simplified). The order matters: a command only moves forward.
type Status uint8

const (
	NotDefined Status = iota
	PreAccepted
	AcceptedInvalidate
	Accepted
	// Committed: the execution timestamp and dependencies are decided.
	Committed
	// Stable: the decision is committed at a quorum; the command may
	// execute once its earlier dependencies have applied.
	Stable
	// PreApplied: the writes are known, waiting for dependencies.
	PreApplied
	Applied
	Invalidated
	// Truncated: applied at every replica; the state was erased.
	Truncated
)

// forgotten marks a journal record of a forgotten command.
const forgotten Status = 255

var statusNames = [...]string{"NotDefined", "PreAccepted", "AcceptedInvalidate", "Accepted", "Committed", "Stable", "PreApplied", "Applied", "Invalidated", "Truncated"}

func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return "Forgotten"
}

// decided reports whether the execution timestamp is decided.
func (s Status) decided() bool { return s >= Committed && s != Invalidated }

// Command is a replica's durable state of a transaction
// (accord.local.Command).
type Command struct {
	ID       TxnID
	Status   Status
	Txn      *Txn  `json:",omitempty"`
	Keys     []Key `json:",omitempty"`
	Promised Ballot
	Accepted Ballot
	// ExecuteAt is the proposed (before Committed) or decided timestamp.
	ExecuteAt Timestamp
	// Deps are the proposed (Accepted) or decided (Committed and after)
	// dependencies; LocalDeps those this replica computed at PreAccept.
	Deps      Deps `json:",omitempty"`
	LocalDeps Deps `json:",omitempty"`
	// FastVote: this replica pre-accepted the transaction at its TxnID in
	// the original coordinator's round.
	FastVote bool   `json:",omitempty"`
	Writes   Writes `json:",omitempty"`
	Result   []byte `json:",omitempty"`
	// Durable: applied at every replica.
	Durable bool `json:",omitempty"`
}

// command is a Command with its runtime state.
type command struct {
	Command
	// owned are the keys this replica holds the command under.
	owned []Key
	// changed is closed (and replaced) whenever the command changes.
	changed chan struct{}
	// ready: every dependency that executes earlier has applied.
	ready     bool
	executing bool
	// skipped are dependencies from before this node joined that it does
	// not know: their effects came with its bootstrap data, so applying
	// does not wait for them, but reads do (they are fetched first).
	skipped []TxnID
	// blockedOn is a dependency the command waits for, since when.
	blockedOn    *TxnID
	blockedSince time.Time
	lastChange   time.Time
	appliedAt    time.Time
	// recoveries counts the recoveries this node started without the
	// command changing since: each waits twice as long as the previous.
	recoveries int
}

// cfk is a key's conflict index (accord.local.CommandsForKey): every
// command on the key, and the largest timestamp witnessed there.
type cfk struct {
	key   Key
	ids   []TxnID
	maxTS Timestamp
}

// store is a node's command store (accord.local.CommandStore): every
// handler runs under its lock.
type store struct {
	n       *Node
	mu      sync.Mutex
	cmds    map[TxnID]*command
	keys    map[string]*cfk
	waiters map[TxnID][]TxnID
	appends int
}

func newStore(n *Node) *store {
	return &store{n: n, cmds: map[TxnID]*command{}, keys: map[string]*cfk{}, waiters: map[TxnID][]TxnID{}}
}

func (s *store) get(id TxnID) *command {
	c := s.cmds[id]
	if c == nil {
		c = &command{Command: Command{ID: id}, changed: make(chan struct{})}
		s.cmds[id] = c
	}
	return c
}

// save journals a command and wakes its waiters.
func (s *store) saveLocked(c *command) error {
	c.lastChange = s.n.cfg.Now()
	c.recoveries = 0
	if err := s.n.cfg.Journal.Append(&c.Command); err != nil {
		return err
	}
	s.appends++
	close(c.changed)
	c.changed = make(chan struct{})
	for _, w := range s.waiters[c.ID] {
		if wc := s.cmds[w]; wc != nil {
			s.maybeExecuteLocked(wc)
		}
	}
	return nil
}

// register indexes a command under the keys this replica owns. The keys
// may be learnt piecemeal (a dependency seen on one key, then the whole
// transaction), so registration grows with them.
func (s *store) registerLocked(c *command, keys []Key) {
	if c.Txn != nil {
		keys = append(slices.Clone(keys), c.Txn.Keys...)
	}
	for _, k := range keys {
		if !slices.Contains(c.Keys, k) {
			c.Keys = append(c.Keys, k)
		}
	}
	slices.SortFunc(c.Keys, Key.Compare)
	if len(c.Keys) == 0 {
		return
	}
	owned := s.n.owned(c.Keys, c.ID.Epoch)
	if owned == nil {
		owned = []Key{}
	}
	for _, k := range owned {
		if slices.Contains(c.owned, k) {
			continue
		}
		ks := keyString(k)
		f := s.keys[ks]
		if f == nil {
			f = &cfk{key: k}
			s.keys[ks] = f
		}
		i, found := slices.BinarySearchFunc(f.ids, c.ID, TxnID.Compare)
		if !found {
			f.ids = slices.Insert(f.ids, i, c.ID)
		}
		if c.Status != NotDefined && c.Status != Truncated {
			f.maxTS = maxTimestamp(f.maxTS, c.ExecuteAt)
		}
	}
	if len(owned) > len(c.owned) {
		c.owned = owned
	}
}

// witness records a timestamp on the command's keys.
func (s *store) witnessLocked(c *command, ts Timestamp) {
	for _, k := range c.owned {
		f := s.keys[keyString(k)]
		f.maxTS = maxTimestamp(f.maxTS, ts)
	}
	s.n.clock.Observe(ts.HLC)
}

// maxConflict returns the largest timestamp witnessed on the command's
// keys before it.
func (s *store) maxConflictLocked(c *command) Timestamp {
	var m Timestamp
	for _, k := range c.owned {
		m = maxTimestamp(m, s.keys[keyString(k)].maxTS)
	}
	return m
}

// depsLocked computes the conflicting commands on the command's keys with
// a TxnID before bound (DepsCalculator).
func (s *store) depsLocked(c *command, bound Timestamp) Deps {
	d := Deps{}
	for _, k := range c.owned {
		for _, id := range s.keys[keyString(k)].ids {
			if !id.Timestamp.Less(bound) {
				break
			}
			if id == c.ID || !id.conflicts(c.ID) {
				continue
			}
			o := s.cmds[id]
			if o == nil || o.Status == Invalidated || o.Status == Truncated || o.Status == NotDefined {
				continue
			}
			d.add(k, id)
		}
	}
	return d
}

// ---- handlers ----

// preAccept witnesses a transaction (Commands.preaccept): its execution
// timestamp is its TxnID if nothing conflicting has a later timestamp and
// this is the original coordinator's round, a new later timestamp
// otherwise.
func (s *store) preAccept(r *preAcceptReq) (*preAcceptReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n.clock.Observe(r.ID.HLC)
	c := s.get(r.ID)
	if r.Ballot.Less(c.Promised) {
		return &preAcceptReply{Outcome: rejectedBallot}, nil
	}
	if c.Status == NotDefined && r.Ballot.IsZero() && r.ID.Epoch < s.n.synced.Load() {
		// A coordinator still on an epoch every node has moved past: its
		// replicas may no longer be the ones that matter (StaleTopology).
		return &preAcceptReply{Outcome: stale}, nil
	}
	if c.Status >= Committed {
		return &preAcceptReply{Outcome: redundant, ExecuteAt: c.ExecuteAt}, nil
	}
	if c.Status == NotDefined {
		if err := s.preAcceptLocked(c, r.Txn, !r.Ballot.IsZero()); err != nil {
			return nil, err
		}
	}
	deps := c.LocalDeps
	if c.Status >= Accepted {
		deps = c.Deps
	}
	return &preAcceptReply{Outcome: ok, ExecuteAt: c.ExecuteAt, Deps: deps, FastVote: c.FastVote}, nil
}

func (s *store) preAcceptLocked(c *command, txn *Txn, recovering bool) error {
	c.Txn = txn
	s.registerLocked(c, txn.Keys)
	// The fast path also needs the transaction's epoch to be current here:
	// otherwise it executes in the newer topology.
	if maxC := s.maxConflictLocked(c); !recovering && maxC.Less(c.ID.Timestamp) && c.ID.Epoch >= s.n.epoch() {
		c.ExecuteAt, c.FastVote = c.ID.Timestamp, true
	} else {
		c.ExecuteAt = Timestamp{Epoch: max(c.ID.Epoch, s.n.epoch(), maxC.Epoch), HLC: s.n.clock.Next(maxC.HLC), Node: s.n.cfg.ID}
	}
	c.LocalDeps = s.depsLocked(c, c.ID.Timestamp)
	c.Status = PreAccepted
	s.witnessLocked(c, c.ID.Timestamp)
	s.witnessLocked(c, c.ExecuteAt)
	return s.saveLocked(c)
}

// accept accepts a proposal (Commands.accept) and returns the dependencies
// this replica knows before the proposed timestamp.
func (s *store) accept(r *acceptReq) (*acceptReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.get(r.ID)
	if r.Ballot.Less(c.Promised) {
		return &acceptReply{Outcome: rejectedBallot, Promised: c.Promised}, nil
	}
	if c.Status >= Committed {
		return &acceptReply{Outcome: redundant}, nil
	}
	c.Promised, c.Accepted = r.Ballot, r.Ballot
	if r.Invalidate {
		c.Status = AcceptedInvalidate
		s.registerLocked(c, r.Keys)
		s.witnessLocked(c, c.ID.Timestamp)
		return &acceptReply{Outcome: ok}, s.saveLocked(c)
	}
	if c.Txn == nil {
		c.Txn = r.Txn
	}
	s.registerLocked(c, r.Keys)
	c.Status, c.ExecuteAt, c.Deps = Accepted, r.ExecuteAt, r.Deps
	s.witnessLocked(c, r.ExecuteAt)
	if r.NoDeps {
		return &acceptReply{Outcome: ok}, s.saveLocked(c)
	}
	deps := s.depsLocked(c, r.ExecuteAt)
	return &acceptReply{Outcome: ok, Deps: deps}, s.saveLocked(c)
}

// commit records a decision (Commands.commit): Committed, then Stable once
// the coordinator knows a quorum has it. With Read, the replica answers
// with its reads once the command can execute (StableThenRead).
func (s *store) commit(ctx context.Context, r *commitReq) (*commitReply, error) {
	s.mu.Lock()
	c := s.get(r.ID)
	if r.Invalidate {
		err := s.invalidateLocked(c, r.Keys)
		s.mu.Unlock()
		return &commitReply{Outcome: ok}, err
	}
	if c.Status == Invalidated {
		s.mu.Unlock()
		return &commitReply{Outcome: redundant}, nil
	}
	if err := s.commitLocked(c, r.ExecuteAt, r.Deps, r.Txn, r.Keys, r.Ballot, r.Stable); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if !r.Read {
		s.mu.Unlock()
		return &commitReply{Outcome: ok}, nil
	}
	// Wait until the command can execute. The read happens under the
	// lock, so the command's own writes (from another coordinator) cannot
	// be applied in between; once they are known the outcome is answered
	// instead.
	for {
		if c.Status >= PreApplied && c.Status != Invalidated {
			rep := &commitReply{Outcome: ok, Applied: true, Writes: c.Writes, Result: c.Result}
			s.mu.Unlock()
			return rep, nil
		}
		if c.ready {
			break
		}
		ch := c.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return &commitReply{Outcome: notReady}, nil
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	if s.missingLocked(c.skipped, c.Keys) {
		return &commitReply{Outcome: notReady}, nil
	}
	reads := map[string][]byte{}
	for _, k := range c.owned {
		b, err := s.n.cfg.Data.Read(k, c.ExecuteAt, c.Txn)
		if err != nil {
			return nil, err
		}
		reads[keyString(k)] = b
	}
	return &commitReply{Outcome: ok, Reads: reads}, nil
}

func (s *store) commitLocked(c *command, executeAt Timestamp, deps Deps, txn *Txn, keys []Key, ballot Ballot, stable bool) error {
	if c.Status == Truncated || c.Status == Applied || c.Status == PreApplied {
		return nil
	}
	if c.Txn == nil {
		c.Txn = txn
	}
	s.registerLocked(c, keys)
	want := Committed
	if stable {
		want = Stable
	}
	switch {
	case c.Status < Committed:
	case c.Status == Committed && (stable || c.Accepted.Less(ballot)):
		// A later decision of the same execution timestamp (recovery may
		// have computed other, equally valid, dependencies): adopt it.
	default:
		return nil
	}
	c.Status, c.ExecuteAt, c.Deps = max(want, c.Status), executeAt, deps
	c.Accepted = maxTimestamp(c.Accepted, ballot)
	s.witnessLocked(c, executeAt)
	if err := s.saveLocked(c); err != nil {
		return err
	}
	s.maybeExecuteLocked(c)
	return nil
}

func (s *store) invalidateLocked(c *command, keys []Key) error {
	if c.Status == Invalidated || c.Status == Truncated {
		return nil
	}
	if c.Status.decided() {
		// Cannot happen: a recovery invalidates only what cannot have been
		// committed.
		return nil
	}
	s.registerLocked(c, keys)
	c.Status = Invalidated
	return s.saveLocked(c)
}

// apply records a transaction's writes (Commands.apply) and answers once
// they are applied.
func (s *store) apply(ctx context.Context, r *applyReq) (*applyReply, error) {
	s.mu.Lock()
	c := s.get(r.ID)
	if c.Status == Invalidated {
		s.mu.Unlock()
		return &applyReply{Outcome: redundant}, nil
	}
	if c.Status < PreApplied {
		if c.Txn == nil {
			c.Txn = r.Txn
		}
		s.registerLocked(c, r.Txn.Keys)
		c.Status, c.ExecuteAt, c.Deps, c.Writes, c.Result = PreApplied, r.ExecuteAt, r.Deps, r.Writes, r.Result
		s.witnessLocked(c, r.ExecuteAt)
		if err := s.saveLocked(c); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.maybeExecuteLocked(c)
	}
	for c.Status != Applied && c.Status != Truncated {
		ch := c.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return &applyReply{Outcome: notReady}, nil
		}
		s.mu.Lock()
	}
	s.mu.Unlock()
	return &applyReply{Outcome: ok}, nil
}

// maybeExecuteLocked checks whether a stable command can execute: every
// dependency on its keys that executes earlier has applied (or was
// invalidated); dependencies not yet decided here are waited for.
func (s *store) maybeExecuteLocked(c *command) {
	if c.Status != Stable && c.Status != PreApplied || c.executing {
		return
	}
	if !c.ready {
		for _, k := range c.owned {
			ks := keyString(k)
			for _, id := range c.Deps[ks] {
				d := s.cmds[id]
				if d != nil {
					switch {
					case d.Status == Applied || d.Status == Invalidated || d.Status == Truncated:
						continue
					case d.Status.decided() && c.ExecuteAt.Less(d.ExecuteAt):
						continue // it executes after this one
					}
				}
				if (d == nil || d.Status < PreAccepted) && id.Epoch < s.n.joined.Load() {
					if !slices.Contains(c.skipped, id) {
						c.skipped = append(c.skipped, id)
					}
					continue
				}
				if c.blockedOn == nil || *c.blockedOn != id {
					idc := id
					c.blockedOn, c.blockedSince = &idc, s.n.cfg.Now()
					if !slices.Contains(s.waiters[id], c.ID) {
						s.waiters[id] = append(s.waiters[id], c.ID)
					}
				}
				return
			}
		}
		c.ready, c.blockedOn = true, nil
		close(c.changed)
		c.changed = make(chan struct{})
	}
	if c.Status != PreApplied {
		return
	}
	c.executing = true
	go s.applyWrites(c.ID, c.Txn, c.ExecuteAt, c.owned, c.Writes)
}

// applyWrites applies a ready command's writes, then marks it Applied.
func (s *store) applyWrites(id TxnID, txn *Txn, executeAt Timestamp, owned []Key, writes Writes) {
	for _, k := range owned {
		if w, ok := writes[keyString(k)]; ok {
			for !s.n.stopped.Load() {
				err := s.n.cfg.Data.Apply(k, executeAt, txn, w)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond) // the store is failing: retry
			}
		}
	}
	if s.n.stopped.Load() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.cmds[id]
	c.executing = false
	if c.Status != PreApplied {
		return
	}
	c.Status, c.appliedAt = Applied, s.n.cfg.Now()
	_ = s.saveLocked(c)
	delete(s.waiters, id)
}

// beginRecovery answers a recovery coordinator (BeginRecovery): it promises
// the ballot, pre-accepts the transaction if it had not been (without a
// fast path vote), and reports what it knows, including whether a
// transaction that executes later has not witnessed this one.
func (s *store) beginRecovery(r *recoverReq) (*recoverReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.get(r.ID)
	if r.Ballot.Less(c.Promised) {
		return &recoverReply{Outcome: rejectedBallot}, nil
	}
	c.Promised = r.Ballot
	s.registerLocked(c, r.Keys)
	if c.Status != NotDefined || r.Txn == nil {
		s.witnessLocked(c, c.ID.Timestamp)
	}
	if c.Status == NotDefined && r.Txn != nil {
		if err := s.preAcceptLocked(c, r.Txn, true); err != nil {
			return nil, err
		}
	} else if err := s.saveLocked(c); err != nil {
		return nil, err
	}
	rep := &recoverReply{Outcome: ok, Status: c.Status, Accepted: c.Accepted, ExecuteAt: c.ExecuteAt, Deps: c.Deps,
		LocalDeps: c.LocalDeps, FastVote: c.FastVote, Txn: c.Txn, Writes: c.Writes, Result: c.Result}
	if c.Status >= Accepted {
		return rep, nil
	}
	// The superseding transactions (BeginRecovery's visitor).
	for _, k := range c.owned {
		ks := keyString(k)
		for _, id := range s.keys[ks].ids {
			o := s.cmds[id]
			if id == c.ID || o == nil || !id.conflicts(c.ID) || !c.ID.Timestamp.Less(o.ExecuteAt) {
				continue
			}
			switch {
			case o.Status.decided() && o.Status != Truncated:
				if !o.Deps.contains(ks, c.ID) {
					rep.Rejects = true
				}
			case o.Status == Accepted:
				if o.Deps.contains(ks, c.ID) {
					continue
				}
				if c.ID.Compare(id) < 0 {
					// Started after this one, and its proposal does not
					// include it: it pre-accepted where this one was not
					// witnessed first, so this one has no fast path.
					rep.Rejects = true
				} else if !slices.Contains(rep.Wait, id) {
					rep.Wait = append(rep.Wait, id)
				}
			}
		}
	}
	return rep, nil
}

func (s *store) checkStatus(id TxnID) *checkStatusReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.cmds[id]
	if c == nil {
		return &checkStatusReply{}
	}
	return &checkStatusReply{Status: c.Status, Promised: c.Promised, Accepted: c.Accepted, ExecuteAt: c.ExecuteAt,
		Deps: c.Deps, Txn: c.Txn, Keys: c.Keys, Writes: c.Writes, Result: c.Result}
}

func (s *store) informDurable(id TxnID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.cmds[id]
	if c == nil || c.Durable || c.Status != Applied {
		return nil
	}
	c.Durable = true
	return s.saveLocked(c)
}

// truncateLocked erases the state of commands applied at every replica
// long enough ago (Cleanup): only their outcome and timestamp remain, as
// they cannot be needed again (CASSANDRA-18365: an erased command is only
// adopted once it is durably decided everywhere).
func (s *store) truncateLocked() {
	s.forgetLocked()
	now := s.n.cfg.Now()
	for _, c := range s.cmds {
		if c.Status != Applied || !c.Durable || now.Sub(c.appliedAt) < s.n.cfg.RetainApplied {
			continue
		}
		for _, k := range c.owned {
			f := s.keys[keyString(k)]
			if i, found := slices.BinarySearchFunc(f.ids, c.ID, TxnID.Compare); found {
				f.ids = slices.Delete(f.ids, i, i+1)
			}
		}
		c.Status, c.Txn, c.Deps, c.LocalDeps, c.Writes, c.Result = Truncated, nil, nil, nil, nil, nil
		_ = s.saveLocked(c)
	}
	if s.appends > 4*len(s.cmds)+1024 {
		var all []*Command
		for _, c := range s.cmds {
			all = append(all, &c.Command)
		}
		if s.n.cfg.Journal.Compact(all) == nil {
			s.appends = len(all)
		}
	}
}

// replay rebuilds the store from the journal.
func (s *store) replay() error {
	cs, err := s.n.cfg.Journal.Load()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.n.cfg.Now()
	for _, saved := range cs {
		c := &command{Command: *saved, changed: make(chan struct{}), lastChange: now, appliedAt: now}
		s.cmds[c.ID] = c
		s.n.clock.Observe(max(c.ID.HLC, c.ExecuteAt.HLC))
	}
	s.appends = len(cs)
	return nil
}

// rescanLocked indexes commands whose keys could not be placed before (no
// topology yet) and retries execution.
func (s *store) rescanLocked() {
	for _, c := range s.cmds {
		if len(c.Keys) > 0 && c.Status != Truncated {
			s.registerLocked(c, nil)
			s.witnessLocked(c, c.ID.Timestamp)
			s.witnessLocked(c, c.ExecuteAt)
		}
	}
	for _, c := range s.cmds {
		s.maybeExecuteLocked(c)
	}
}

// fastCommit is the privileged coordinator's local decision of the fast
// path: it commits only if no recovery has promised a ballot here, which a
// recovery that reaches this node relies on to invalidate safely.
func (s *store) fastCommit(id TxnID, txn *Txn, deps Deps) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.get(id)
	if !c.Promised.IsZero() {
		return c.Status.decided() && c.ExecuteAt == id.Timestamp
	}
	if c.Status.decided() {
		return c.ExecuteAt == id.Timestamp
	}
	return s.commitLocked(c, id.Timestamp, deps, txn, txn.Keys, Ballot{}, false) == nil
}

// ephemeralDeps returns the writes on keys a read must wait for
// (GetEphemeralReadDeps): every conflicting write not yet applied at every
// replica.
func (s *store) ephemeralDeps(keys []Key, ranges []Range) *ephemeralReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := Deps{}
	var fs []*cfk
	for _, k := range s.n.owned(keys, 0) {
		if f := s.keys[keyString(k)]; f != nil {
			fs = append(fs, f)
		}
	}
	if len(ranges) > 0 {
		// Every key this replica indexes in the ranges.
		for _, f := range s.keys {
			if slices.ContainsFunc(ranges, func(r Range) bool { return r.contains(f.key) }) {
				fs = append(fs, f)
			}
		}
	}
	for _, f := range fs {
		k := f.key
		for _, id := range f.ids {
			c := s.cmds[id]
			if c == nil || id.Kind != Write || c.Status == Invalidated || c.Status == Truncated || c.Status == Applied && c.Durable {
				continue
			}
			d.add(k, id)
		}
	}
	return &ephemeralReply{Outcome: ok, Deps: d}
}

// ephemeralRead reads keys once every dependency on them has applied here
// (ReadEphemeralTxnData); dependencies unknown here are fetched.
func (s *store) ephemeralRead(ctx context.Context, r *ephemeralReq) (*ephemeralReply, error) {
	owned := s.n.owned(r.Keys, 0)
	for ks := range r.Deps {
		if k, ok := parseKeyString(ks); ok && slices.ContainsFunc(r.Ranges, func(rg Range) bool { return rg.contains(k) }) {
			owned = append(owned, k)
		}
	}
	start := s.n.cfg.Now()
	fetched := map[TxnID]bool{}
	s.mu.Lock()
	for {
		var wait *command
		for _, k := range owned {
			for _, id := range r.Deps[keyString(k)] {
				c := s.get(id)
				if c.Status == Applied || c.Status == Invalidated || c.Status == Truncated {
					continue
				}
				if len(c.Keys) == 0 {
					s.registerLocked(c, []Key{k})
				}
				if c.Status < PreAccepted && id.Epoch < s.n.joined.Load() {
					// From before this node joined: another replica reads.
					s.missingLocked([]TxnID{id}, []Key{k})
					s.mu.Unlock()
					return &ephemeralReply{Outcome: notReady}, nil
				}
				wait = c
				break
			}
			if wait != nil {
				break
			}
		}
		if wait == nil {
			break
		}
		if !fetched[wait.ID] && s.n.cfg.Now().Sub(start) > s.n.cfg.RecoverAfter {
			fetched[wait.ID] = true
			id, keys := wait.ID, slices.Clone(wait.Keys)
			go func() {
				if _, busy := s.n.recovering.LoadOrStore(id, true); busy {
					return
				}
				defer s.n.recovering.Delete(id)
				fctx, cancel := context.WithTimeout(context.Background(), s.n.recoveryTimeout())
				defer cancel()
				if !s.n.fetch(fctx, id, keys) {
					_ = s.n.recover(fctx, id, nil, keys)
				}
			}()
		}
		ch := wait.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return &ephemeralReply{Outcome: notReady}, nil
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	reads := map[string][]byte{}
	for _, k := range s.n.owned(r.Keys, 0) {
		b, err := s.n.cfg.Data.Read(k, Timestamp{}, r.Txn)
		if err != nil {
			return nil, err
		}
		reads[keyString(k)] = b
	}
	if len(r.Ranges) > 0 {
		rr, ok := s.n.cfg.Data.(RangeReader)
		if !ok {
			return nil, errors.New("accord: the data store cannot read ranges")
		}
		for _, rg := range r.Ranges {
			b, err := rr.ReadRange(rg, r.Txn)
			if err != nil {
				return nil, err
			}
			reads[rg.String()] = b
		}
	}
	return &ephemeralReply{Outcome: ok, Reads: reads}, nil
}

// epochStatus reports the oldest epoch of a transaction in progress here.
func (s *store) epochStatus() *epochStatus {
	latest := s.n.epoch()
	st := &epochStatus{Active: latest, Latest: latest}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.cmds {
		switch c.Status {
		case Applied, Invalidated, Truncated:
			continue
		case NotDefined:
			if c.Promised.IsZero() {
				continue
			}
		}
		st.Active = min(st.Active, c.ID.Epoch)
	}
	return st
}

// forgetLocked drops the tombstones of transactions truncated or
// invalidated long ago. A key's largest witnessed timestamp is kept, so a
// late message about a forgotten transaction cannot make it pass the fast
// path again: it is recovered and invalidated.
func (s *store) forgetLocked() {
	now := s.n.cfg.Now()
	for id, c := range s.cmds {
		if c.Status != Truncated && c.Status != Invalidated || now.Sub(c.lastChange) < s.n.cfg.TombstoneRetention {
			continue
		}
		for _, k := range c.owned {
			if f := s.keys[keyString(k)]; f != nil {
				if i, found := slices.BinarySearchFunc(f.ids, id, TxnID.Compare); found {
					f.ids = slices.Delete(f.ids, i, i+1)
				}
			}
		}
		if s.n.cfg.Journal.Delete(id) == nil {
			delete(s.cmds, id)
			delete(s.waiters, id)
		}
	}
}

// missingLocked reports whether some of ids are not applied here yet, and
// fetches them in the background.
func (s *store) missingLocked(ids []TxnID, keys []Key) bool {
	missing := false
	for _, id := range ids {
		d := s.cmds[id]
		if d != nil && (d.Status == Applied || d.Status == Invalidated || d.Status == Truncated) {
			continue
		}
		missing = true
		if d != nil && len(d.Keys) > 0 {
			keys = d.Keys
		}
		go s.n.fetchOnce(id, slices.Clone(keys))
	}
	return missing
}

// parseKeyString inverts keyString.
func parseKeyString(ks string) (Key, bool) {
	i := strings.IndexByte(ks, '/')
	if i < 0 {
		return Key{}, false
	}
	tok, err := strconv.ParseInt(ks[:i], 10, 64)
	if err != nil {
		return Key{}, false
	}
	return Key{Token: tok, ID: ks[i+1:]}, true
}
