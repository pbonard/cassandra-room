package accord

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
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
	// UniqueHLC, when above ExecuteAt.HLC, is the HLC the transaction's
	// writes carry so they exceed every earlier write applied on its keys
	// (TimestampWithUniqueHlc).
	UniqueHLC int64 `json:",omitempty"`
	// HLCBallot is the ballot UniqueHLC was accepted with (DecideUniqueHlc)
	// before the transaction applied here: a recovery adopts the one of
	// the highest ballot, so that every replica writes it at the same HLC.
	HLCBallot Ballot `json:",omitempty"`
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
	// Redundant: a sync point applied at every replica of its ranges, so
	// every transaction with a lower id there is durable or never commits
	// (RedundantBefore).
	Redundant bool `json:",omitempty"`
	// Bootstrap: not a command, but the sync points this node bootstrapped
	// ranges at (Bounds), kept across restarts.
	Bootstrap bool    `json:",omitempty"`
	Bounds    []bound `json:",omitempty"`
	// SyncedEpoch: not a command, but the oldest epoch any node still has
	// transactions of (the node's synced epoch), kept across restarts.
	SyncedEpoch int64 `json:",omitempty"`
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
	// reading counts reads of the command in progress: its writes wait.
	reading int
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
// command on the key, the largest timestamp witnessed there, and the
// largest write HLC applied there.
type cfk struct {
	key           Key
	ids           []TxnID
	maxTS         Timestamp
	maxAppliedHLC int64
}

func (c *command) writeHLC() int64 { return max(c.ExecuteAt.HLC, c.UniqueHLC) }

func (c *command) writeAt() Timestamp {
	return Timestamp{Epoch: c.ExecuteAt.Epoch, HLC: c.writeHLC(), Node: c.ExecuteAt.Node}
}

// store is a node's command store (accord.local.CommandStore): every
// handler runs under its lock.
type store struct {
	n    *Node
	mu   sync.Mutex
	cmds map[TxnID]*command
	keys map[string]*cfk
	// sorted indexes the keys in token order, for range reads.
	sorted []*cfk
	// barriers are the sync points witnessed per range: a new transaction
	// with a lower id is refused there. redundant are the bounds below
	// which every transaction is durable or never commits.
	barriers, redundant []bound
	// majority are bounds below which every transaction applied at a
	// majority of the replicas (or never commits): not kept across restarts.
	majority []bound
	// gc commits journal records in groups, outside the lock (nil: the
	// journal is written synchronously under it); lastSeq is the last
	// record queued.
	gc      *groupCommit
	lastSeq uint64
	waiters map[TxnID][]TxnID
	appends int
	// bootstrapRec and syncedRec are the journal records of the bootstrap
	// points and of the synced epoch.
	bootstrapRec, syncedRec *Command
	// bootstrapped are the sync points this node bootstrapped ranges at:
	// the transactions on a range before its point are reflected in the
	// data it received for the range (streamed in when it became a
	// replica of it), so it does not wait for those it does not know.
	// Per range: on the ranges it already replicated, it knows every
	// transaction and waits for each (a single node-wide point let a
	// later bootstrap skip dependencies there).
	bootstrapped []bound
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
	if s.gc != nil {
		b, err := json.Marshal(&c.Command)
		if err != nil {
			return err
		}
		s.lastSeq = s.gc.enqueue(Record{ID: c.ID, Data: b})
	} else if err := s.n.cfg.Journal.Append(&c.Command); err != nil {
		s.n.metrics.journalErrors.Add(1)
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
	// Copy on write: replies built from the command are encoded after the
	// store lock is released.
	var add []Key
	for _, k := range keys {
		if !slices.Contains(c.Keys, k) && !slices.Contains(add, k) {
			add = append(add, k)
		}
	}
	if len(add) > 0 {
		nk := append(slices.Clone(c.Keys), add...)
		slices.SortFunc(nk, Key.Compare)
		c.Keys = nk
	}
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
			i, _ := slices.BinarySearchFunc(s.sorted, k, func(c *cfk, k Key) int { return c.key.Compare(k) })
			s.sorted = slices.Insert(s.sorted, i, f)
		}
		i, found := slices.BinarySearchFunc(f.ids, c.ID, TxnID.Compare)
		if !found {
			f.ids = slices.Insert(f.ids, i, c.ID)
		}
		if c.Status != NotDefined && c.Status != Truncated {
			f.maxTS = mergeMax(f.maxTS, c.writeAt())
		}
		if c.Status == Applied && c.ID.Kind != SyncPoint {
			f.maxAppliedHLC = max(f.maxAppliedHLC, c.writeHLC())
		}
	}
	if len(owned) > len(c.owned) {
		if c.Status == Applied || c.executing {
			s.checkLateOwnedLocked(c, owned)
		}
		c.owned = owned
	}
}

// checkLateOwnedLocked checks a command applied here gaining keys: that
// can only be from an epoch after the one it executed at (a range this
// node starts receiving, whose data, the command's writes included, it
// streams). A key owned by the epoch it executed at would have been
// applied then, had this node known that epoch: such writes are lost.
func (s *store) checkLateOwnedLocked(c *command, owned []Key) {
	then := s.n.ownedIn(c.Keys, c.ID.Epoch, c.ExecuteAt.Epoch)
	for _, k := range owned {
		if !slices.Contains(c.owned, k) && slices.ContainsFunc(then, func(o Key) bool { return o == k }) {
			s.n.metrics.lateOwned.Add(1)
			return
		}
	}
}

// witness records a timestamp on the command's keys (a sync point's
// barrier is recorded per range instead).
func (s *store) witnessLocked(c *command, ts Timestamp) {
	if c.ID.Kind != SyncPoint {
		for _, k := range c.owned {
			if f := s.keys[keyString(k)]; f != nil {
				f.maxTS = mergeMax(f.maxTS, ts)
			}
		}
	}
	s.n.clock.Observe(ts.HLC)
}

// keyLocked returns a key's conflicts (empty, not to be changed, for a key
// forgotten).
func (s *store) keyLocked(k Key) *cfk {
	if f := s.keys[keyString(k)]; f != nil {
		return f
	}
	return &cfk{key: k}
}

// maxConflict returns a timestamp at least every one witnessed on the
// command's keys before it, in epoch and in HLC (see mergeMax).
func (s *store) maxConflictLocked(c *command) Timestamp {
	var m Timestamp
	for _, k := range c.owned {
		m = mergeMax(m, s.keyLocked(k).maxTS)
	}
	return m
}

// depsLocked computes the conflicting commands on the command's keys with
// a TxnID before bound (DepsCalculator).
func (s *store) depsLocked(c *command, bound Timestamp) Deps {
	d := Deps{}
	for _, k := range c.owned {
		for _, id := range s.keyLocked(k).ids {
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

func (s *store) preAccept(r *preAcceptReq) (*preAcceptReply, error) {
	s.mu.Lock()
	rep, err := s.preAcceptLocked2(r)
	if uerr := s.unlock(); err == nil {
		err = uerr
	}
	return rep, err
}

// preAccept witnesses a transaction (Commands.preaccept): its execution
// timestamp is its TxnID if nothing conflicting has a later timestamp and
// this is the original coordinator's round, a new later timestamp
// otherwise.
func (s *store) preAcceptLocked2(r *preAcceptReq) (*preAcceptReply, error) {
	s.n.clock.Observe(r.ID.HLC)
	c := s.get(r.ID)
	if r.Ballot.Less(c.Promised) {
		return &preAcceptReply{Outcome: rejectedBallot}, nil
	}
	if c.Status == NotDefined && r.Ballot.IsZero() && r.Txn != nil && s.belowBarrierLocked(r.ID, r.Txn.Keys) {
		// A sync point witnessed here follows every transaction with a
		// lower id it knew of: a new one is refused (it is retried with a
		// new id).
		return &preAcceptReply{Outcome: stale}, nil
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
	// The fast path needs a strictly higher HLC than every conflict, not
	// just a higher timestamp: the HLC is the written cells' timestamp, and
	// two conflicting transactions writing at the same one would tie.
	if maxC := s.maxConflictLocked(c); !recovering && maxC.Less(c.ID.Timestamp) && maxC.HLC < c.ID.HLC && c.ID.Epoch >= s.n.epoch() {
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

func (s *store) accept(r *acceptReq) (*acceptReply, error) {
	s.mu.Lock()
	rep, err := s.acceptLocked2(r)
	if uerr := s.unlock(); err == nil {
		err = uerr
	}
	return rep, err
}

// accept accepts a proposal (Commands.accept) and returns the dependencies
// this replica knows before the proposed timestamp.
func (s *store) acceptLocked2(r *acceptReq) (*acceptReply, error) {
	c := s.get(r.ID)
	if r.Ballot.Less(c.Promised) {
		return &acceptReply{Outcome: rejectedBallot, Promised: c.Promised}, nil
	}
	if c.Status >= Committed {
		return &acceptReply{Outcome: redundant}, nil
	}
	if c.Status == NotDefined && r.Ballot.IsZero() && !r.Invalidate && s.belowBarrierLocked(r.ID, r.Keys) {
		return &acceptReply{Outcome: stale}, nil
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
	if s.forgottenLocked(r.ID, r.Keys) {
		// A late message about a transaction forgotten below the
		// redundancy bound: recreating it would leave it waiting forever.
		return &commitReply{Outcome: redundant}, s.unlock()
	}
	c := s.get(r.ID)
	if r.Invalidate {
		err := s.invalidateLocked(c, r.Keys)
		if uerr := s.unlock(); err == nil {
			err = uerr
		}
		return &commitReply{Outcome: ok}, err
	}
	if c.Status == Invalidated {
		return &commitReply{Outcome: redundant}, s.unlock()
	}
	if err := s.commitLocked(c, r.ExecuteAt, r.Deps, r.Txn, r.Keys, r.Ballot, r.Stable); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if !r.Read {
		return &commitReply{Outcome: ok}, s.unlock()
	}
	// Wait until the command can read: every dependency on the keys this
	// node reads for it applied (those it is only pending on, whose data
	// may still be streaming to it, are neither read nor waited for); once
	// its writes are known the outcome is answered instead.
	var readKeys []Key
	var skipped []TxnID
	for {
		if c.Status >= PreApplied && c.Status != Invalidated {
			rep := &commitReply{Outcome: ok, Applied: true, UniqueHLC: c.UniqueHLC, Writes: c.Writes, Result: c.Result}
			return rep, s.unlock()
		}
		var wait <-chan struct{}
		if c.Status == Stable && s.knowsEpochsLocked(c) {
			readKeys, skipped = s.n.readOwned(c.Keys, c.ExecuteAt.Epoch), nil
			id, blocked := s.blockerLocked(c, readKeys, &skipped)
			if !blocked {
				break
			}
			if d := s.cmds[id]; d != nil {
				wait = d.changed
			}
		}
		ch := c.changed
		if err := s.unlock(); err != nil {
			return nil, err
		}
		select {
		case <-ch:
		case <-wait:
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return &commitReply{Outcome: notReady}, nil
		}
		s.mu.Lock()
	}
	if s.missingLocked(skipped, readKeys, c.Deps) {
		return &commitReply{Outcome: notReady}, s.unlock()
	}
	// Every earlier dependency on readKeys applied here: the transaction's
	// writes must carry an HLC strictly above all of them (and above any
	// streamed bootstrap bound), even when an older-epoch slow-path commit
	// or a same-microsecond slow-path commit on another node wrote at >=
	// ExecuteAt.HLC.
	minHLC := c.ExecuteAt.HLC
	for _, k := range readKeys {
		ks := keyString(k)
		if f := s.keys[ks]; f != nil && f.maxAppliedHLC >= minHLC {
			minHLC = f.maxAppliedHLC + 1
		}
		for _, depID := range c.Deps[ks] {
			if d := s.cmds[depID]; d != nil && d.Status == Applied && d.ID.Kind != SyncPoint && d.writeHLC() >= minHLC {
				minHLC = d.writeHLC() + 1
			}
		}
		if bHLC := boundMaxHLC(s.bootstrapped, k); bHLC > 0 && bHLC >= minHLC {
			minHLC = bHLC + 1
		}
	}
	// The read happens outside the lock: while it runs, the command's own
	// writes (from another coordinator) are held back (reading), and no
	// later conflicting transaction applies before them.
	c.reading++
	executeAt, txn := c.ExecuteAt, c.Txn
	err := s.unlock()
	reads := map[string][]byte{}
	for _, k := range readKeys {
		if err != nil {
			break
		}
		var b []byte
		if b, err = s.n.cfg.Data.Read(k, executeAt, txn); err == nil {
			reads[keyString(k)] = b
		}
	}
	s.mu.Lock()
	c.reading--
	if c.reading == 0 {
		s.maybeExecuteLocked(c)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &commitReply{Outcome: ok, Reads: reads, MinHLC: minHLC}, nil
}

// decideHLC accepts, at a ballot, the HLC a transaction's writes carry
// (a Paxos accept of that one value: the command's ballots order its
// executors). The HLC depends on what the executor's read replica applied
// before (UniqueHLC), so two executors (its coordinator and a recovery)
// may compute different ones; without agreeing on one first, each replica
// kept the first it was sent, and a later transaction, writing just above
// one replica's, lost to another's.
func (s *store) decideHLC(r *decideHLCReq) (*decideHLCReply, error) {
	s.mu.Lock()
	if s.forgottenLocked(r.ID, r.Keys) {
		return &decideHLCReply{Outcome: redundant}, s.unlock()
	}
	c := s.get(r.ID)
	switch {
	case c.Status == Invalidated:
		return &decideHLCReply{Outcome: redundant}, s.unlock()
	case c.Status >= PreApplied:
		// The HLC its writes were applied at, never below the execution
		// timestamp's (UniqueHLC alone is 0 for a command applied without
		// one).
		return &decideHLCReply{Outcome: ok, Applied: true, UniqueHLC: c.writeHLC()}, s.unlock()
	case r.Ballot.Less(c.Promised):
		return &decideHLCReply{Outcome: rejectedBallot}, s.unlock()
	}
	if c.UniqueHLC == 0 || !r.Ballot.Less(c.HLCBallot) {
		c.UniqueHLC, c.HLCBallot = r.UniqueHLC, r.Ballot
		if len(c.Keys) == 0 {
			s.registerLocked(c, r.Keys)
		}
		if err := s.saveLocked(c); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	// Read before unlocking: in one return statement the field read is
	// not ordered before the call.
	rep := &decideHLCReply{Outcome: ok, UniqueHLC: c.UniqueHLC}
	return rep, s.unlock()
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
	if r.Txn != nil && s.forgottenLocked(r.ID, r.Txn.Keys) {
		// Forgotten below the redundancy bound: the universal sync point
		// that set it applied here after this transaction.
		return &applyReply{Outcome: ok}, s.unlock()
	}
	c := s.get(r.ID)
	if c.Status == Invalidated {
		return &applyReply{Outcome: redundant}, s.unlock()
	}
	if c.Status < PreApplied {
		if c.Txn == nil {
			c.Txn = r.Txn
		}
		s.registerLocked(c, r.Txn.Keys)
		c.Status, c.ExecuteAt, c.UniqueHLC, c.Deps, c.Writes, c.Result = PreApplied, r.ExecuteAt, r.UniqueHLC, r.Deps, r.Writes, r.Result
		if r.Txn.Kind == SyncPoint {
			s.syncOwnedLocked(c)
		}
		s.witnessLocked(c, c.writeAt())
		if err := s.saveLocked(c); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.maybeExecuteLocked(c)
	}
	for c.Status != Applied && c.Status != Truncated {
		ch := c.changed
		if err := s.unlock(); err != nil {
			return nil, err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return &applyReply{Outcome: notReady}, nil
		}
		s.mu.Lock()
	}
	var maxHLC int64
	if r.Txn != nil && r.Txn.Kind == SyncPoint {
		maxHLC = s.maxRangeHLCLocked(r.Txn.Ranges, c.ExecuteAt.HLC)
	}
	return &applyReply{Outcome: ok, MaxHLC: maxHLC}, s.unlock()
}

// maxRangeHLCLocked returns the largest HLC applied or witnessed on ranges.
func (s *store) maxRangeHLCLocked(ranges []Range, floor int64) int64 {
	m := floor
	for _, rg := range ranges {
		i := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i].key.Token >= rg.Lo })
		for ; i < len(s.sorted) && s.sorted[i].key.Token <= rg.Hi; i++ {
			if f := s.sorted[i]; rg.contains(f.key) {
				m = max(m, f.maxAppliedHLC, f.maxTS.HLC)
			}
		}
	}
	return m
}

// maybeExecuteLocked checks whether a stable command can execute: every
// dependency on its keys that executes earlier has applied (or was
// invalidated); dependencies not yet decided here are waited for.
func (s *store) maybeExecuteLocked(c *command) {
	if c.Status != Stable && c.Status != PreApplied || c.executing {
		return
	}
	// Not before this node knows the epochs it was planned and executes
	// at: its keys here are those it owns then (SetTopology rescans).
	if !s.knowsEpochsLocked(c) {
		return
	}
	if !c.ready {
		if id, blocked := s.blockerLocked(c, c.owned, &c.skipped); blocked {
			if c.blockedOn == nil || *c.blockedOn != id {
				c.blockedOn, c.blockedSince = &id, s.n.cfg.Now()
				if !slices.Contains(s.waiters[id], c.ID) {
					s.waiters[id] = append(s.waiters[id], c.ID)
				}
			}
			return
		}
		c.ready, c.blockedOn = true, nil
		close(c.changed)
		c.changed = make(chan struct{})
	}
	if c.Status != PreApplied || c.reading > 0 {
		return
	}
	c.executing = true
	id, txn, at, owned, writes := c.ID, c.Txn, c.writeAt(), c.owned, c.Writes
	s.n.background(func(context.Context) { s.applyWrites(id, txn, at, owned, writes) })
}

// knowsEpochsLocked reports whether this node knows the epochs a command
// was planned and executes at.
func (s *store) knowsEpochsLocked(c *command) bool {
	return max(c.ID.Epoch, c.ExecuteAt.Epoch) <= s.n.epoch()
}

// blockerLocked returns a dependency on keys that a command still waits
// for, if any: one that executes earlier and has not applied here.
// Dependencies from before this node received the keys' data (its
// bootstrap point) are added to skipped instead: they need not apply here,
// but a read waits until they are known.
func (s *store) blockerLocked(c *command, keys []Key, skipped *[]TxnID) (TxnID, bool) {
	for _, k := range keys {
		ks := keyString(k)
		for _, id := range c.Deps[ks] {
			d := s.cmds[id]
			if d != nil {
				switch {
				case d.Status == Applied || d.Status == Invalidated || d.Status == Truncated:
					continue
				case d.Status.decided() && c.ExecuteAt.Less(d.ExecuteAt) && (c.Txn == nil || c.Txn.Kind != SyncPoint):
					continue // it executes after this one (a sync point waits for every dependency)
				}
			}
			if (d == nil || d.Status < PreAccepted) && id.Timestamp.Less(boundFor(s.redundant, k)) {
				continue // forgotten: durable
			}
			if (d == nil || d.Status < PreAccepted) && id.Timestamp.Less(boundFor(s.bootstrapped, k)) {
				if !slices.Contains(*skipped, id) {
					*skipped = append(*skipped, id)
				}
				continue
			}
			return id, true
		}
	}
	return TxnID{}, false
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
	// Record it applied (the group commit writes it, retrying failures).
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.cmds[id]
	if c == nil {
		return
	}
	c.executing = false
	if c.Status != PreApplied {
		return
	}
	c.Status, c.appliedAt = Applied, s.n.cfg.Now()
	wAt := c.writeAt()
	s.witnessLocked(c, wAt)
	if c.ID.Kind != SyncPoint {
		for _, k := range c.owned {
			if f := s.keys[keyString(k)]; f != nil {
				f.maxAppliedHLC = max(f.maxAppliedHLC, wAt.HLC)
			}
		}
	}
	_ = s.saveLocked(c)
	delete(s.waiters, id)
}

func (s *store) beginRecovery(r *recoverReq) (*recoverReply, error) {
	s.mu.Lock()
	rep, err := s.beginRecoveryLocked2(r)
	if uerr := s.unlock(); err == nil {
		err = uerr
	}
	return rep, err
}

// beginRecovery answers a recovery coordinator (BeginRecovery): it promises
// the ballot, pre-accepts the transaction if it had not been (without a
// fast path vote), and reports what it knows, including whether a
// transaction that executes later has not witnessed this one.
func (s *store) beginRecoveryLocked2(r *recoverReq) (*recoverReply, error) {
	c := s.get(r.ID)
	if r.Ballot.Less(c.Promised) {
		return &recoverReply{Outcome: rejectedBallot}, nil
	}
	unknown := c.Status == NotDefined
	if unknown && s.belowRedundantLocked(r.ID, r.Keys) {
		// Forgotten (or refused) long ago: durable, or never committed.
		return &recoverReply{Outcome: ok, Status: Truncated}, nil
	}
	refused := unknown && s.belowBarrierLocked(r.ID, r.Keys)
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
	rep := &recoverReply{Outcome: ok, Status: c.Status, Accepted: c.Accepted, ExecuteAt: c.ExecuteAt, UniqueHLC: c.UniqueHLC, HLCBallot: c.HLCBallot, Deps: c.Deps,
		LocalDeps: c.LocalDeps, FastVote: c.FastVote, Txn: c.Txn, Writes: c.Writes, Result: c.Result, Rejects: refused}
	if c.Status >= Accepted {
		return rep, nil
	}
	// The superseding transactions (BeginRecovery's visitor).
	for _, k := range c.owned {
		ks := keyString(k)
		for _, id := range s.keyLocked(k).ids {
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

func (s *store) checkStatus(id TxnID, keys []Key) *checkStatusReply {
	s.mu.Lock()
	defer func() { _ = s.unlock() }() // answer only with durable state
	c := s.cmds[id]
	if c == nil || c.Status == NotDefined {
		if len(keys) > 0 && s.belowRedundantLocked(id, keys) {
			var bs []bound
			for _, b := range s.redundant {
				if slices.ContainsFunc(keys, b.R.contains) && id.Timestamp.Less(b.TS) {
					bs = append(bs, b)
				}
			}
			return &checkStatusReply{Status: Truncated, Redundant: bs}
		}
		if c == nil {
			return &checkStatusReply{}
		}
	}
	return &checkStatusReply{Status: c.Status, Promised: c.Promised, Accepted: c.Accepted, ExecuteAt: c.ExecuteAt,
		UniqueHLC: c.UniqueHLC, Deps: c.Deps, Txn: c.Txn, Keys: c.Keys, Writes: c.Writes, Result: c.Result}
}

func (s *store) informDurable(id TxnID) error {
	s.mu.Lock()
	err := s.informDurableLocked2(id)
	if uerr := s.unlock(); err == nil {
		err = uerr
	}
	return err
}

func (s *store) informDurableLocked2(id TxnID) error {
	c := s.cmds[id]
	// Kept once the outcome is known here (PreApplied: the writes may
	// still wait for a dependency); truncation still waits for Applied.
	if c == nil || c.Durable || c.Status < PreApplied || c.Status == Invalidated || c.Status == Truncated {
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
	s.compactMajorityLocked()
	now := s.n.cfg.Now()
	for _, c := range s.cmds {
		if c.Status != Applied || !c.Durable || now.Sub(c.appliedAt) < s.n.cfg.RetainApplied {
			continue
		}
		for _, k := range c.owned {
			f := s.keyLocked(k)
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
		for _, rec := range []*Command{s.bootstrapRec, s.syncedRec} {
			if rec != nil {
				all = append(all, rec)
			}
		}
		if s.gc != nil && !s.gc.isDurable(s.lastSeq) {
			return // compacted at a later tick, once the queue is written
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
		if saved.Bootstrap {
			s.bootstrapRec = saved
			s.bootstrapped = slices.Clone(saved.Bounds)
			for _, b := range saved.Bounds {
				s.n.clock.Observe(max(b.TS.HLC, b.MaxHLC))
			}
			continue
		}
		if saved.SyncedEpoch > 0 {
			s.syncedRec = saved
			s.n.synced.Store(max(s.n.synced.Load(), saved.SyncedEpoch))
			continue
		}
		c := &command{Command: *saved, changed: make(chan struct{}), lastChange: now, appliedAt: now}
		s.cmds[c.ID] = c
		s.n.clock.Observe(max(c.ID.HLC, c.ExecuteAt.HLC, c.UniqueHLC))
		if c.Txn != nil && c.Txn.Kind == SyncPoint && c.Status != Invalidated {
			for _, r := range c.Txn.Ranges {
				s.barriers = addBound(s.barriers, r, c.ID.Timestamp)
				if c.Redundant {
					s.redundant = addBound(s.redundant, r, c.ID.Timestamp)
				}
			}
		}
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
			s.witnessLocked(c, c.writeAt())
			if c.Status == Applied && c.ID.Kind != SyncPoint {
				for _, k := range c.owned {
					if f := s.keys[keyString(k)]; f != nil {
						f.maxAppliedHLC = max(f.maxAppliedHLC, c.writeHLC())
					}
				}
			}
		}
	}
	for _, c := range s.cmds {
		s.maybeExecuteLocked(c)
	}
}

func (s *store) fastCommit(id TxnID, txn *Txn, deps Deps) bool {
	s.mu.Lock()
	ok := s.fastCommitLocked2(id, txn, deps)
	return s.unlock() == nil && ok
}

// fastCommit is the privileged coordinator's local decision of the fast
// path: it commits only if no recovery has promised a ballot here, which a
// recovery that reaches this node relies on to invalidate safely.
func (s *store) fastCommitLocked2(id TxnID, txn *Txn, deps Deps) bool {
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
	defer func() { _ = s.unlock() }() // answer only with durable state
	d := Deps{}
	var fs []*cfk
	for _, k := range s.n.owned(keys, 0) {
		if f := s.keys[keyString(k)]; f != nil {
			fs = append(fs, f)
		}
	}
	for _, r := range ranges {
		// The keys this replica indexes in the range, by token.
		i := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i].key.Token >= r.Lo })
		for ; i < len(s.sorted) && s.sorted[i].key.Token <= r.Hi; i++ {
			if f := s.sorted[i]; r.contains(f.key) {
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
	return &ephemeralReply{Outcome: ok, Deps: d, Epoch: s.n.epoch()}
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
				if s.cmds[id] == nil && id.Timestamp.Less(boundFor(s.redundant, k)) {
					continue // forgotten: durable
				}
				c := s.get(id)
				if c.Status == Applied || c.Status == Invalidated || c.Status == Truncated {
					continue
				}
				if len(c.Keys) == 0 {
					s.registerLocked(c, []Key{k})
				}
				if c.Status < PreAccepted && id.Timestamp.Less(boundFor(s.redundant, k)) {
					continue // forgotten: durable
				}
				if c.Status < PreAccepted && id.Timestamp.Less(boundFor(s.bootstrapped, k)) {
					// From before this node joined: another replica reads.
					s.missingLocked([]TxnID{id}, []Key{k}, nil)
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
		if !fetched[wait.ID] && s.n.cfg.Now().Sub(start) > s.n.stallAfter(wait.ID, s.n.cfg.RecoverAfter) {
			fetched[wait.ID] = true
			id, keys := wait.ID, slices.Clone(wait.Keys)
			s.n.background(func(ctx context.Context) {
				if _, busy := s.n.recovering.LoadOrStore(id, true); busy {
					return
				}
				defer s.n.recovering.Delete(id)
				fctx, cancel := context.WithTimeout(ctx, s.n.recoveryTimeout())
				defer cancel()
				if !s.n.fetch(fctx, id, keys) {
					_ = s.n.recover(fctx, id, nil, keys)
				}
			})
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
	// Every dependency is applied here: the read needs no lock.
	s.mu.Unlock()
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

// forgetLocked drops the tombstones of transactions truncated or
// invalidated long ago. A key's largest witnessed timestamp is kept, so a
// late message about a forgotten transaction cannot make it pass the fast
// path again: it is recovered and invalidated.
func (s *store) forgetLocked() {
	now := s.n.cfg.Now()
	for id, c := range s.cmds {
		terminal := c.Status == Applied || c.Status == Invalidated || c.Status == Truncated
		var drop bool
		switch {
		case c.Status == NotDefined && c.Promised.IsZero() && len(c.Keys) > 0 && len(s.redundant) > 0 && s.belowRedundantLocked(id, c.Keys):
			drop = true // a placeholder for a dependency made redundant
		case !terminal && !c.Status.decided() && c.ID.Kind != SyncPoint && len(c.Keys) > 0 && len(s.redundant) > 0 && s.belowRedundantLocked(id, c.Keys):
			// Undecided below the redundancy bound: had it been a dependency
			// of the durable sync point, that sync point could not have
			// applied here before it. So it never commits: invalidated here.
			c.Status = Invalidated
			_ = s.saveLocked(c)
		case !terminal:
		case c.Txn != nil && c.Txn.Kind == SyncPoint:
			// A sync point superseded by a newer redundancy bound on every
			// one of its ranges.
			drop = len(s.redundant) > 0
			for _, r := range c.Txn.Ranges {
				newer := false
				for _, b := range s.redundant {
					if b.R == r && id.Timestamp.Less(b.TS) {
						newer = true
					}
				}
				drop = drop && newer
			}
		case len(s.redundant) > 0 && len(c.Keys) > 0 && s.belowRedundantLocked(id, c.Keys):
			// RedundantBefore: every replica applied it (or it never
			// committed); it can never be needed again.
			drop = true
		case s.n.cfg.DurabilityRanges == nil:
			// Without durability rounds: forget truncated or invalidated
			// records after a retention period.
			drop = (c.Status == Truncated || c.Status == Invalidated) && now.Sub(c.lastChange) >= s.n.cfg.TombstoneRetention
		}
		if !drop {
			continue
		}
		for _, k := range c.owned {
			if f := s.keys[keyString(k)]; f != nil {
				if i, found := slices.BinarySearchFunc(f.ids, id, TxnID.Compare); found {
					f.ids = slices.Delete(f.ids, i, i+1)
				}
			}
		}
		if s.forgetRecordLocked(id) == nil {
			delete(s.cmds, id)
			delete(s.waiters, id)
		}
	}
	// A key with no transactions left whose latest timestamp is below its
	// redundancy bound is forgotten too: the barrier refuses anything that
	// timestamp would have ordered.
	if len(s.redundant) > 0 {
		s.sorted = slices.DeleteFunc(s.sorted, func(f *cfk) bool {
			b := boundFor(s.redundant, f.key)
			if len(f.ids) == 0 && f.maxTS.Less(b) && f.maxAppliedHLC < b.HLC {
				delete(s.keys, keyString(f.key))
				return true
			}
			return false
		})
	}
}

// missingLocked reports whether some of ids are not applied here yet, and
// fetches them in the background.
// deps, if not nil, are the dependencies ids come from: each is fetched on
// the keys it is a dependency on, which it touches (a recovery on other
// keys could wrongly invalidate it), else on keys.
func (s *store) missingLocked(ids []TxnID, keys []Key, deps Deps) bool {
	missing := false
	for _, id := range ids {
		d := s.cmds[id]
		if d != nil && (d.Status == Applied || d.Status == Invalidated || d.Status == Truncated) {
			continue
		}
		missing = true
		on := depKeys(deps, id)
		if len(on) == 0 && d != nil && len(d.Keys) > 0 {
			on = slices.Clone(d.Keys)
		}
		if len(on) == 0 {
			on = slices.Clone(keys)
		}
		s.n.background(func(ctx context.Context) { s.n.fetchOnce(ctx, id, on) })
	}
	return missing
}

// depKeys returns the keys deps holds id on: keys the transaction touches.
func depKeys(deps Deps, id TxnID) []Key {
	var out []Key
	for ks, ids := range deps {
		if slices.Contains(ids, id) {
			if k, ok := parseKeyString(ks); ok {
				out = append(out, k)
			}
		}
	}
	slices.SortFunc(out, Key.Compare)
	return out
}

// sameKeys reports whether a and b hold the same keys.
func sameKeys(a, b []Key) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.SortFunc(a, Key.Compare)
	slices.SortFunc(b, Key.Compare)
	return slices.EqualFunc(a, b, func(x, y Key) bool { return x.Compare(y) == 0 })
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

// belowBarrierLocked reports whether a transaction's id is below a sync
// point witnessed on one of its keys here.
func (s *store) belowBarrierLocked(id TxnID, keys []Key) bool {
	if len(s.barriers) == 0 {
		return false
	}
	for _, k := range s.n.owned(keys, id.Epoch) {
		if id.Timestamp.Less(boundFor(s.barriers, k)) {
			return true
		}
	}
	return false
}

// belowRedundantLocked reports whether a transaction's id is below the
// redundancy bound on all of its keys here.
// forgottenLocked reports whether a transaction this replica does not
// know is below its redundancy bound (so it was forgotten here).
func (s *store) forgottenLocked(id TxnID, keys []Key) bool {
	if c := s.cmds[id]; c != nil && c.Status != NotDefined {
		return false
	}
	return id.Kind != SyncPoint && len(keys) > 0 && s.belowRedundantLocked(id, keys)
}

func (s *store) belowRedundantLocked(id TxnID, keys []Key) bool {
	owned := s.n.owned(keys, id.Epoch)
	if len(s.redundant) == 0 || len(owned) == 0 {
		return false
	}
	for _, k := range owned {
		if !id.Timestamp.Less(boundFor(s.redundant, k)) {
			return false
		}
	}
	return true
}

// forgetRecordLocked removes a command's journal record.
func (s *store) forgetRecordLocked(id TxnID) error {
	if s.gc != nil {
		s.lastSeq = s.gc.enqueue(Record{ID: id})
		return nil
	}
	return s.n.cfg.Journal.Delete(id)
}

// unlock releases the store lock, then waits until every journal record
// queued so far is durable: a handler answers only after its changes are.
func (s *store) unlock() error {
	seq := s.lastSeq
	s.mu.Unlock()
	if s.gc == nil || seq == 0 {
		return nil
	}
	return s.gc.wait(seq)
}
