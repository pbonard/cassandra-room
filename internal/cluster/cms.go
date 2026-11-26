package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// CMS is the Cluster Metadata Service of Cassandra's CEP-21 (Transactional
// Cluster Metadata): cluster metadata changes form a log of epochs, and the
// entry of each epoch is chosen by Paxos among the CMS members
// (PaxosBackedProcessor), which linearizes them. An entry holds only the
// change (a transformation's effect on the metadata before it), and every
// node applies the entries in epoch order (LocalLog). A node that falls
// behind fetches the entries it misses from a peer (FetchPeerLog,
// FetchCMSLog), or a full snapshot when the peer no longer has them all
// (TCM's ForceSnapshot).
//
// The acceptors of an epoch are the members recorded in the metadata of
// the epoch before, read together with it, so every proposer for an epoch
// asks the same members.
//
// Any node may propose: it computes the next entry from its current
// metadata and tries to get it chosen for the next epoch; if another entry
// was chosen there, it applies that one and plans its change again.
type CMS struct {
	m     *Messaging
	self  string
	store Store

	mu        sync.Mutex
	acceptors map[int64]*acceptor // Paxos state per epoch, for members
	round     int64
	// persist, if set, makes the acceptor state durable before a promise
	// or acceptance is answered (TCM keeps it in system tables): a member
	// that restarts must not forget what it promised.
	persist func([]byte) error
	// synced, if set, answers a sync once the node has caught up: the epoch
	// it reached, once the requests planned before its placements are done
	// (a progress barrier's acknowledgement).
	synced func(ctx context.Context) (int64, error)
	// catchingUp serializes catching up.
	catchingUp sync.Mutex
}

// Entry is the change committed at an epoch, encoded by the Store.
type Entry struct {
	Epoch int64
	Value []byte
}

// Store is a node's metadata, as the CMS sees it.
type Store interface {
	// Current returns the local epoch, its metadata, and the CMS members
	// it records (the acceptors of the next epoch).
	Current() (epoch int64, meta []byte, members []string)
	// Contains reports whether meta includes the change with an entry id
	// (TCM's Entry.Id, for idempotence: a proposal can be chosen through
	// another proposer without its own knowing).
	Contains(meta []byte, id string) (bool, error)
	// Append applies the entry of the epoch after the current one; it
	// ignores entries of past epochs and returns ErrGap for later ones.
	Append(e Entry) error
	// Since returns the entries after an epoch, or false if the log no
	// longer holds them all.
	Since(epoch int64) ([]Entry, bool)
	// Snapshot installs full metadata, if it is newer than the current.
	Snapshot(meta []byte) error
}

// ErrGap is returned by Store.Append for an entry past the next epoch.
var ErrGap = errors.New("cluster: metadata entry past the next epoch")

// fetchRequest asks a node for the metadata log after an epoch.
type fetchRequest struct{ Since int64 }

// fetchResponse holds the entries asked for, or a snapshot.
type fetchResponse struct {
	Entries  []Entry `json:",omitempty"`
	Snapshot []byte  `json:",omitempty"`
}

// SetPersistence restores the acceptor state saved by persist and makes
// every later change go through it. Call before the messaging starts.
func (c *CMS) SetPersistence(saved []byte, persist func([]byte) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(saved) > 0 {
		if err := json.Unmarshal(saved, &c.acceptors); err != nil {
			return fmt.Errorf("cluster: corrupt metadata service state: %w", err)
		}
		if c.acceptors == nil {
			c.acceptors = map[int64]*acceptor{}
		}
	}
	c.persist = persist
	return nil
}

func (c *CMS) persistLocked() error {
	if c.persist == nil {
		return nil
	}
	b, err := json.Marshal(c.acceptors)
	if err != nil {
		return err
	}
	return c.persist(b)
}

type ballot struct {
	Round int64
	Node  string
}

func (b ballot) less(o ballot) bool {
	return b.Round < o.Round || b.Round == o.Round && b.Node < o.Node
}

type acceptor struct {
	Promised ballot
	Accepted ballot
	Value    []byte
}

type paxosReq struct {
	Epoch  int64
	Ballot ballot
	Value  []byte `json:",omitempty"`
}

type paxosResp struct {
	OK bool
	// Behind is set when the acceptor already has this epoch committed:
	// the proposer must catch up first.
	Behind bool
	// Lagging is set when the acceptor misses epochs before this one: it
	// does not vote until it has caught up (it does so in the background),
	// so that every member counted in a quorum knows the log, and the
	// members, that the epoch builds on.
	Lagging  bool
	Accepted ballot
	Value    []byte
	Promised ballot
}

// ErrNoQuorum is returned when a majority of the CMS cannot be reached.
var ErrNoQuorum = errors.New("cluster: no quorum of the cluster metadata service")

// NewCMS creates the metadata service client (and acceptor, on members).
func NewCMS(m *Messaging, store Store) *CMS {
	c := &CMS{m: m, self: m.Addr(), store: store, acceptors: map[int64]*acceptor{}}
	m.Handle(VerbPaxosPrepare, c.handlePrepare)
	m.Handle(VerbPaxosAccept, c.handleAccept)
	m.Handle(VerbMetadataFetch, c.handleFetch)
	m.Handle(VerbMetadataPush, c.handlePush)
	m.Handle(VerbMetadataSync, c.handleSync)
	return c
}

// Members returns the CMS members.
func (c *CMS) Members() []string {
	_, _, members := c.store.Current()
	return members
}

func (c *CMS) epoch() int64 {
	e, _, _ := c.store.Current()
	return e
}

func (c *CMS) acceptorLocked(epoch int64) *acceptor {
	a := c.acceptors[epoch]
	if a == nil {
		a = &acceptor{}
		c.acceptors[epoch] = a
	}
	// Forget the state of epochs long committed.
	cur := c.epoch()
	for e := range c.acceptors {
		if e < cur-16 {
			delete(c.acceptors, e)
		}
	}
	return a
}

func (c *CMS) handlePrepare(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var r paxosReq
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	if c.epoch() >= r.Epoch {
		return json.Marshal(paxosResp{Behind: true})
	}
	if c.epoch() < r.Epoch-1 {
		go func() { _ = c.CatchUp(from) }()
		return json.Marshal(paxosResp{Lagging: true})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.acceptorLocked(r.Epoch)
	if r.Ballot.less(a.Promised) {
		return json.Marshal(paxosResp{Promised: a.Promised})
	}
	if a.Promised.less(r.Ballot) {
		old := *a
		a.Promised = r.Ballot
		if err := c.persistLocked(); err != nil {
			*a = old // not promised: nothing durable says so
			return nil, err
		}
	}
	return json.Marshal(paxosResp{OK: true, Accepted: a.Accepted, Value: a.Value})
}

func (c *CMS) handleAccept(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var r paxosReq
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	if c.epoch() >= r.Epoch {
		return json.Marshal(paxosResp{Behind: true})
	}
	if c.epoch() < r.Epoch-1 {
		go func() { _ = c.CatchUp(from) }()
		return json.Marshal(paxosResp{Lagging: true})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.acceptorLocked(r.Epoch)
	if r.Ballot.less(a.Promised) {
		return json.Marshal(paxosResp{Promised: a.Promised})
	}
	old := *a
	a.Promised, a.Accepted, a.Value = r.Ballot, r.Ballot, r.Value
	if err := c.persistLocked(); err != nil {
		*a = old // not accepted: nothing durable says so
		return nil, err
	}
	return json.Marshal(paxosResp{OK: true})
}

// handleFetch serves the log after an epoch, or a snapshot. An empty
// request asks for a snapshot.
func (c *CMS) handleFetch(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var r fetchResponse
	if len(payload) > 0 {
		var q fetchRequest
		if err := json.Unmarshal(payload, &q); err != nil {
			return nil, err
		}
		if entries, ok := c.store.Since(q.Since); ok {
			r.Entries = entries
			return json.Marshal(r)
		}
	}
	_, r.Snapshot, _ = c.store.Current()
	return json.Marshal(r)
}

// handlePush applies an entry pushed after a commit; past a gap, the node
// catches up from the sender (in the background: the sender waits on this
// answer, and catching up asks it in turn).
func (c *CMS) handlePush(_ context.Context, from string, payload []byte) ([]byte, error) {
	var e Entry
	if err := json.Unmarshal(payload, &e); err != nil {
		return nil, err
	}
	err := c.store.Append(e)
	if errors.Is(err, ErrGap) {
		go func() { _ = c.CatchUp(from) }()
		return nil, nil
	}
	return nil, err
}

// handleSync catches up from the node named in the request and returns
// the epoch reached: a node about to join the metadata service is brought
// up to date first.
func (c *CMS) handleSync(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	if err := c.CatchUp(string(payload)); err != nil {
		return nil, err
	}
	if c.synced != nil {
		e, err := c.synced(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(e)
	}
	return json.Marshal(c.epoch())
}

// SetSynced sets what answers a sync once caught up (see CMS.synced).
func (c *CMS) SetSynced(f func(ctx context.Context) (int64, error)) { c.synced = f }

// Sync asks node to catch up from this one, and returns its epoch.
func (c *CMS) Sync(ctx context.Context, node string) (int64, error) {
	if node == c.self {
		if c.synced != nil {
			return c.synced(ctx)
		}
		return c.epoch(), nil
	}
	b, err := c.m.Request(ctx, node, VerbMetadataSync, []byte(c.self))
	if err != nil {
		return 0, err
	}
	var e int64
	err = json.Unmarshal(b, &e)
	return e, err
}

// CatchUp fetches what this node misses of the log from a node: the
// entries after its epoch, replayed in order, or a snapshot.
func (c *CMS) CatchUp(from string) error {
	c.catchingUp.Lock()
	defer c.catchingUp.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q, _ := json.Marshal(fetchRequest{Since: c.epoch()})
	b, err := c.m.Request(ctx, from, VerbMetadataFetch, q)
	if err != nil {
		return err
	}
	var r fetchResponse
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.Snapshot != nil {
		return c.store.Snapshot(r.Snapshot)
	}
	for _, e := range r.Entries {
		if err := c.store.Append(e); err != nil {
			if !errors.Is(err, ErrGap) {
				// A diverged or unreadable entry: start again from the
				// sender's full metadata.
				return c.snapshotFrom(ctx, from)
			}
			return err
		}
	}
	return nil
}

func (c *CMS) snapshotFrom(ctx context.Context, from string) error {
	b, err := c.m.Request(ctx, from, VerbMetadataFetch, nil)
	if err != nil {
		return err
	}
	var r fetchResponse
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	return c.store.Snapshot(r.Snapshot)
}

// broadcast sends a request to every member, collecting the answers.
func (c *CMS) broadcast(ctx context.Context, members []string, verb Verb, req paxosReq) []paxosResp {
	b, _ := json.Marshal(req)
	type res struct {
		r   paxosResp
		err error
	}
	ch := make(chan res, len(members))
	for _, mb := range members {
		go func() {
			out, err := c.m.Request(ctx, mb, verb, b)
			var r paxosResp
			if err == nil {
				err = json.Unmarshal(out, &r)
			}
			ch <- res{r, err}
		}()
	}
	var out []paxosResp
	for range members {
		if r := <-ch; r.err == nil {
			out = append(out, r.r)
		}
	}
	return out
}

// Commit commits a metadata change: plan computes the entry of the next
// epoch from the current metadata, recording the entry id (it may be
// called several times, on newer metadata, if other changes win the race).
// It returns the epoch at which the change is committed.
func (c *CMS) Commit(ctx context.Context, plan func(meta []byte, id string) ([]byte, error)) (int64, error) {
	id := fmt.Sprintf("%s/%d/%d", c.self, time.Now().UnixNano(), rand.Int63())
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrNoQuorum, err)
		}
		if attempt > 0 {
			time.Sleep(time.Duration(rand.Intn(20*attempt)) * time.Millisecond)
		}
		// The epoch, its metadata and the members are read together: the
		// members of the epoch before decide the next one.
		epoch, cur, members := c.store.Current()
		if applied, err := c.store.Contains(cur, id); err != nil {
			return 0, err
		} else if applied {
			return epoch, nil // chosen through another proposer
		}
		next, err := plan(cur, id)
		if err != nil {
			return 0, err
		}
		slot := epoch + 1
		quorum := len(members)/2 + 1
		c.mu.Lock()
		c.round++
		b := ballot{Round: max(c.round, time.Now().UnixNano()), Node: c.self}
		c.round = b.Round
		c.mu.Unlock()

		// Phase 1: prepare.
		promises := c.broadcast(ctx, members, VerbPaxosPrepare, paxosReq{Epoch: slot, Ballot: b})
		var ok []paxosResp
		behind := false
		for _, p := range promises {
			if p.Behind {
				behind = true
			}
			if p.OK {
				ok = append(ok, p)
			}
			if p.Promised.Round > 0 {
				c.mu.Lock()
				c.round = max(c.round, p.Promised.Round)
				c.mu.Unlock()
			}
		}
		if behind {
			c.catchUpFromMembers(members)
			continue
		}
		if len(ok) < quorum {
			if len(promises) < quorum {
				return 0, ErrNoQuorum
			}
			continue // a higher ballot: retry
		}
		value := next
		var highest ballot
		for _, p := range ok {
			if p.Value != nil && highest.less(p.Accepted) {
				highest, value = p.Accepted, p.Value
			}
		}
		// Phase 2: accept.
		accepts := c.broadcast(ctx, members, VerbPaxosAccept, paxosReq{Epoch: slot, Ballot: b, Value: value})
		n := 0
		for _, a := range accepts {
			if a.OK {
				n++
			}
			if a.Behind {
				behind = true
			}
		}
		if behind && n < quorum {
			c.catchUpFromMembers(members)
			continue
		}
		if n < quorum {
			if len(accepts) < quorum {
				return 0, ErrNoQuorum
			}
			continue
		}
		// Chosen: apply it here and push it to the other members.
		chosen := Entry{Epoch: slot, Value: value}
		if err := c.store.Append(chosen); errors.Is(err, ErrGap) {
			c.catchUpFromMembers(members)
		} else if err != nil {
			return 0, err
		}
		go c.Push(members, chosen)
		if bytes.Equal(value, next) {
			return slot, nil
		}
		// Another proposal was chosen for this epoch: plan again on it.
	}
}

// catchUpFromMembers catches up from the members, asking each in turn until
// this node's epoch advances (one that answers may be no further ahead).
func (c *CMS) catchUpFromMembers(members []string) {
	start := c.epoch()
	for _, mb := range members {
		if mb != c.self && c.CatchUp(mb) == nil && c.epoch() > start {
			return
		}
	}
}

// CatchUpFromMembers brings this node up to date from the metadata
// service's members.
func (c *CMS) CatchUpFromMembers() {
	_, _, members := c.store.Current()
	for _, mb := range members {
		if mb != c.self {
			_ = c.CatchUp(mb)
		}
	}
}

// Push sends an entry to nodes (log replication after a commit).
func (c *CMS) Push(nodes []string, e Entry) {
	b, _ := json.Marshal(e)
	var wg sync.WaitGroup
	for _, n := range nodes {
		if n == c.self {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = c.m.Request(ctx, n, VerbMetadataPush, b)
		}()
	}
	wg.Wait()
}
