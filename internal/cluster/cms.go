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
// value of each epoch is chosen by Paxos among the CMS members
// (PaxosBackedProcessor), which linearizes them. Every node applies the
// epochs in order. Here an epoch's value is the full metadata after the
// change (a snapshot, as TCM's ForceSnapshot), so a node that falls behind
// catches up from any peer in one step (FetchPeerLog, FetchCMSLog).
//
// Any node may propose: it computes the next metadata from its current
// one and tries to get it chosen for the next epoch; if another value was
// chosen there, it adopts that one and plans its change again.
type CMS struct {
	m       *Messaging
	members []string
	self    string
	// current returns the local epoch and metadata; install installs a
	// newer metadata snapshot (ignoring older ones).
	current func() (int64, []byte)
	install func([]byte) error
	// info returns a metadata's epoch and whether it contains the change
	// with a given entry id (TCM's Entry.Id, for idempotence: a proposal
	// can be chosen through another proposer without its own knowing).
	info func(meta []byte, id string) (epoch int64, applied bool, err error)

	mu        sync.Mutex
	acceptors map[int64]*acceptor // Paxos state per epoch, for members
	round     int64
	// persist, if set, makes the acceptor state durable before a promise
	// or acceptance is answered (TCM keeps it in system tables): a member
	// that restarts must not forget what it promised.
	persist func([]byte) error
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
	Behind   bool
	Accepted ballot
	Value    []byte
	Promised ballot
}

// ErrNoQuorum is returned when a majority of the CMS cannot be reached.
var ErrNoQuorum = errors.New("cluster: no quorum of the cluster metadata service")

// NewCMS creates the metadata service client (and acceptor, on members).
func NewCMS(m *Messaging, members []string, current func() (int64, []byte), install func([]byte) error,
	info func(meta []byte, id string) (int64, bool, error)) *CMS {
	c := &CMS{m: m, members: members, self: m.Addr(), current: current, install: install, info: info, acceptors: map[int64]*acceptor{}}
	m.Handle(VerbPaxosPrepare, c.handlePrepare)
	m.Handle(VerbPaxosAccept, c.handleAccept)
	m.Handle(VerbMetadataFetch, c.handleFetch)
	m.Handle(VerbMetadataPush, c.handlePush)
	return c
}

// Members returns the CMS members.
func (c *CMS) Members() []string { return c.members }

func (c *CMS) acceptorLocked(epoch int64) *acceptor {
	a := c.acceptors[epoch]
	if a == nil {
		a = &acceptor{}
		c.acceptors[epoch] = a
	}
	// Forget the state of epochs long committed.
	cur, _ := c.current()
	for e := range c.acceptors {
		if e < cur-16 {
			delete(c.acceptors, e)
		}
	}
	return a
}

func (c *CMS) handlePrepare(_ string, payload []byte) ([]byte, error) {
	var r paxosReq
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	if cur, _ := c.current(); cur >= r.Epoch {
		return json.Marshal(paxosResp{Behind: true})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.acceptorLocked(r.Epoch)
	if r.Ballot.less(a.Promised) {
		return json.Marshal(paxosResp{Promised: a.Promised})
	}
	if a.Promised.less(r.Ballot) {
		a.Promised = r.Ballot
		if err := c.persistLocked(); err != nil {
			return nil, err
		}
	}
	return json.Marshal(paxosResp{OK: true, Accepted: a.Accepted, Value: a.Value})
}

func (c *CMS) handleAccept(_ string, payload []byte) ([]byte, error) {
	var r paxosReq
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	if cur, _ := c.current(); cur >= r.Epoch {
		return json.Marshal(paxosResp{Behind: true})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.acceptorLocked(r.Epoch)
	if r.Ballot.less(a.Promised) {
		return json.Marshal(paxosResp{Promised: a.Promised})
	}
	a.Promised, a.Accepted, a.Value = r.Ballot, r.Ballot, r.Value
	if err := c.persistLocked(); err != nil {
		return nil, err
	}
	return json.Marshal(paxosResp{OK: true})
}

func (c *CMS) handleFetch(string, []byte) ([]byte, error) {
	_, b := c.current()
	return b, nil
}

func (c *CMS) handlePush(_ string, payload []byte) ([]byte, error) {
	return nil, c.install(payload)
}

// CatchUp fetches the metadata from a node that is ahead.
func (c *CMS) CatchUp(from string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := c.m.Request(ctx, from, VerbMetadataFetch, nil)
	if err != nil {
		return err
	}
	return c.install(b)
}

// broadcast sends a request to every member, collecting the answers.
func (c *CMS) broadcast(ctx context.Context, verb Verb, req paxosReq) []paxosResp {
	b, _ := json.Marshal(req)
	type res struct {
		r   paxosResp
		err error
	}
	ch := make(chan res, len(c.members))
	for _, mb := range c.members {
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
	for range c.members {
		if r := <-ch; r.err == nil {
			out = append(out, r.r)
		}
	}
	return out
}

// Commit applies a metadata change: plan computes the next metadata from
// the current one, recording the entry id (it may be called several times,
// on newer metadata, if other changes win the race). It returns the epoch
// at which the change is committed.
func (c *CMS) Commit(ctx context.Context, plan func(current []byte, id string) ([]byte, error)) (int64, error) {
	quorum := len(c.members)/2 + 1
	id := fmt.Sprintf("%s/%d/%d", c.self, time.Now().UnixNano(), rand.Int63())
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrNoQuorum, err)
		}
		if attempt > 0 {
			time.Sleep(time.Duration(rand.Intn(20*attempt)) * time.Millisecond)
		}
		epoch, cur := c.current()
		if _, applied, err := c.info(cur, id); err != nil {
			return 0, err
		} else if applied {
			return epoch, nil // chosen through another proposer
		}
		next, err := plan(cur, id)
		if err != nil {
			return 0, err
		}
		slot := epoch + 1
		if e, _, err := c.info(next, id); err != nil || e != slot {
			return 0, fmt.Errorf("cluster: planned metadata has epoch %d, want %d", e, slot)
		}
		c.mu.Lock()
		c.round++
		b := ballot{Round: max(c.round, time.Now().UnixNano()), Node: c.self}
		c.round = b.Round
		c.mu.Unlock()

		// Phase 1: prepare.
		promises := c.broadcast(ctx, VerbPaxosPrepare, paxosReq{Epoch: slot, Ballot: b})
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
			c.catchUpFromMembers()
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
		accepts := c.broadcast(ctx, VerbPaxosAccept, paxosReq{Epoch: slot, Ballot: b, Value: value})
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
			c.catchUpFromMembers()
			continue
		}
		if n < quorum {
			if len(accepts) < quorum {
				return 0, ErrNoQuorum
			}
			continue
		}
		// Chosen: install it here and on the other members.
		if err := c.install(value); err != nil {
			return 0, err
		}
		for _, mb := range c.members {
			if mb != c.self {
				go func() {
					pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					_, _ = c.m.Request(pctx, mb, VerbMetadataPush, value)
				}()
			}
		}
		if bytes.Equal(value, next) {
			return slot, nil
		}
		// Another proposal was chosen for this epoch: plan again on it.
	}
}

func (c *CMS) catchUpFromMembers() {
	for _, mb := range c.members {
		if mb != c.self && c.CatchUp(mb) == nil {
			return
		}
	}
}

// Push sends metadata to nodes (dissemination after a commit).
func (c *CMS) Push(nodes []string, value []byte) {
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
			_, _ = c.m.Request(ctx, n, VerbMetadataPush, value)
		}()
	}
	wg.Wait()
}
