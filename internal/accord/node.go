package accord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Transport sends a request to a node and returns its reply.
type Transport interface {
	Request(ctx context.Context, to NodeID, payload []byte) ([]byte, error)
}

// DataStore reads and writes a node's data for transactions
// (accord.api.DataStore, Read and Write).
type DataStore interface {
	// Read reads a key for a transaction, at its execution timestamp.
	Read(key Key, executeAt Timestamp, txn *Txn) ([]byte, error)
	// Apply applies a transaction's write to a key, timestamped with its
	// execution timestamp; applying the same write again must be harmless.
	Apply(key Key, executeAt Timestamp, txn *Txn, write []byte) error
}

// Agent computes a transaction's writes and result from what it read
// (accord.api.Update and Query); it must be deterministic.
type Agent interface {
	Execute(txn *Txn, executeAt Timestamp, reads map[string][]byte) (Writes, []byte, error)
}

// Config configures a node.
type Config struct {
	ID NodeID
	// Tag distinguishes the node's clock values (see Clock); nodes should
	// have different tags.
	Tag       int
	Now       func() time.Time
	Transport Transport
	Data      DataStore
	Agent     Agent
	// Journal makes command state durable; nil keeps it in memory.
	Journal Journal
	// ProgressInterval is how often the progress log looks for stalled
	// transactions; RecoverAfter is how long a transaction may stall
	// before this node fetches its outcome or recovers it. Defaults: 100ms
	// and 2s.
	ProgressInterval, RecoverAfter time.Duration
	// RetainApplied is how long a transaction applied at every replica
	// keeps its full state before it is truncated. Default: one minute.
	RetainApplied time.Duration
	// RequestTimeout bounds each request. Default: 2s.
	RequestTimeout time.Duration
}

// Errors returned by Coordinate.
var (
	// ErrInvalidated: the transaction was not executed (a recovery
	// invalidated it); it may be retried.
	ErrInvalidated = errors.New("accord: transaction invalidated")
	// ErrTimeout: the outcome is unknown.
	ErrTimeout = errors.New("accord: transaction outcome unknown")

	errStopped = errors.New("accord: node stopped")
)

// Node is an Accord node: a replica of the keys it owns (its command
// store) and a coordinator of transactions.
type Node struct {
	cfg   Config
	clock *Clock
	s     *store

	topoMu sync.RWMutex
	topos  map[int64]*Topology
	latest int64

	stop    chan struct{}
	stopped atomic.Bool
	wg      sync.WaitGroup
	// recovering marks transactions this node is recovering or fetching.
	recovering sync.Map
}

// NewNode creates a node and replays its journal.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 100 * time.Millisecond
	}
	if cfg.RecoverAfter <= 0 {
		cfg.RecoverAfter = 2 * time.Second
	}
	if cfg.RetainApplied <= 0 {
		cfg.RetainApplied = time.Minute
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Journal == nil {
		cfg.Journal = &MemJournal{}
	}
	n := &Node{cfg: cfg, clock: NewClock(cfg.Tag, cfg.Now), topos: map[int64]*Topology{}, stop: make(chan struct{})}
	n.s = newStore(n)
	if err := n.s.replay(); err != nil {
		return nil, err
	}
	return n, nil
}

// ID returns the node's id.
func (n *Node) ID() NodeID { return n.cfg.ID }

// SetTopology adds the topology of an epoch (TopologyManager). Recent
// epochs are kept, so transactions started before a change still find
// their replicas.
func (n *Node) SetTopology(t *Topology) {
	n.topoMu.Lock()
	n.topos[t.Epoch] = t
	n.latest = max(n.latest, t.Epoch)
	for e := range n.topos {
		if e < n.latest-64 {
			delete(n.topos, e)
		}
	}
	n.topoMu.Unlock()
	n.s.mu.Lock()
	n.s.rescanLocked()
	n.s.mu.Unlock()
}

// epoch returns the newest topology epoch known.
func (n *Node) epoch() int64 {
	n.topoMu.RLock()
	defer n.topoMu.RUnlock()
	return n.latest
}

// topologies returns the shards of keys over epochs [from, to], skipping
// epochs this node does not know (and using the newest if it knows none).
func (n *Node) topologies(keys []Key, from, to int64) *Topologies {
	n.topoMu.RLock()
	defer n.topoMu.RUnlock()
	ts := &Topologies{}
	for e := max(from, n.latest-64); e <= to; e++ {
		if t := n.topos[e]; t != nil {
			ts.Shards = append(ts.Shards, t.shards(keys)...)
		}
	}
	if len(ts.Shards) == 0 {
		if t := n.topos[n.latest]; t != nil {
			ts.Shards = t.shards(keys)
		}
	}
	return ts
}

// owned returns the keys this node replicates at some epoch from from on.
func (n *Node) owned(keys []Key, from int64) []Key {
	ts := n.topologies(keys, from, n.epoch())
	var out []Key
	for _, s := range ts.Shards {
		if slices.Contains(s.Nodes, n.cfg.ID) {
			for _, k := range s.Keys {
				if !slices.ContainsFunc(out, func(o Key) bool { return o == k }) {
					out = append(out, k)
				}
			}
		}
	}
	slices.SortFunc(out, Key.Compare)
	return out
}

// newTxnID returns a new transaction id.
func (n *Node) newTxnID(kind Kind) TxnID {
	return TxnID{Timestamp: Timestamp{Epoch: n.epoch(), HLC: n.clock.Next(0), Node: n.cfg.ID}, Kind: kind}
}

// Start starts the progress log.
func (n *Node) Start() {
	n.wg.Add(1)
	go n.progressLog()
}

// Stop stops the node: it no longer sends or answers requests.
func (n *Node) Stop() {
	n.stopped.Store(true)
	close(n.stop)
	n.wg.Wait()
}

// ---- messages ----

type msgType string

const (
	mPreAccept     msgType = "PreAccept"
	mAccept        msgType = "Accept"
	mCommit        msgType = "Commit"
	mApply         msgType = "Apply"
	mRecover       msgType = "BeginRecovery"
	mCheckStatus   msgType = "CheckStatus"
	mInformDurable msgType = "InformDurable"
)

type envelope struct {
	T msgType
	B json.RawMessage
}

// outcome of a request on a replica (accord.local.Commands.AcceptOutcome).
type outcome uint8

const (
	ok outcome = iota
	rejectedBallot
	// redundant: the transaction is already decided (or truncated).
	redundant
	// notReady: the replica could not finish in time (a read or an apply
	// waiting on dependencies).
	notReady
)

type preAcceptReq struct {
	ID     TxnID
	Txn    *Txn
	Ballot Ballot
}

type preAcceptReply struct {
	Outcome   outcome
	ExecuteAt Timestamp
	Deps      Deps
	FastVote  bool
}

type acceptReq struct {
	ID         TxnID
	Ballot     Ballot
	ExecuteAt  Timestamp
	Deps       Deps
	Txn        *Txn
	Keys       []Key
	Invalidate bool
}

type acceptReply struct {
	Outcome  outcome
	Deps     Deps
	Promised Ballot
}

type commitReq struct {
	ID        TxnID
	Ballot    Ballot
	ExecuteAt Timestamp
	Deps      Deps
	Txn       *Txn
	Keys      []Key
	Stable    bool
	// Read asks for the replica's reads once it can execute
	// (StableThenRead).
	Read       bool
	Invalidate bool
}

type commitReply struct {
	Outcome outcome
	Reads   map[string][]byte
	// Applied, Writes, Result: the transaction was already applied here.
	Applied bool
	Writes  Writes
	Result  []byte
}

type applyReq struct {
	ID        TxnID
	ExecuteAt Timestamp
	Deps      Deps
	Txn       *Txn
	Writes    Writes
	Result    []byte
}

type applyReply struct {
	Outcome outcome
}

type recoverReq struct {
	ID     TxnID
	Ballot Ballot
	Txn    *Txn
	Keys   []Key
}

type recoverReply struct {
	Outcome   outcome
	Status    Status
	Accepted  Ballot
	ExecuteAt Timestamp
	Deps      Deps
	LocalDeps Deps
	FastVote  bool
	// Rejects: a transaction that executes later did not witness this one,
	// so it cannot have taken the fast path (supersedingRejects).
	Rejects bool
	// Wait: transactions that may execute later and have not decided their
	// dependencies yet; recovery must wait for them (simpleWait).
	Wait   []TxnID
	Txn    *Txn
	Writes Writes
	Result []byte
}

type checkStatusReq struct {
	ID TxnID
}

type checkStatusReply struct {
	Status    Status
	Promised  Ballot
	Accepted  Ballot
	ExecuteAt Timestamp
	Deps      Deps
	Txn       *Txn
	Keys      []Key
	Writes    Writes
	Result    []byte
}

// Handle answers a request from another node.
func (n *Node) Handle(ctx context.Context, from NodeID, payload []byte) ([]byte, error) {
	if n.stopped.Load() {
		return nil, errStopped
	}
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, err
	}
	var reply any
	var err error
	switch env.T {
	case mPreAccept:
		var r preAcceptReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			reply, err = n.s.preAccept(&r)
		}
	case mAccept:
		var r acceptReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			reply, err = n.s.accept(&r)
		}
	case mCommit:
		var r commitReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			reply, err = n.s.commit(ctx, &r)
		}
	case mApply:
		var r applyReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			reply, err = n.s.apply(ctx, &r)
		}
	case mRecover:
		var r recoverReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			reply, err = n.s.beginRecovery(&r)
		}
	case mCheckStatus:
		var r checkStatusReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			reply = n.s.checkStatus(r.ID)
		}
	case mInformDurable:
		var r checkStatusReq
		if err = json.Unmarshal(env.B, &r); err == nil {
			err = n.s.informDurable(r.ID)
		}
	default:
		err = fmt.Errorf("accord: unknown message %q", env.T)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(reply)
}

// send sends a request to a node (this node's requests are handled
// directly) and decodes the reply.
func (n *Node) send(ctx context.Context, to NodeID, t msgType, req, reply any) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if n.stopped.Load() {
		return errStopped
	}
	payload, _ := json.Marshal(envelope{T: t, B: b})
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	var out []byte
	if to == n.cfg.ID {
		out, err = n.Handle(ctx, to, payload)
	} else {
		out, err = n.cfg.Transport.Request(ctx, to, payload)
	}
	if err != nil {
		return err
	}
	if reply == nil {
		return nil
	}
	return json.Unmarshal(out, reply)
}

// response is one node's reply to a broadcast.
type response[R any] struct {
	from  NodeID
	reply R
	err   error
}

// broadcast sends a request to nodes; replies arrive on the channel.
func broadcast[R any](ctx context.Context, n *Node, nodes []NodeID, t msgType, req func(NodeID) any) <-chan response[R] {
	ch := make(chan response[R], len(nodes))
	for _, to := range nodes {
		go func() {
			var r R
			err := n.send(ctx, to, t, req(to), &r)
			ch <- response[R]{from: to, reply: r, err: err}
		}()
	}
	return ch
}

// jitter returns d plus up to half of it, at random.
func jitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int64N(int64(d)/2+1))
}
