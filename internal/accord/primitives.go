// Package accord implements the Accord transaction protocol (CEP-15), after
// Apache Cassandra's accord library (cassandra-accord: accord.primitives,
// accord.local, accord.messages, accord.coordinate) and its integration in
// Cassandra (service/accord): leaderless, strict-serializable transactions
// over keys, committed in one round trip when uncontended (the fast path)
// and two otherwise, executed on every replica in timestamp order.
//
// A transaction gets a TxnID (a timestamp) from its coordinator. Replicas
// PreAccept it, each proposing an execution timestamp (the TxnID if nothing
// conflicting has a later one) and the conflicting transactions it must
// follow (its dependencies). If a fast quorum of every shard proposes the
// TxnID, it executes at the TxnID; otherwise the coordinator proposes the
// latest timestamp in an Accept round, which also recomputes dependencies.
// The decision is committed to a quorum before it is marked stable
// (CASSANDRA-18365's "deps stability": recovery then always finds the
// committed dependencies), then each replica executes the transaction once
// its dependencies that execute earlier have been applied: the coordinator
// reads from one replica per shard, computes the writes and the result, and
// sends them to every replica to apply.
//
// A transaction whose coordinator fails is recovered by any replica (the
// progress log): a ballot-numbered round finds whether it may have been
// committed, and completes it, or invalidates it.
package accord

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// NodeID is a node's messaging address.
type NodeID = string

// Timestamp orders transactions: epoch, then hybrid logical clock, then
// node (accord.primitives.Timestamp). A node's clock values are unique: a
// node's hlc values are strictly increasing and carry the node's tag in
// their low bits, so two timestamps from different nodes never share an
// hlc, and an execution timestamp's hlc can serve as the write timestamp of
// the transaction's updates (Timestamp.uniqueHlc).
type Timestamp struct {
	Epoch int64  `json:"e,omitempty"`
	HLC   int64  `json:"h,omitempty"`
	Node  NodeID `json:"n,omitempty"`
}

// Compare orders timestamps.
func (t Timestamp) Compare(o Timestamp) int {
	if c := cmp.Compare(t.Epoch, o.Epoch); c != 0 {
		return c
	}
	if c := cmp.Compare(t.HLC, o.HLC); c != 0 {
		return c
	}
	return strings.Compare(t.Node, o.Node)
}

func (t Timestamp) Less(o Timestamp) bool { return t.Compare(o) < 0 }
func (t Timestamp) IsZero() bool          { return t == Timestamp{} }

func (t Timestamp) String() string { return fmt.Sprintf("%d:%d:%s", t.Epoch, t.HLC, t.Node) }

func maxTimestamp(a, b Timestamp) Timestamp {
	if a.Less(b) {
		return b
	}
	return a
}

// Kind is a transaction's kind: reads do not conflict with reads.
type Kind uint8

const (
	Write Kind = iota
	Read
)

// TxnID identifies a transaction: the timestamp its coordinator gave it,
// and its kind (accord.primitives.TxnId).
type TxnID struct {
	Timestamp
	Kind Kind `json:"k,omitempty"`
	// Privileged: the coordinator (a replica) counts its own vote, and
	// decides the fast path locally only if no recovery promised a ballot
	// there first (TxnId.hasPrivilegedCoordinator).
	Privileged bool `json:"p,omitempty"`
}

func (id TxnID) Compare(o TxnID) int { return id.Timestamp.Compare(o.Timestamp) }

// conflicts reports whether two transactions on a common key conflict.
func (id TxnID) conflicts(o TxnID) bool { return id.Kind == Write || o.Kind == Write }

// Ballot orders recovery attempts; zero is the original coordinator's.
type Ballot = Timestamp

// Clock issues a node's unique timestamps (accord's hybrid logical clock).
type Clock struct {
	mu   sync.Mutex
	last int64
	tag  int64
	now  func() time.Time
}

// tagBits is how many low bits of an hlc carry the node's tag.
const tagBits = 10

// NewClock creates a clock whose values carry tag (taken modulo 1024).
func NewClock(tag int, now func() time.Time) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{tag: int64(tag) & (1<<tagBits - 1), now: now}
}

// Next returns a new hlc above both the clock's previous values and after.
func (c *Clock) Next(after int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := max(c.now().UnixMicro(), c.last+1, after+1)
	// Round up to the next value carrying the tag.
	v = (v-c.tag+(1<<tagBits)-1)>>tagBits<<tagBits + c.tag
	c.last = v
	return v
}

// Observe advances the clock past a timestamp seen from another node.
func (c *Clock) Observe(hlc int64) {
	c.mu.Lock()
	c.last = max(c.last, hlc)
	c.mu.Unlock()
}

// Key is a key a transaction touches: its token routes it to replicas, the
// rest identifies it (a table and a partition key).
type Key struct {
	Token int64  `json:"t"`
	ID    string `json:"i"`
}

func (k Key) Compare(o Key) int {
	if c := cmp.Compare(k.Token, o.Token); c != 0 {
		return c
	}
	return strings.Compare(k.ID, o.ID)
}

// Txn is a transaction's definition. Read and Update are interpreted by the
// application (the Agent and the DataStore); they must be deterministic so
// that any node can execute the transaction when recovering it.
type Txn struct {
	Kind   Kind   `json:"k,omitempty"`
	Keys   []Key  `json:"keys"`
	Read   []byte `json:"r,omitempty"`
	Update []byte `json:"u,omitempty"`
}

// Deps are, per key, the conflicting transactions a transaction must follow
// (accord.primitives.KeyDeps). Ids are sorted per key.
type Deps map[string][]TxnID

func keyString(k Key) string { return fmt.Sprintf("%d/%s", k.Token, k.ID) }

func (d Deps) add(k Key, id TxnID) {
	ks := keyString(k)
	ids := d[ks]
	i, found := slices.BinarySearchFunc(ids, id, TxnID.Compare)
	if !found {
		d[ks] = slices.Insert(ids, i, id)
	}
}

// with returns the union of two dependency sets.
func (d Deps) with(o Deps) Deps {
	out := Deps{}
	for k, ids := range d {
		out[k] = slices.Clone(ids)
	}
	for k, ids := range o {
		for _, id := range ids {
			i, found := slices.BinarySearchFunc(out[k], id, TxnID.Compare)
			if !found {
				out[k] = slices.Insert(out[k], i, id)
			}
		}
	}
	return out
}

func (d Deps) contains(ks string, id TxnID) bool {
	_, found := slices.BinarySearchFunc(d[ks], id, TxnID.Compare)
	return found
}

// all returns every dependency once.
func (d Deps) all() []TxnID {
	seen := map[TxnID]bool{}
	var out []TxnID
	for _, ids := range d {
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	slices.SortFunc(out, TxnID.Compare)
	return out
}

// Writes are a transaction's updates per key, to apply at its execution
// timestamp.
type Writes map[string][]byte
