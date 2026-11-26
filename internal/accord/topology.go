package accord

import (
	"slices"
	"strings"
)

// Topology places keys on replicas at an epoch (accord.topology.Topology).
// The replicas of a key form its shard; shards are told apart by their
// replica sets.
type Topology struct {
	Epoch    int64
	Replicas func(Key) []NodeID
	// Pending, if set, returns replicas a key is moving to (joining
	// nodes): they receive every transaction's writes, without voting.
	Pending func(Key) []NodeID
	// Nodes are every node of the epoch (for epoch synchronisation).
	Nodes []NodeID
	// RangeReplicas, if set, splits a range into sub-ranges each owned by
	// one replica set (for range reads).
	RangeReplicas func(Range) []RangeShard
}

// Shard is a set of replicas and the keys of a transaction they own
// (accord.topology.Shard).
type Shard struct {
	Epoch   int64
	Nodes   []NodeID
	Keys    []Key
	Pending []NodeID
}

func (s *Shard) slowQuorum() int { return len(s.Nodes) - (len(s.Nodes)-1)/2 }

// fastQuorum is Shard.simpleFastQuorumSize with every replica in the
// electorate and recovery quorums the size of slow ones: every recovery
// quorum then holds a majority of any fast quorum.
func (s *Shard) fastQuorum() int {
	rf := len(s.Nodes)
	return max((1+rf+rf-s.slowQuorum()+1)/2, rf/2+1)
}

// privilegedFastQuorum is Shard.privilegedWithoutDepsFastQuorumSize: the
// coordinator's own vote counts, so 2 of 3 replicas suffice.
func (s *Shard) privilegedFastQuorum() int {
	rf := len(s.Nodes)
	return max((rf+rf-s.slowQuorum()+1)/2, rf/2+1)
}

// shards groups keys by replica set at a topology.
func (t *Topology) shards(keys []Key) []*Shard {
	byNodes := map[string]*Shard{}
	var out []*Shard
	for _, k := range keys {
		nodes := slices.Clone(t.Replicas(k))
		slices.Sort(nodes)
		nodes = slices.Compact(nodes)
		var pending []NodeID
		if t.Pending != nil {
			for _, p := range t.Pending(k) {
				if !slices.Contains(pending, p) && !slices.Contains(nodes, p) {
					pending = append(pending, p)
				}
			}
			slices.Sort(pending)
		}
		// Keys share a shard only if they have the same replicas and the
		// same pending ones: a node pending on one key only (a joining node
		// taking over its range) must not own the shard's other keys, or it
		// waits on their transactions, which never reach it.
		id := strings.Join(nodes, ",") + "|" + strings.Join(pending, ",")
		s := byNodes[id]
		if s == nil {
			s = &Shard{Epoch: t.Epoch, Nodes: nodes, Pending: pending}
			byNodes[id] = s
			out = append(out, s)
		}
		s.Keys = append(s.Keys, k)
	}
	return out
}

// Topologies are the shards of a transaction's keys over a range of epochs
// (accord.topology.Topologies): a phase needs a quorum of every shard of
// every epoch, so a transaction is witnessed by replicas both before and
// after a topology change (CASSANDRA-18365: an Accept must also reach the
// replicas of newer epochs, or it misses their dependencies).
type Topologies struct {
	Shards []*Shard
}

// nodes returns every replica once.
func (ts *Topologies) nodes() []NodeID {
	var out []NodeID
	for _, s := range ts.Shards {
		out = append(out, s.Nodes...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// applyNodes returns every replica and pending replica once: the
// targets of a transaction's writes.
func (ts *Topologies) applyNodes() []NodeID {
	out := ts.nodes()
	for _, s := range ts.Shards {
		out = append(out, s.Pending...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// latest returns the shards of the newest epoch.
func (ts *Topologies) latest() []*Shard {
	var e int64
	for _, s := range ts.Shards {
		e = max(e, s.Epoch)
	}
	var out []*Shard
	for _, s := range ts.Shards {
		if s.Epoch == e {
			out = append(out, s)
		}
	}
	return out
}

// tracker counts the replies of a phase per shard (accord's
// QuorumTracker).
type tracker struct {
	shards  []*Shard
	ok      []map[NodeID]bool
	failed  []map[NodeID]bool
	quorum  func(*Shard) int
	members map[NodeID][]int
}

func newTracker(ts *Topologies, quorum func(*Shard) int) *tracker {
	t := &tracker{shards: ts.Shards, quorum: quorum, members: map[NodeID][]int{}}
	for i, s := range ts.Shards {
		t.ok = append(t.ok, map[NodeID]bool{})
		t.failed = append(t.failed, map[NodeID]bool{})
		for _, n := range s.Nodes {
			t.members[n] = append(t.members[n], i)
		}
	}
	return t
}

func (t *tracker) success(n NodeID) {
	for _, i := range t.members[n] {
		t.ok[i][n] = true
	}
}

func (t *tracker) failure(n NodeID) {
	for _, i := range t.members[n] {
		t.failed[i][n] = true
	}
}

// done reports whether every shard has a quorum.
func (t *tracker) done() bool {
	for i, s := range t.shards {
		if len(t.ok[i]) < t.quorum(s) {
			return false
		}
	}
	return true
}

// anyDone reports whether some shard has a quorum.
func (t *tracker) anyDone() bool {
	for i, s := range t.shards {
		if len(t.ok[i]) >= t.quorum(s) {
			return true
		}
	}
	return false
}

// impossible reports whether some shard can no longer reach a quorum.
func (t *tracker) impossible() bool {
	for i, s := range t.shards {
		if len(s.Nodes)-len(t.failed[i]) < t.quorum(s) {
			return true
		}
	}
	return false
}
