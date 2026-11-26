package cluster

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

// Node states in the cluster metadata (NodeState).
const (
	StatusRegistered = "REGISTERED"
	StatusJoining    = "JOINING"
	StatusNormal     = "NORMAL"
	StatusLeaving    = "LEAVING"
	StatusLeft       = "LEFT"
	// StatusMidJoin and StatusMidLeave are the middle steps of a range
	// movement (TCM's MidJoin and MidLeave): reads have moved to the new
	// owners, while writes still reach the old ones too, so that a
	// coordinator still on the previous step reads what one on this step
	// writes, and the other way around.
	StatusMidJoin  = "MID_JOIN"
	StatusMidLeave = "MID_LEAVE"
)

// Moving reports whether a node's status is a step of a range movement.
func Moving(status string) bool {
	switch status {
	case StatusJoining, StatusMidJoin, StatusLeaving, StatusMidLeave:
		return true
	}
	return false
}

// Placements of a range movement (TCM's data placements), each a ring:
//
//	          JOINING      MID_JOIN     LEAVING      MID_LEAVE
//	read      old          new          old          new
//	write     old + new    old + new    old + new    old + new
//
// then NORMAL (or LEFT): new only. Old owners are NORMAL, LEAVING and
// MID_LEAVE nodes, new owners NORMAL, JOINING and MID_JOIN ones (less the
// nodes they replace). Between two steps, every live node learns the
// first before the second is committed (a progress barrier), so reads at
// any step meet the writes at any neighbouring step.

// Node is a node as the cluster metadata describes it (Directory): its
// addresses, location, tokens and state.
type Node struct {
	Addr           string // internode address
	HostID         string
	NativeAddr     string
	NativePort     int
	DataCenter     string
	Rack           string
	Tokens         []int64
	Status         string
	ReleaseVersion string
	// Replaces is the host id of the dead node a joining node replaces.
	Replaces string `json:",omitempty"`
	// InternalAddr is the internode address the node listens on, when it
	// differs from Addr (the one it is known by): nodes of its data center
	// connect to it there (prefer_local).
	InternalAddr string `json:",omitempty"`
	// Ordinal is a small number unique among the cluster's nodes (from 1),
	// assigned when the node registers: it tags the node's transaction
	// timestamps (Accord's hybrid logical clock).
	Ordinal int `json:",omitempty"`
}

// MaxOrdinal bounds node ordinals (Accord's clock tag bits).
const MaxOrdinal = 1<<10 - 1

// NextOrdinal returns the smallest ordinal no node in the metadata holds
// (left nodes keep theirs, so their timestamps stay unambiguous, unless
// every ordinal is taken), or 0 if there is none.
func NextOrdinal(nodes map[string]*Node) int {
	used, live := map[int]bool{}, map[int]bool{}
	for _, n := range nodes {
		used[n.Ordinal] = true
		if n.Status != StatusLeft {
			live[n.Ordinal] = true
		}
	}
	for _, taken := range []map[int]bool{used, live} {
		for o := 1; o <= MaxOrdinal; o++ {
			if !taken[o] {
				return o
			}
		}
	}
	return 0
}

// Ring is the token ring (TokenMetadata): each token owns the range
// (previous token, token], and a key's replicas are found by walking the
// ring clockwise from its token.
type Ring struct {
	tokens []int64
	owner  map[int64]*Node
	nodes  map[string]*Node
}

// NewRing builds the read placement: the ring of NORMAL, LEAVING and
// MID_JOIN nodes (a node being replaced, until its replacement reaches
// MID_JOIN).
func NewRing(states []Node) *Ring {
	replaced := map[string]bool{}
	for _, s := range states {
		if s.Status == StatusMidJoin && s.Replaces != "" {
			replaced[s.Replaces] = true
		}
	}
	return ringOf(states, func(s *Node) bool {
		switch s.Status {
		case StatusNormal, StatusLeaving, StatusMidJoin:
			return !replaced[s.HostID]
		}
		return false
	})
}

// NewOldRing builds the placement before the movements in progress: the
// ring of NORMAL, LEAVING and MID_LEAVE nodes.
func NewOldRing(states []Node) *Ring {
	return ringOf(states, func(s *Node) bool {
		switch s.Status {
		case StatusNormal, StatusLeaving, StatusMidLeave:
			return true
		}
		return false
	})
}

// ringOf builds the ring of the nodes owns selects.
func ringOf(states []Node, owns func(*Node) bool) *Ring {
	r := &Ring{owner: map[int64]*Node{}, nodes: map[string]*Node{}}
	for i := range states {
		s := &states[i]
		if !owns(s) {
			continue
		}
		r.nodes[s.Addr] = s
		for _, t := range s.Tokens {
			if _, dup := r.owner[t]; !dup {
				r.tokens = append(r.tokens, t)
			}
			r.owner[t] = s
		}
	}
	sort.Slice(r.tokens, func(i, j int) bool { return r.tokens[i] < r.tokens[j] })
	return r
}

// NewFutureRing builds the ring as it will be once joining nodes have
// joined and leaving nodes have left (pending ranges).
func NewFutureRing(states []Node) *Ring {
	replaced := map[string]bool{}
	for _, s := range states {
		if (s.Status == StatusJoining || s.Status == StatusMidJoin) && s.Replaces != "" {
			replaced[s.Replaces] = true
		}
	}
	return ringOf(states, func(s *Node) bool {
		switch s.Status {
		case StatusNormal, StatusJoining, StatusMidJoin:
			return !replaced[s.HostID]
		}
		return false
	})
}

// Ranges returns the ring's ranges as (start, end] pairs, the first one
// wrapping around the end of the token space.
func (r *Ring) Ranges() [][2]int64 {
	out := make([][2]int64, len(r.tokens))
	for i, t := range r.tokens {
		prev := r.tokens[(i+len(r.tokens)-1)%len(r.tokens)]
		out[i] = [2]int64{prev, t}
	}
	return out
}

// Empty reports whether no node owns tokens yet.
func (r *Ring) Empty() bool { return len(r.tokens) == 0 }

// Nodes returns the ring's nodes.
func (r *Ring) Nodes() []*Node {
	out := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// firstIndex returns the index of the token owning t.
func (r *Ring) firstIndex(t int64) int {
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= t })
	if i == len(r.tokens) {
		return 0
	}
	return i
}

// RangeEnd returns the last token of the ring range containing t, and
// whether that range wraps around the end of the token space.
func (r *Ring) RangeEnd(t int64) (end int64, wraps bool) {
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= t })
	if i == len(r.tokens) {
		return math.MaxInt64, true
	}
	return r.tokens[i], false
}

// Replicas returns the replicas of token t under a keyspace's replication
// options (AbstractReplicationStrategy.calculateNaturalReplicas).
func (r *Ring) Replicas(replication map[string]string, t int64) []*Node {
	if len(r.tokens) == 0 {
		return nil
	}
	class := replication["class"]
	switch {
	case strings.HasSuffix(class, "NetworkTopologyStrategy"):
		return r.ntsReplicas(replication, t)
	case strings.HasSuffix(class, "LocalStrategy"):
		return nil
	}
	rf, _ := strconv.Atoi(replication["replication_factor"])
	var out []*Node
	seen := map[string]bool{}
	start := r.firstIndex(t)
	for i := 0; i < len(r.tokens) && len(out) < rf; i++ {
		n := r.owner[r.tokens[(start+i)%len(r.tokens)]]
		if !seen[n.Addr] {
			seen[n.Addr] = true
			out = append(out, n)
		}
	}
	return out
}

// ntsReplicas is NetworkTopologyStrategy: per data center, walk the ring
// taking nodes on racks not used yet, then the skipped nodes.
func (r *Ring) ntsReplicas(replication map[string]string, t int64) []*Node {
	want := map[string]int{}
	for dc, v := range replication {
		if dc == "class" {
			continue
		}
		n, _ := strconv.Atoi(v)
		want[dc] = n
	}
	// Racks of the nodes that own tokens: a rack whose nodes own none is
	// never reached, and waiting for it would keep the skipped nodes out.
	racks := map[string]map[string]bool{}
	for _, n := range r.owner {
		if racks[n.DataCenter] == nil {
			racks[n.DataCenter] = map[string]bool{}
		}
		racks[n.DataCenter][n.Rack] = true
	}
	var out []*Node
	have := map[string]int{}
	usedRacks := map[string]map[string]bool{}
	skipped := map[string][]*Node{}
	seen := map[string]bool{}
	start := r.firstIndex(t)
	for i := 0; i < len(r.tokens); i++ {
		n := r.owner[r.tokens[(start+i)%len(r.tokens)]]
		dc := n.DataCenter
		if seen[n.Addr] || have[dc] >= want[dc] {
			continue
		}
		seen[n.Addr] = true
		if usedRacks[dc] == nil {
			usedRacks[dc] = map[string]bool{}
		}
		if usedRacks[dc][n.Rack] && len(usedRacks[dc]) < len(racks[dc]) {
			skipped[dc] = append(skipped[dc], n)
			continue
		}
		usedRacks[dc][n.Rack] = true
		out = append(out, n)
		have[dc]++
		if len(usedRacks[dc]) == len(racks[dc]) {
			for len(skipped[dc]) > 0 && have[dc] < want[dc] {
				out = append(out, skipped[dc][0])
				skipped[dc] = skipped[dc][1:]
				have[dc]++
			}
		}
	}
	// Fewer racks than replicas left nodes skipped: take them in ring order.
	for dc, s := range skipped {
		for len(s) > 0 && have[dc] < want[dc] {
			out = append(out, s[0])
			s = s[1:]
			have[dc]++
		}
	}
	return out
}

// ReplicationFactor returns the total and per-DC replication factors.
func ReplicationFactor(replication map[string]string) (total int, perDC map[string]int) {
	perDC = map[string]int{}
	if strings.HasSuffix(replication["class"], "NetworkTopologyStrategy") {
		for dc, v := range replication {
			if dc != "class" {
				n, _ := strconv.Atoi(v)
				perDC[dc] = n
				total += n
			}
		}
		return total, perDC
	}
	total, _ = strconv.Atoi(replication["replication_factor"])
	return total, perDC
}

// RandomTokens picks n distinct random tokens (num_tokens without token
// allocation), uniformly over the ring: Murmur3Partitioner's tokens are
// above math.MinInt64, its minimum token, which no node takes.
func RandomTokens(n int) []int64 {
	out := make([]int64, 0, n)
	seen := make(map[int64]bool, n)
	for len(out) < n {
		t := int64(rand.Uint64())
		if t == math.MinInt64 || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
