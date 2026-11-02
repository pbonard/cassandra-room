package cluster

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

// Ring is the token ring (TokenMetadata): each token owns the range
// (previous token, token], and a key's replicas are found by walking the
// ring clockwise from its token.
type Ring struct {
	tokens []int64
	owner  map[int64]*EndpointState
	nodes  map[string]*EndpointState
}

// NewRing builds a ring from the nodes that own tokens: normal nodes and,
// when withLeaving, leaving ones.
func NewRing(states []EndpointState) *Ring {
	r := &Ring{owner: map[int64]*EndpointState{}, nodes: map[string]*EndpointState{}}
	for i := range states {
		s := &states[i]
		if s.Status != StatusNormal && s.Status != StatusLeaving {
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
func NewFutureRing(states []EndpointState) *Ring {
	var next []EndpointState
	for _, s := range states {
		switch s.Status {
		case StatusJoining:
			s.Status = StatusNormal
			next = append(next, s)
		case StatusNormal:
			next = append(next, s)
		}
	}
	return NewRing(next)
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
func (r *Ring) Nodes() []*EndpointState {
	out := make([]*EndpointState, 0, len(r.nodes))
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
func (r *Ring) Replicas(replication map[string]string, t int64) []*EndpointState {
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
	var out []*EndpointState
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
func (r *Ring) ntsReplicas(replication map[string]string, t int64) []*EndpointState {
	want := map[string]int{}
	for dc, v := range replication {
		if dc == "class" {
			continue
		}
		n, _ := strconv.Atoi(v)
		want[dc] = n
	}
	racks := map[string]map[string]bool{}
	for _, n := range r.nodes {
		if racks[n.DataCenter] == nil {
			racks[n.DataCenter] = map[string]bool{}
		}
		racks[n.DataCenter][n.Rack] = true
	}
	var out []*EndpointState
	have := map[string]int{}
	usedRacks := map[string]map[string]bool{}
	skipped := map[string][]*EndpointState{}
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

// RandomTokens picks n random tokens (num_tokens without token
// allocation).
func RandomTokens(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = rand.Int63() - rand.Int63()
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
