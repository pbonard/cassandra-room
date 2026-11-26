package cluster

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// placementSteps checks that through a range movement's steps, a QUORUM
// read at any step meets a QUORUM write at the same or a neighbouring step
// (a progress barrier keeps coordinators at most one step apart): the
// write placement is old + new and needs a quorum plus every pending
// replica, the read placement switches from old to new at the MID step.
func placementSteps(t *testing.T, rf int, steps [][]Node) {
	t.Helper()
	repl := map[string]string{"class": "SimpleStrategy", "replication_factor": fmt.Sprint(rf)}
	q := rf/2 + 1
	type placement struct{ read, write []string }
	at := func(states []Node, tok int64) placement {
		ids := func(ns []*Node) []string {
			var out []string
			for _, n := range ns {
				if !slices.Contains(out, n.HostID) {
					out = append(out, n.HostID)
				}
			}
			return out
		}
		read := ids(NewRing(states).Replicas(repl, tok))
		write := slices.Clone(read)
		for _, id := range append(ids(NewOldRing(states).Replicas(repl, tok)), ids(NewFutureRing(states).Replicas(repl, tok))...) {
			if !slices.Contains(write, id) {
				write = append(write, id)
			}
		}
		return placement{read, write}
	}
	// Every range of every step's rings: a token at each range's end.
	var toks []int64
	for _, s := range steps {
		for _, r := range []*Ring{NewRing(s), NewOldRing(s), NewFutureRing(s)} {
			for _, rg := range r.Ranges() {
				toks = append(toks, rg[1])
			}
		}
	}
	for _, tok := range toks {
		for i := range steps {
			r := at(steps[i], tok)
			require.Len(t, r.read, rf, "step %d reads RF replicas", i)
			for j := max(0, i-1); j <= min(len(steps)-1, i+1); j++ {
				w := at(steps[j], tok)
				// The write is acknowledged by all but rf-q of its
				// placement; the worst case misses read replicas.
				missed := rf - q
				in := 0
				for _, id := range r.read {
					if slices.Contains(w.write, id) {
						in++
					}
				}
				// Read replicas the write surely reached: in-missed; a
				// quorum read avoids them only if rf-(in-missed) >= q.
				require.Less(t, rf-(in-missed), q, "token %d: a read at step %d (%v) can miss a write at step %d (%v)", tok, i, r.read, j, w.write)
			}
		}
	}
}

func testNodes(n int) []Node {
	var out []Node
	for i := 0; i < n; i++ {
		out = append(out, Node{Addr: fmt.Sprint("n", i), HostID: fmt.Sprint("h", i), Status: StatusNormal, Tokens: RandomTokens(8)})
	}
	return out
}

func withStatus(states []Node, i int, status string) []Node {
	out := slices.Clone(states)
	out[i].Status = status
	return out
}

func TestPlacementsJoin(t *testing.T) {
	for _, rf := range []int{1, 2, 3} {
		base := testNodes(4)
		joining := Node{Addr: "n9", HostID: "h9", Tokens: RandomTokens(8)}
		step := func(status string) []Node {
			j := joining
			j.Status = status
			return append(slices.Clone(base), j)
		}
		placementSteps(t, rf, [][]Node{base, step(StatusJoining), step(StatusMidJoin), step(StatusNormal)})
	}
}

func TestPlacementsLeave(t *testing.T) {
	for _, rf := range []int{1, 2, 3} {
		base := testNodes(5)
		placementSteps(t, rf, [][]Node{base, withStatus(base, 2, StatusLeaving), withStatus(base, 2, StatusMidLeave), withStatus(base, 2, StatusLeft)})
	}
}

func TestPlacementsReplace(t *testing.T) {
	for _, rf := range []int{1, 2, 3} {
		base := testNodes(4)
		step := func(status string) []Node {
			s := slices.Clone(base)
			r := Node{Addr: "n9", HostID: "h9", Tokens: slices.Clone(base[1].Tokens), Status: status, Replaces: "h1"}
			if status == StatusNormal {
				r.Replaces = ""
				s[1].Status = StatusLeft
			}
			return append(s, r)
		}
		placementSteps(t, rf, [][]Node{base, step(StatusJoining), step(StatusMidJoin), step(StatusNormal)})
	}
}

// TestPlacementsSkippingAStep: without the MID step (reads moving with the
// last step, as before), a read at the final placement can miss a write
// at the first one, which is what the steps and barriers prevent.
func TestPlacementsSkippingAStep(t *testing.T) {
	base := testNodes(3)
	joining := Node{Addr: "n9", HostID: "h9", Tokens: RandomTokens(8), Status: StatusNormal}
	final := append(slices.Clone(base), joining)
	ft := &testing.T{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		placementSteps(ft, 3, [][]Node{base, final})
	}()
	<-done
	require.True(t, ft.Failed(), "jumping from the old placement to the new one breaks quorum intersection")
}
