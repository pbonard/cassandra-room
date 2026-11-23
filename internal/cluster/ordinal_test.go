package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextOrdinal(t *testing.T) {
	nodes := map[string]*Node{}
	require.Equal(t, 1, NextOrdinal(nodes))
	nodes["a"] = &Node{Ordinal: 1, Status: StatusNormal}
	nodes["b"] = &Node{Ordinal: 3, Status: StatusLeft}
	require.Equal(t, 2, NextOrdinal(nodes))
	nodes["c"] = &Node{Ordinal: 2, Status: StatusNormal}
	require.Equal(t, 4, NextOrdinal(nodes), "a left node keeps its ordinal while others are free")
	for o := 4; o <= MaxOrdinal; o++ {
		nodes[string(rune(o))+"x"] = &Node{Ordinal: o, Status: StatusNormal}
	}
	require.Equal(t, 3, NextOrdinal(nodes), "a left node's ordinal is reused once every other is taken")
	nodes["b"].Status = StatusNormal
	require.Equal(t, 0, NextOrdinal(nodes))
}
