package cluster

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRandomTokensUniform: tokens spread evenly over the ring (the
// difference of two random numbers made a triangular distribution, with
// few tokens near the ring's ends), never math.MinInt64, never repeated.
func TestRandomTokensUniform(t *testing.T) {
	const n = 40000
	toks := RandomTokens(n)
	require.Len(t, toks, n)
	var quarters [4]int
	for i, tok := range toks {
		require.NotEqual(t, int64(math.MinInt64), tok)
		if i > 0 {
			require.Less(t, toks[i-1], tok, "sorted and distinct")
		}
		quarters[int((uint64(tok)+1<<63)>>62)]++
	}
	for q, c := range quarters {
		require.InDelta(t, n/4, c, n/40, "quarter %d of the ring holds %d tokens", q, c)
	}
}
