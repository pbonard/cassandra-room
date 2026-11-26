package lsm

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOwnership: densities and shards are measured over the owned token
// space. A node owning an eighth of the ring in one range sees a flush of
// all its data span 1 (not 1/8, which made it 8 times denser), and shards
// cut its range, not the whole ring.
func TestOwnership(t *testing.T) {
	eighth := uint64(1) << 61
	o := newOwnership([]TokenRange{{First: 3 * eighth, Last: 4*eighth - 1}})
	all := &fileMeta{size: 1 << 20}
	all.smallest.UserKey, all.largest.UserKey = tokenKey(3*eighth), tokenKey(4*eighth-1)
	require.InDelta(t, 1.0, o.span(all.smallest.UserKey, all.largest.UserKey), 1e-9)
	require.InDelta(t, float64(1<<20), o.density(all), 1)
	require.InDelta(t, 8*float64(1<<20), (*ownership)(nil).density(all), 1e-3*float64(1<<20), "the whole-ring measure")

	bs := o.boundaries(4)
	require.Len(t, bs, 3)
	for i, b := range bs {
		want := 3*eighth + uint64(i+1)*eighth/4
		require.InDelta(t, float64(want), float64(keyToken(b)), float64(eighth)/1e6, "boundary %d", i)
	}

	// Two ranges: an even cut puts half the owned tokens on each side.
	two := newOwnership([]TokenRange{{First: 0, Last: eighth - 1}, {First: 4 * eighth, Last: 5*eighth - 1}})
	b := two.boundaries(2)
	require.Len(t, b, 1)
	require.InDelta(t, float64(4*eighth), float64(keyToken(b[0])), float64(eighth)/1e6)
	require.Nil(t, newOwnership(nil))
	require.Equal(t, shardBoundaries(4), (*ownership)(nil).boundaries(4))
	require.InDelta(t, 1.0, newOwnership([]TokenRange{{0, math.MaxUint64}}).span(tokenKey(0), tokenKey(math.MaxUint64)), 1e-9)
}
