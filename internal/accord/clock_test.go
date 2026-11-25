package accord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClockKeepsPace: values follow the wall clock in microseconds and
// only step by one when it has not moved; they never run ahead of it by
// more than the values issued within one microsecond.
func TestClockKeepsPace(t *testing.T) {
	now := time.UnixMicro(1_000_000)
	c := NewClock(func() time.Time { return now })
	require.EqualValues(t, 1_000_000, c.Next(0))
	for i := 1; i <= 1000; i++ {
		require.EqualValues(t, 1_000_000+i, c.Next(0), "a step of one microsecond")
	}
	now = now.Add(time.Second)
	require.EqualValues(t, 2_000_000, c.Next(0), "back to the wall clock")
	require.EqualValues(t, 3_000_001, c.Next(3_000_000), "above a value seen")
	c.Observe(5_000_000)
	require.EqualValues(t, 5_000_001, c.Next(0))
}
