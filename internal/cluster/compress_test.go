package cluster

import (
	"math/rand/v2"
	"testing"
)

// TestCompressRoundTrip compresses and decompresses frames of mixed
// content up to stream chunk size (lz4 v4.0.3 corrupted some 4 MiB
// blocks, failing streaming).
func TestCompressRoundTrip(t *testing.T) {
	m := &Messaging{opts: MessagingOptions{Compress: true}}
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 60; i++ {
		size := r.IntN(5<<20) + 512
		if i%3 == 0 {
			size = 4 << 20
		}
		p := make([]byte, size)
		// Mixed: runs of repeated bytes and random bytes.
		for j := 0; j < size; {
			n := min(size-j, 1+r.IntN(4096))
			if r.IntN(2) == 0 {
				b := byte(r.IntN(256))
				for k := 0; k < n; k++ {
					p[j+k] = b
				}
			} else {
				for k := 0; k < n; k++ {
					p[j+k] = byte(r.IntN(256))
				}
			}
			j += n
		}
		flag, packed := m.pack(p)
		_, out, err := unpack(flag, packed)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if string(out) != string(p) {
			t.Fatalf("size %d: mismatch", size)
		}
	}
}
