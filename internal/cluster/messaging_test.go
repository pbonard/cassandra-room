package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func startPair(t *testing.T, o MessagingOptions) (*Messaging, *Messaging) {
	a, err := NewMessagingWith("127.0.0.1:0", o)
	require.NoError(t, err)
	b, err := NewMessagingWith("127.0.0.1:0", o)
	require.NoError(t, err)
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

// TestConnectionClasses checks that verbs and large messages use their own
// connections, so a slow large message does not delay small ones.
func TestConnectionClasses(t *testing.T) {
	a, b := startPair(t, MessagingOptions{})
	release := make(chan struct{})
	b.Handle(VerbStreamPush, func(context.Context, string, []byte) ([]byte, error) { <-release; return nil, nil })
	b.Handle(VerbMutation, func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil })
	b.Handle(VerbPing, func(context.Context, string, []byte) ([]byte, error) { return []byte("pong"), nil })
	a.Start()
	b.Start()
	go func() { _, _ = a.Request(context.Background(), b.Addr(), VerbStreamPush, make([]byte, 1<<20)) }()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := a.Request(ctx, b.Addr(), VerbMutation, []byte("w"))
	require.NoError(t, err, "a small message is not stuck behind the large one")
	require.Equal(t, "w", string(out))
	_, err = a.Request(ctx, b.Addr(), VerbPing, nil)
	require.NoError(t, err)
	close(release)
	a.mu.Lock()
	classes := map[int]bool{}
	for k := range a.conns {
		classes[k.class] = true
	}
	a.mu.Unlock()
	require.Equal(t, map[int]bool{classUrgent: true, classSmall: true, classLarge: true}, classes)
	require.Equal(t, classLarge, classOf(VerbMutation, largeMessage+1))
	require.Equal(t, ProtocolVersion, a.PeerVersion(b.Addr()))
}

// TestFrameChecksum checks that a corrupted frame is detected.
func TestFrameChecksum(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeFrame(&buf, kindRequest, VerbMutation, 7, 0, []byte("payload")))
	good := buf.Bytes()
	f, err := readFrame(bytes.NewReader(good))
	require.NoError(t, err)
	require.Equal(t, "payload", string(f.payload))
	bad := bytes.Clone(good)
	bad[len(bad)-1] ^= 1
	_, err = readFrame(bytes.NewReader(bad))
	require.ErrorIs(t, err, errCorrupt)
}

// TestHandshakeVersions checks that a peer with no common version is
// refused.
func TestHandshakeVersions(t *testing.T) {
	_, err := negotiate(ProtocolVersion+5, ProtocolVersion+1)
	require.Error(t, err, "a newer peer that cannot speak our version")
	v, err := negotiate(ProtocolVersion+5, MinProtocolVersion)
	require.NoError(t, err)
	require.EqualValues(t, ProtocolVersion, v)
	// A raw client speaking an unsupported version is refused.
	b, err := NewMessaging("127.0.0.1:0")
	require.NoError(t, err)
	b.Start()
	defer b.Close()
	c, err := net.Dial("tcp", b.Addr())
	require.NoError(t, err)
	defer c.Close()
	hello := append(handshakeMagic[:], 0, 0, 0, 0, classSmall)
	binary.BigEndian.PutUint16(hello[4:], 1)
	binary.BigEndian.PutUint16(hello[6:], 1)
	hello = binary.BigEndian.AppendUint16(hello, 0)
	_, err = c.Write(hello)
	require.NoError(t, err)
	var reply [7]byte
	_, err = c.Read(reply[:])
	require.NoError(t, err)
	require.Zero(t, reply[6], "refused")
}

// TestExpiredRequestsDropped checks that a request whose deadline passes
// while waiting for a worker is not executed.
func TestExpiredRequestsDropped(t *testing.T) {
	a, b := startPair(t, MessagingOptions{})
	var ran atomic.Int64
	block := make(chan struct{})
	b.Handle(VerbStreamFile, func(_ context.Context, _ string, p []byte) ([]byte, error) {
		if string(p) == "block" {
			<-block
		}
		ran.Add(1)
		return nil, nil
	})
	a.Start()
	b.Start()
	// Occupy every streaming worker.
	for range stageWorkers[stageStreaming] {
		go func() {
			_, err := a.Request(context.Background(), b.Addr(), VerbStreamFile, []byte("block"))
			if err != nil {
				t.Logf("blocker: %v", err)
			}
		}()
	}
	require.Eventually(t, func() bool { return len(b.stages[stageStreaming]) == stageWorkers[stageStreaming] }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := a.Request(ctx, b.Addr(), VerbStreamFile, []byte("late"))
	require.ErrorIs(t, err, ErrTimeout)
	close(block)
	for start := time.Now(); ran.Load() < int64(stageWorkers[stageStreaming]) && time.Since(start) < time.Second; {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, stageWorkers[stageStreaming], ran.Load(), "the expired request never ran; dropped %v", b.Stats().Dropped)
	require.EqualValues(t, 1, b.Stats().Dropped[VerbStreamFile])
}

// TestOverloadAndBackpressure checks the outbound limits and that a
// receiver over its limits stops reading rather than buffering.
func TestOverloadAndBackpressure(t *testing.T) {
	o := MessagingOptions{QueueCapacity: 1 << 20, EndpointReserve: 1 << 20, GlobalReserve: 1 << 20}
	a, b := startPair(t, o)
	block := make(chan struct{})
	var mu sync.Mutex
	concurrent, peak := 0, 0
	b.Handle(VerbMutation, func(context.Context, string, []byte) ([]byte, error) {
		mu.Lock()
		concurrent++
		peak = max(peak, concurrent)
		mu.Unlock()
		<-block
		mu.Lock()
		concurrent--
		mu.Unlock()
		return nil, nil
	})
	a.Start()
	b.Start()
	payload := make([]byte, 60<<10) // small class, 60 KiB
	var wg sync.WaitGroup
	var overloaded atomic.Int64
	for range 60 { // 3.6 MiB against 2 MiB of outbound room
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := a.Request(ctx, b.Addr(), VerbMutation, payload); errors.Is(err, ErrOverloaded) {
				overloaded.Add(1)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	p := peak
	mu.Unlock()
	require.LessOrEqual(t, p, 34, "the receiver holds at most its limits' worth of requests")
	close(block)
	wg.Wait()
	require.Positive(t, overloaded.Load())
	require.EqualValues(t, overloaded.Load(), a.Stats().Overloaded)
}
