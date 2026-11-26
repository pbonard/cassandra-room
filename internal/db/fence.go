package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pbonard/cassandra-room/internal/cluster"
)

// Epoch fencing (TCM's coordinator/replica epoch checks): a coordinator
// tags each request to a replica with the epoch of the placements it
// planned it with; a replica whose placements changed after that epoch
// refuses it, and the coordinator catches up with the metadata log and
// plans the request again. A progress barrier's acknowledgement waits for
// the requests the replica admitted before its placements changed. So
// once a quorum of a range's replicas acknowledged a step, no request
// planned before the step can reach a quorum of them: requests from a
// coordinator that was paused or cut off are refused, however late they
// arrive, instead of waiting out request timeouts.

// placement is the placements of one epoch, read together by coordinators.
type placement struct {
	epoch  int64
	ring   *cluster.Ring
	moving *movingRings // nil when no range moves
}

// placementOf builds the placements of a metadata version.
func placementOf(s *Schema) *placement {
	nodes := nodeList(s)
	pl := &placement{epoch: s.Epoch, ring: cluster.NewRing(nodes)}
	if slices.ContainsFunc(nodes, func(n cluster.Node) bool { return cluster.Moving(n.Status) }) {
		pl.moving = &movingRings{old: cluster.NewOldRing(nodes), future: cluster.NewFutureRing(nodes)}
	}
	return pl
}

// coordinatorBehind is in the error a replica answers a request planned
// before its placements changed with (it crosses the wire as text).
const coordinatorBehind = "coordinator behind the replica's placements"

// isBehind reports whether a replica refused a request as planned with
// outdated placements.
func isBehind(err error) bool {
	return err != nil && strings.Contains(err.Error(), coordinatorBehind)
}

// fencedRetries bounds how often a coordinator catches up and plans a
// request again.
const fencedRetries = 3

// fence admits a replica's requests by the epoch of their placements.
type fence struct {
	mu sync.Mutex
	// epoch is the epoch this node's placements last changed at: requests
	// planned before it are refused.
	epoch int64
	// inflight counts the admitted requests in progress, by epoch.
	inflight map[int64]int
}

// enter admits a request planned at epoch e (0: not planned with
// placements, as hints and streaming are, never refused) until done.
func (f *fence) enter(e int64) (done func(), err error) {
	if e == 0 {
		return func() {}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if e < f.epoch {
		return nil, fmt.Errorf("db: %s (request epoch %d, placements of epoch %d)", coordinatorBehind, e, f.epoch)
	}
	if f.inflight == nil {
		f.inflight = map[int64]int{}
	}
	f.inflight[e]++
	return func() {
		f.mu.Lock()
		if f.inflight[e]--; f.inflight[e] == 0 {
			delete(f.inflight, e)
		}
		f.mu.Unlock()
	}, nil
}

// advance refuses from now on the requests planned before epoch e.
func (f *fence) advance(e int64) {
	f.mu.Lock()
	f.epoch = max(f.epoch, e)
	f.mu.Unlock()
}

// drain waits until no request planned before the fence's epoch (as it is
// now) is in progress.
func (f *fence) drain(ctx context.Context) error {
	f.mu.Lock()
	below := f.epoch
	f.mu.Unlock()
	for {
		f.mu.Lock()
		busy := false
		for e := range f.inflight {
			if e < below {
				busy = true
				break
			}
		}
		f.mu.Unlock()
		if !busy {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("db: requests planned before epoch %d still in progress: %w", below, ctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// fenced prefixes a request's payload with the epoch it was planned at.
func fenced(epoch int64, payload []byte) []byte {
	b := binary.AppendUvarint(make([]byte, 0, len(payload)+binary.MaxVarintLen64), uint64(epoch))
	return append(b, payload...)
}

// admit reads a fenced request's epoch and admits it; the rest is the
// request.
func (p *proxy) admit(payload []byte) (rest []byte, done func(), err error) {
	e, k := binary.Uvarint(payload)
	if k <= 0 {
		return nil, nil, errors.New("db: request without an epoch")
	}
	done, err = p.fence.enter(int64(e))
	return payload[k:], done, err
}

// synced answers a progress barrier once this node caught up: its epoch,
// once no request admitted before its placements changed is in progress.
func (p *proxy) synced(ctx context.Context) (int64, error) {
	// An install in progress (it holds ddlMu) has set the schema but
	// perhaps not yet the fence.
	p.d.ddlMu.Lock()
	epoch := p.d.schema.Load().Epoch
	p.d.ddlMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, max(p.opts.ReadTimeout, p.opts.WriteTimeout)*3)
	defer cancel()
	if err := p.fence.drain(ctx); err != nil {
		return 0, err
	}
	return epoch, nil
}

// placementKey identifies what decides placements: the nodes' addresses,
// tokens and states, and the keyspaces' replication.
func placementKey(s *Schema) string {
	var parts []string
	for _, n := range s.Nodes {
		parts = append(parts, fmt.Sprintf("n %s %s %s %s %s %s %v", n.HostID, n.Addr, n.DataCenter, n.Status, n.Replaces, n.Rack, n.Tokens))
	}
	for _, k := range s.Keyspaces {
		var r []string
		for o, v := range k.Replication {
			r = append(r, o+"="+v)
		}
		slices.Sort(r)
		parts = append(parts, "k "+k.Name+" "+strings.Join(r, ","))
	}
	slices.Sort(parts)
	return strings.Join(parts, "\n")
}

// catchUpFrom brings this node's metadata up to a replica's after it
// refused a request.
func (p *proxy) catchUpFrom(addr string) {
	if addr == p.self {
		return
	}
	_ = p.cms.CatchUp(addr)
}
