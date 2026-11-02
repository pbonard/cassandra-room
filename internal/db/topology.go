package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// Topology changes: bootstrap (BootStrapper, RangeStreamer), decommission
// (StorageService.decommission) and repair (RepairJob with Merkle trees).

// keyRangesOf turns a ring range (start, end] into key ranges [lo, hi),
// two when it wraps; nil bounds are open.
func keyRangesOf(start, end int64) [][2][]byte {
	lo := func(t int64) []byte {
		if t == math.MaxInt64 {
			return nil
		}
		return appendToken(nil, t+1)
	}
	hi := func(t int64) []byte {
		if t == math.MaxInt64 {
			return nil
		}
		return appendToken(nil, t+1)
	}
	if start < end {
		return [][2][]byte{{lo(start), hi(end)}}
	}
	// Wrapping (or the whole ring): (start, max] and [min, end].
	out := [][2][]byte{{nil, hi(end)}}
	if start != math.MaxInt64 {
		out = append(out, [2][]byte{lo(start), nil})
	}
	return out
}

// userTables returns the tables of non-system keyspaces.
func (d *DB) userTables() []*Table {
	var out []*Table
	for _, k := range d.schema.Load().Keyspaces {
		if k.virtual {
			continue
		}
		for _, t := range k.Tables {
			out = append(out, t)
		}
	}
	return out
}

func containsAddr(nodes []*cluster.Node, addr string) bool {
	return slices.ContainsFunc(nodes, func(n *cluster.Node) bool { return n.Addr == addr })
}

// bootstrap streams to this joining node the ranges it will own, from
// their current replicas, then rebuilds its local indexes.
func (p *proxy) bootstrap() error {
	cur := p.ring.Load()
	future := cluster.NewFutureRing(nodeList(p.d.schema.Load()))
	for _, t := range p.d.userTables() {
		k := p.d.schema.Load().Keyspaces[t.Keyspace]
		for _, r := range future.Ranges() {
			if !containsAddr(future.Replicas(k.Replication, r[1]), p.self) {
				continue
			}
			// Every current replica is a possible source, the ones known
			// to be up first: a joining node's membership view may not
			// know yet who is alive, and skipping a range would lose data.
			var srcs []*cluster.Node
			for _, n := range cur.Replicas(k.Replication, r[1]) {
				if n.Addr != p.self && !p.replacedNode(n) {
					srcs = append(srcs, n)
				}
			}
			slices.SortStableFunc(srcs, func(a, b *cluster.Node) int {
				ua, ub := p.up(a), p.up(b)
				switch {
				case ua && !ub:
					return -1
				case ub && !ua:
					return 1
				}
				return 0
			})
			var errs []error
			done := len(srcs) == 0
			for _, src := range srcs {
				err := func() error {
					for _, kr := range keyRangesOf(r[0], r[1]) {
						unit := progressUnit(t.Storage, kr[0], kr[1])
						if p.d.progress().done("bootstrap", unit) {
							continue // streamed before an interruption
						}
						if h := p.d.streamHook; h != nil {
							if err := h("bootstrap", unit); err != nil {
								return err
							}
						}
						if _, err := p.streamBetween(src.Addr, p.self, t.Storage, kr[0], kr[1]); err != nil {
							return err
						}
						if err := p.d.progress().mark("bootstrap", unit); err != nil {
							return err
						}
					}
					return nil
				}()
				if err == nil {
					done = true
					break
				}
				errs = append(errs, fmt.Errorf("%s: %w", src.Addr, err))
			}
			if !done {
				return fmt.Errorf("db: bootstrap streaming of range (%d, %d]: %w", r[0], r[1], errors.Join(errs...))
			}
		}
		for _, ix := range t.Indexes {
			if err := p.d.buildIndex(t, ix, time.Now().Unix()); err != nil {
				return err
			}
		}
	}
	return nil
}

// pendingReplicas are the nodes that will become replicas of a token once
// pending joins and departures complete: writes go to them too.
func (p *proxy) pendingReplicas(k *Keyspace, token int64, current []*cluster.Node) []*cluster.Node {
	states := nodeList(p.d.schema.Load())
	moving := false
	for _, s := range states {
		if s.Status == cluster.StatusJoining || s.Status == cluster.StatusLeaving {
			moving = true
			break
		}
	}
	if !moving {
		return nil
	}
	var out []*cluster.Node
	for _, n := range cluster.NewFutureRing(states).Replicas(k.Replication, token) {
		// By host id: a replacement may reuse the dead node's address.
		if !slices.ContainsFunc(current, func(c *cluster.Node) bool { return c.HostID == n.HostID }) {
			out = append(out, n)
		}
	}
	return out
}

// Decommission streams this node's data to the nodes that take over its
// ranges and leaves the ring.
func (d *DB) Decommission() error {
	p := d.proxy
	if p == nil {
		return errors.New("db: not a cluster node")
	}
	if slices.Contains(p.cmsMembers(), p.self) {
		return errors.New("db: a member of the cluster metadata service cannot be decommissioned")
	}
	// Like StorageService.decommission, refuse to leave fewer nodes than a
	// keyspace's replication factor.
	remaining := map[string]int{}
	for _, n := range p.ring.Load().Nodes() {
		if n.Addr != p.self {
			remaining[n.DataCenter]++
		}
	}
	for _, k := range d.schema.Load().Keyspaces {
		if k.virtual {
			continue
		}
		total, perDC := cluster.ReplicationFactor(k.Replication)
		left := 0
		for _, c := range remaining {
			left += c
		}
		if len(perDC) == 0 && left < total {
			return fmt.Errorf("db: not enough live nodes to maintain replication factor %d of keyspace %s; reduce it first", total, k.Name)
		}
		for dc, rf := range perDC {
			if remaining[dc] < rf {
				return fmt.Errorf("db: not enough live nodes in %s to maintain replication factor %d of keyspace %s; reduce it first", dc, rf, k.Name)
			}
		}
	}
	// PrepareLeave: once committed (and pushed to the live nodes), writes
	// also go to the nodes taking over this node's ranges.
	if err := p.setStatus("PREPARE_LEAVE", cluster.StatusLeaving); err != nil {
		return err
	}
	cur := p.ring.Load()
	future := cluster.NewFutureRing(nodeList(d.schema.Load()))
	for _, t := range d.userTables() {
		k := d.schema.Load().Keyspaces[t.Keyspace]
		for _, r := range cur.Ranges() {
			if !containsAddr(cur.Replicas(k.Replication, r[1]), p.self) {
				continue
			}
			for _, n := range future.Replicas(k.Replication, r[1]) {
				if containsAddr(cur.Replicas(k.Replication, r[1]), n.Addr) || !p.g.IsAlive(n.Addr) {
					continue
				}
				for _, kr := range keyRangesOf(r[0], r[1]) {
					unit := n.HostID + "/" + progressUnit(t.Storage, kr[0], kr[1])
					if d.progress().done("decommission", unit) {
						continue
					}
					if _, err := p.streamBetween(p.self, n.Addr, t.Storage, kr[0], kr[1]); err != nil {
						return fmt.Errorf("db: decommission streaming to %s: %w", n.Addr, err)
					}
					if err := d.progress().mark("decommission", unit); err != nil {
						return err
					}
				}
			}
		}
	}
	p.replayHints()
	// FinishLeave.
	if err := p.setStatus("FINISH_LEAVE", cluster.StatusLeft); err != nil {
		return err
	}
	return d.progress().clear("decommission")
}

// ---- repair ----

const repairDepth = 10

type treeRequest struct {
	Table       uint64
	First, Last uint64
}

func (p *proxy) handleRepair(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var r treeRequest
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	lt, err := p.d.engine.Table(lsm.TableID(r.Table))
	if err != nil {
		return nil, err
	}
	m, err := lt.MerkleTree(lsm.TokenRange{First: r.First, Last: r.Last}, repairDepth)
	if err != nil {
		return nil, err
	}
	return json.Marshal(m.Leaves)
}

// RepairStats summarise a repair.
type RepairStats struct {
	Ranges, Mismatches int
	Streamed           int64 // bytes
}

// Repair synchronizes this node's ranges of a keyspace's tables (all user
// keyspaces if empty) with the other replicas: each side builds a Merkle
// tree of every range, and the leaves that differ are streamed both ways
// (nodetool repair).
func (d *DB) Repair(keyspace string) (RepairStats, error) {
	var st RepairStats
	p := d.proxy
	if p == nil {
		return st, errors.New("db: not a cluster node")
	}
	ring := p.ring.Load()
	for _, t := range d.userTables() {
		if keyspace != "" && t.Keyspace != keyspace {
			continue
		}
		k := d.schema.Load().Keyspaces[t.Keyspace]
		lt, err := d.engine.Table(t.Storage)
		if err != nil {
			continue
		}
		for _, r := range ring.Ranges() {
			reps := ring.Replicas(k.Replication, r[1])
			if !containsAddr(reps, p.self) {
				continue
			}
			for _, tr := range tokenRangesOf(r[0], r[1]) {
				st.Ranges++
				mine, err := lt.MerkleTree(tr, repairDepth)
				if err != nil {
					return st, err
				}
				for _, peer := range reps {
					if peer.Addr == p.self || !p.g.IsAlive(peer.Addr) {
						continue
					}
					req, _ := json.Marshal(treeRequest{Table: uint64(t.Storage), First: tr.First, Last: tr.Last})
					ctx, cancel := context.WithTimeout(context.Background(), p.opts.ReadTimeout*4)
					b, err := p.m.Request(ctx, peer.Addr, cluster.VerbRepair, req)
					cancel()
					if err != nil {
						return st, fmt.Errorf("db: repair tree from %s: %w", peer.Addr, err)
					}
					theirs := &lsm.MerkleTree{Range: tr, Depth: repairDepth}
					if err := json.Unmarshal(b, &theirs.Leaves); err != nil {
						return st, err
					}
					diff, err := mine.Diff(theirs)
					if err != nil {
						return st, err
					}
					for _, dr := range diff {
						st.Mismatches++
						lo, hi := dr.Bounds()
						n, err := p.streamBetween(peer.Addr, p.self, t.Storage, lo, hi)
						if err != nil {
							return st, err
						}
						m, err := p.streamBetween(p.self, peer.Addr, t.Storage, lo, hi)
						if err != nil {
							return st, err
						}
						st.Streamed += n + m
					}
				}
			}
		}
	}
	return st, nil
}

// tokenRangesOf converts a ring range (start, end] to the engine's token
// ranges (unsigned, both ends inclusive).
func tokenRangesOf(start, end int64) []lsm.TokenRange {
	u := func(t int64) uint64 { return uint64(t) ^ (1 << 63) }
	if start < end {
		return []lsm.TokenRange{{First: u(start + 1), Last: u(end)}}
	}
	out := []lsm.TokenRange{{First: u(math.MinInt64), Last: u(end)}}
	if start != math.MaxInt64 {
		out = append(out, lsm.TokenRange{First: u(start + 1), Last: u(math.MaxInt64)})
	}
	return out
}

// Status describes the cluster as nodetool status does.
func (d *DB) Status() string {
	if d.proxy == nil {
		return "single node\n"
	}
	p := d.proxy
	var b strings.Builder
	fmt.Fprintf(&b, "Datacenter: %s\n", d.opts.DataCenter)
	fmt.Fprintf(&b, "%-4s %-22s %-7s %-37s %s\n", "--", "Address", "Tokens", "Host ID", "Rack")
	nodes := nodeList(d.schema.Load())
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Addr < nodes[j].Addr })
	for _, s := range nodes {
		state := "U"
		if s.Addr != p.self && !p.g.IsAlive(s.Addr) {
			state = "D"
		}
		state += map[string]string{cluster.StatusNormal: "N", cluster.StatusJoining: "J", cluster.StatusLeaving: "L", cluster.StatusLeft: "-"}[s.Status]
		fmt.Fprintf(&b, "%-4s %-22s %-7d %-37s %s\n", state, s.Addr, len(s.Tokens), s.HostID, s.Rack)
	}
	return b.String()
}

// Cleanup removes the data of ranges this node no longer replicates
// (nodetool cleanup), for one keyspace or all: after nodes join, the old
// replicas of their ranges keep the data until cleaned. Like
// CompactionManager.performCleanup it refuses while ranges are moving.
// It returns the number of tables cleaned.
func (d *DB) Cleanup(keyspace string) (int, error) {
	p := d.proxy
	if p == nil {
		return 0, nil // a single node owns everything
	}
	s := d.schema.Load()
	for _, n := range s.Nodes {
		if n.Status == cluster.StatusJoining || n.Status == cluster.StatusLeaving {
			return 0, fmt.Errorf("db: node %s is %s: cleanup must wait until ranges stop moving", n.Addr, n.Status)
		}
	}
	ring := p.ring.Load()
	cleaned := 0
	for _, t := range d.userTables() {
		if keyspace != "" && t.Keyspace != keyspace {
			continue
		}
		k := s.Keyspaces[t.Keyspace]
		owned := map[int64]bool{} // by range end
		keep := func(tok int64) bool {
			end, _ := ring.RangeEnd(tok)
			o, ok := owned[end]
			if !ok {
				o = containsAddr(ring.Replicas(k.Replication, tok), p.self)
				owned[end] = o
			}
			return o
		}
		lt, err := d.engine.Table(t.Storage)
		if err != nil {
			return cleaned, err
		}
		if err := lt.Cleanup(func(key []byte) bool { return keep(tokenOf(key)) }); err != nil {
			return cleaned, err
		}
		for _, ix := range t.Indexes {
			it, err := d.engine.Table(ix.Storage)
			if err != nil {
				return cleaned, err
			}
			if err := it.Cleanup(func(key []byte) bool {
				rest, err := ix.entryRowKey(key)
				return err != nil || len(rest) < 8 || keep(tokenOf(rest))
			}); err != nil {
				return cleaned, err
			}
		}
		cleaned++
	}
	return cleaned, nil
}

// RemoveNode removes a dead node from the cluster (nodetool removenode):
// it is marked LEAVING, the nodes that become replicas of its ranges get
// their data from the surviving replicas, then it is LEFT.
func (d *DB) RemoveNode(target string) error {
	p := d.proxy
	if p == nil {
		return errors.New("db: not a cluster node")
	}
	var dead *cluster.Node
	for _, n := range d.schema.Load().Nodes {
		if (n.Addr == target || n.HostID == target) && n.Status != cluster.StatusLeft {
			dead = n
		}
	}
	switch {
	case dead == nil:
		return fmt.Errorf("db: no node %s in the cluster", target)
	case dead.HostID == d.local.HostID:
		return errors.New("db: a node cannot remove itself; use decommission")
	case p.g.IsAlive(dead.Addr):
		return fmt.Errorf("db: node %s is alive; decommission it instead", target)
	case slices.Contains(p.cmsMembers(), dead.Addr):
		return fmt.Errorf("db: node %s is a member of the cluster metadata service; reconfigure it first", target)
	}
	op := "removenode-" + dead.HostID
	if err := p.setNodeStatus("PREPARE_REMOVE", dead.HostID, cluster.StatusLeaving); err != nil {
		return err
	}
	cur := p.ring.Load()
	future := cluster.NewFutureRing(nodeList(d.schema.Load()))
	for _, t := range d.userTables() {
		k := d.schema.Load().Keyspaces[t.Keyspace]
		for _, r := range cur.Ranges() {
			reps := cur.Replicas(k.Replication, r[1])
			if !containsAddr(reps, dead.Addr) {
				continue
			}
			var src *cluster.Node
			for _, n := range reps {
				if n.HostID != dead.HostID && p.up(n) {
					src = n
					break
				}
			}
			for _, n := range future.Replicas(k.Replication, r[1]) {
				if slices.ContainsFunc(reps, func(c *cluster.Node) bool { return c.HostID == n.HostID }) {
					continue
				}
				if src == nil {
					return fmt.Errorf("db: no live replica left for range (%d, %d] of %s", r[0], r[1], k.Name)
				}
				for _, kr := range keyRangesOf(r[0], r[1]) {
					unit := n.HostID + "/" + progressUnit(t.Storage, kr[0], kr[1])
					if d.progress().done(op, unit) {
						continue
					}
					if _, err := p.streamBetween(src.Addr, n.Addr, t.Storage, kr[0], kr[1]); err != nil {
						return fmt.Errorf("db: removenode streaming from %s to %s: %w", src.Addr, n.Addr, err)
					}
					if err := d.progress().mark(op, unit); err != nil {
						return err
					}
				}
			}
		}
	}
	if err := p.setNodeStatus("FINISH_REMOVE", dead.HostID, cluster.StatusLeft); err != nil {
		return err
	}
	return d.progress().clear(op)
}

// ReconfigureCMS sets the members of the cluster metadata service (TCM's
// nodetool cms reconfigure): they must be normal, live nodes. The change
// is committed by the current members; the next epochs are decided by the
// new ones.
func (d *DB) ReconfigureCMS(members []string) error {
	p := d.proxy
	if p == nil {
		return errors.New("db: not a cluster node")
	}
	if len(members) == 0 {
		return errors.New("db: the metadata service needs at least one member")
	}
	s := d.schema.Load()
	for _, m := range members {
		ok := false
		for _, n := range s.Nodes {
			if n.Addr == m && n.Status == cluster.StatusNormal && p.up(n) {
				ok = true
			}
		}
		if !ok {
			return fmt.Errorf("db: %s is not a live normal node", m)
		}
	}
	members = slices.Clone(members)
	slices.Sort(members)
	members = slices.Compact(members)
	_, err := d.change("RECONFIGURE_CMS", func(next *Schema) (message.Message, bool, error) {
		if slices.Equal(next.CMS, members) {
			return nil, false, nil
		}
		next.CMS = members
		next.logDetail = strings.Join(members, ",")
		return nil, true, nil
	})
	return err
}
