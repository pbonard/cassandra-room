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

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/pbonard/cassandra-room/internal/storage/lsm"
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
	// Accord's bootstrap: a sync point over the ranges it will own in every
	// table, applied at every
	// current replica, before streaming. The data streamed then holds every
	// transaction before it (later ones reach this node as a pending
	// replica), and this node does not wait for the earlier ones it never
	// saw.
	// It is best effort: when a range has no quorum (a replica being
	// replaced at RF 2), this node instead fetches the earlier transactions
	// it does not know before using them.
	if p.accord != nil && !cur.Empty() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = p.accord.Bootstrap(ctx, p.accordOwnedRanges(cluster.NewFutureRing(nodeList(p.d.schema.Load())), false))
		cancel()
	}
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

// leaveTargetsDown lists the nodes that would take over this node's ranges
// in future and are down.
func (p *proxy) leaveTargetsDown(future *cluster.Ring) []string {
	cur := p.ring.Load()
	down := map[string]bool{}
	for _, t := range p.d.userTables() {
		k := p.d.schema.Load().Keyspaces[t.Keyspace]
		if k == nil {
			continue
		}
		for _, r := range cur.Ranges() {
			if !containsAddr(cur.Replicas(k.Replication, r[1]), p.self) {
				continue
			}
			for _, n := range future.Replicas(k.Replication, r[1]) {
				if n.Addr != p.self && !containsAddr(cur.Replicas(k.Replication, r[1]), n.Addr) && !p.g.IsAlive(n.Addr) {
					down[n.Addr] = true
				}
			}
		}
	}
	out := make([]string, 0, len(down))
	for a := range down {
		out = append(out, a)
	}
	slices.Sort(out)
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
	// Every node taking over a range must be up to receive it: leaving
	// without streaming to one would leave it without the range's data
	// (and, with one replica, lose it). StorageService.unbootstrap fails
	// the same way.
	states := nodeList(d.schema.Load())
	for i := range states {
		if states[i].Addr == p.self {
			states[i].Status = cluster.StatusLeaving
		}
	}
	if down := p.leaveTargetsDown(cluster.NewFutureRing(states)); len(down) > 0 {
		return fmt.Errorf("db: nodes taking over this node's ranges are down: %s; bring them up (or remove them) first", strings.Join(down, ", "))
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
				if containsAddr(cur.Replicas(k.Replication, r[1]), n.Addr) {
					continue
				}
				if !p.g.IsAlive(n.Addr) {
					// Still leaving: a retry resumes where this stopped.
					return fmt.Errorf("db: decommission: %s, which takes over a range, is down; retry once it is up", n.Addr)
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
	Ranges     int   `json:"ranges"`
	Mismatches int   `json:"mismatches"`
	Streamed   int64 `json:"streamed"` // bytes
}

// Repair synchronizes this node's ranges of a keyspace's tables (all user
// keyspaces if empty) with the other replicas: each side builds a Merkle
// tree of every range, and the leaves that differ are streamed both ways
// (nodetool repair).
func (d *DB) Repair(keyspace string) (RepairStats, error) {
	return d.repair(keyspace, "", nil)
}

// RepairRange repairs the part of the token range (start, end] this node
// replicates, for one table of a keyspace (or all its tables if table is
// empty): a segment of a scheduled repair (Reaper's subrange repair). start
// may be greater than end (a range wrapping around the ring).
func (d *DB) RepairRange(keyspace, table string, start, end int64) (RepairStats, error) {
	if keyspace == "" {
		return RepairStats{}, errors.New("db: a range repair needs a keyspace")
	}
	segment := tokenRangesOf(start, end)
	return d.repair(keyspace, table, segment)
}

// repair synchronizes this node's ranges of the tables, restricted to the
// token ranges of segment (all of them if nil).
func (d *DB) repair(keyspace, table string, segment []lsm.TokenRange) (RepairStats, error) {
	var st RepairStats
	p := d.proxy
	if p == nil {
		return st, errors.New("db: not a cluster node")
	}
	ring := p.ring.Load()
	found := false
	for _, t := range d.userTables() {
		if keyspace != "" && t.Keyspace != keyspace || table != "" && t.Name != table {
			continue
		}
		found = true
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
			for _, tr := range intersectTokenRanges(tokenRangesOf(r[0], r[1]), segment) {
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
	if !found && (keyspace != "" || table != "") {
		return st, fmt.Errorf("db: no table %s.%s to repair", keyspace, table)
	}
	return st, nil
}

// intersectTokenRanges returns the parts of ranges within segment (all of
// ranges if segment is nil).
func intersectTokenRanges(ranges, segment []lsm.TokenRange) []lsm.TokenRange {
	if segment == nil {
		return ranges
	}
	var out []lsm.TokenRange
	for _, r := range ranges {
		for _, s := range segment {
			if first, last := max(r.First, s.First), min(r.Last, s.Last); first <= last {
				out = append(out, lsm.TokenRange{First: first, Last: last})
			}
		}
	}
	return out
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
		// An sstable needs rewriting only if its token span touches a range
		// this node does not replicate.
		segs := p.tokenSegments()
		dirty := func(smallest, largest []byte) bool {
			if len(smallest) < 8 || len(largest) < 8 {
				return true
			}
			lo, hi := tokenOf(smallest), tokenOf(largest)
			for _, sg := range segs {
				if sg[1] >= lo && (sg[0] < hi || sg[0] == math.MinInt64) && !keep(sg[1]) {
					return true
				}
			}
			return false
		}
		if err := lt.Cleanup(func(key []byte) bool { return keep(tokenOf(key)) }, dirty); err != nil {
			return cleaned, err
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
// nodetool cms reconfigure): they must be normal, live nodes. As TCM's
// PrepareCMSReconfiguration and AdvanceCMSReconfiguration, it moves one
// member at a time, additions first, each its own epoch: a node is brought
// up to date with the log before it joins, and every majority of the
// members before a step shares a node with every majority after it. An
// interrupted reconfiguration is continued by running it again.
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
	// This node may itself be behind the log: catch up first, so that new
	// members are brought up to the service's epoch, not a stale one.
	p.cms.CatchUpFromMembers()
	upToDate := func(m string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		epoch, err := p.cms.Sync(ctx, m)
		if err != nil {
			return fmt.Errorf("db: bringing %s up to date with the metadata log: %w", m, err)
		}
		if epoch < d.schema.Load().Epoch {
			return fmt.Errorf("db: %s is behind the metadata log (epoch %d)", m, epoch)
		}
		return nil
	}
	for {
		cur := slices.Clone(p.cmsMembers())
		slices.Sort(cur)
		step, next, ok := cmsStep(cur, members)
		if !ok {
			return nil
		}
		switch step[0] {
		case '+':
			if err := upToDate(step[1:]); err != nil {
				return err
			}
		case '-':
			// The members that remain must know the log up to here (the
			// additions included) before one leaves: otherwise one could be
			// left alone, believing the old members still serve.
			for _, m := range next {
				if err := upToDate(m); err != nil {
					return err
				}
			}
		}
		_, err := d.change("RECONFIGURE_CMS", func(n *Schema) (message.Message, bool, error) {
			have := slices.Clone(n.CMS)
			if len(have) == 0 {
				have = slices.Clone(p.initialCMS)
			}
			slices.Sort(have)
			if !slices.Equal(have, cur) {
				return nil, false, errors.New("db: the metadata service was reconfigured concurrently")
			}
			n.CMS = next
			n.logDetail = step
			return nil, true, nil
		})
		if err != nil {
			return err
		}
	}
}

// cmsStep returns the next step from the members cur to want ("+addr" or
// "-addr") and the members after it, or false when there is none.
func cmsStep(cur, want []string) (string, []string, bool) {
	for _, m := range want {
		if !slices.Contains(cur, m) {
			next := append(slices.Clone(cur), m)
			slices.Sort(next)
			return "+" + m, next, true
		}
	}
	for i, m := range cur {
		if !slices.Contains(want, m) {
			return "-" + m, slices.Delete(slices.Clone(cur), i, i+1), true
		}
	}
	return "", nil, false
}
