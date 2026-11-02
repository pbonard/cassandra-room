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

// fetchRange copies a key range of a table from a node to this one.
func (p *proxy) fetchRange(from string, id lsm.TableID, lo, hi []byte) (int, error) {
	n := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), p.opts.ReadTimeout)
		b, err := p.m.Request(ctx, from, cluster.VerbReadRange, encodeKey(id, lo, hi))
		cancel()
		if err != nil {
			return n, err
		}
		kvs, more, err := decodeKVs(b)
		if err != nil {
			return n, err
		}
		ws := make([]write, len(kvs))
		for i, e := range kvs {
			ws[i] = write{table: id, key: e.k, value: e.v}
		}
		if err := p.d.applyLocal(ws); err != nil {
			return n, err
		}
		n += len(kvs)
		if !more || len(kvs) == 0 {
			return n, nil
		}
		lo = append(slices.Clone(kvs[len(kvs)-1].k), 0)
	}
}

// pushRange sends a key range of a local table to a node.
func (p *proxy) pushRange(to string, id lsm.TableID, lo, hi []byte) (int, error) {
	lt, err := p.d.engine.Table(id)
	if err != nil {
		return 0, err
	}
	it, err := lt.NewIter(lo, hi)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var ws []write
	n := 0
	flush := func() error {
		if len(ws) == 0 {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout*5)
		defer cancel()
		_, err := p.m.Request(ctx, to, cluster.VerbMutation, encodeWrites(ws))
		ws = ws[:0]
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		ws = append(ws, write{table: id, key: slices.Clone(it.Key()), value: slices.Clone(it.Value())})
		n++
		if len(ws) >= rangePage {
			if err := flush(); err != nil {
				return n, err
			}
		}
	}
	if err := it.Error(); err != nil {
		return n, err
	}
	return n, flush()
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
			var src string
			for _, n := range cur.Replicas(k.Replication, r[1]) {
				if n.Addr != p.self && p.g.IsAlive(n.Addr) {
					src = n.Addr
					break
				}
			}
			if src == "" {
				continue
			}
			for _, kr := range keyRangesOf(r[0], r[1]) {
				if _, err := p.fetchRange(src, t.Storage, kr[0], kr[1]); err != nil {
					return fmt.Errorf("db: bootstrap streaming from %s: %w", src, err)
				}
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
		if !containsAddr(current, n.Addr) {
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
	if slices.Contains(p.cms.Members(), p.self) {
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
	if err := p.setStatus(cluster.StatusLeaving); err != nil {
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
					if _, err := p.pushRange(n.Addr, t.Storage, kr[0], kr[1]); err != nil {
						return fmt.Errorf("db: decommission streaming to %s: %w", n.Addr, err)
					}
				}
			}
		}
	}
	p.replayHints()
	// FinishLeave.
	return p.setStatus(cluster.StatusLeft)
}

// ---- repair ----

const repairDepth = 10

type treeRequest struct {
	Table       uint64
	First, Last uint64
}

func (p *proxy) handleRepair(_ string, payload []byte) ([]byte, error) {
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
	Ranges, Mismatches, Streamed int
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
						n, err := p.fetchRange(peer.Addr, t.Storage, lo, hi)
						if err != nil {
							return st, err
						}
						m, err := p.pushRange(peer.Addr, t.Storage, lo, hi)
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

