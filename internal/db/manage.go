package db

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/pbonard/cassandra-room/internal/storage/lsm"
)

// Operations for the management API (what Cassandra's Management API and
// nodetool expose to an operator): node and ring information, flushing,
// snapshots and imports for backups, range repairs.

// NodeInfo describes a node of the cluster.
type NodeInfo struct {
	HostID       string `json:"hostId"`
	Address      string `json:"address"` // internode, the node's identity
	InternalAddr string `json:"internalAddress,omitempty"`
	NativeAddr   string `json:"nativeAddress"`
	NativePort   int    `json:"nativePort"`
	DataCenter   string `json:"datacenter"`
	Rack         string `json:"rack"`
	Status       string `json:"status"` // NORMAL, JOINING, LEAVING, LEFT, REGISTERED
	Up           bool   `json:"up"`
	Tokens       int    `json:"tokens"`
	Ordinal      int    `json:"ordinal,omitempty"`
	CMS          bool   `json:"cms,omitempty"` // a member of the metadata service
	Self         bool   `json:"self,omitempty"`
}

// Nodes describes every node of the cluster (just this one for a single
// node), ordered by address.
func (d *DB) Nodes() []NodeInfo {
	p := d.proxy
	if p == nil {
		return []NodeInfo{{HostID: d.local.HostID, Address: d.opts.Address.String(), NativeAddr: d.opts.Address.String(),
			NativePort: d.opts.Port, DataCenter: d.opts.DataCenter, Rack: d.opts.Rack, Status: cluster.StatusNormal,
			Up: true, Tokens: 1, Self: true}}
	}
	s := d.schema.Load()
	var out []NodeInfo
	for _, n := range nodeList(s) {
		out = append(out, NodeInfo{HostID: n.HostID, Address: n.Addr, InternalAddr: n.InternalAddr, NativeAddr: n.NativeAddr,
			NativePort: n.NativePort, DataCenter: n.DataCenter, Rack: n.Rack, Status: n.Status,
			Up: n.Addr == p.self || p.g.IsAlive(n.Addr), Tokens: len(n.Tokens), Ordinal: n.Ordinal,
			CMS: slices.Contains(p.cmsMembers(), n.Addr), Self: n.HostID == d.local.HostID})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out
}

// LocalNode describes this node.
func (d *DB) LocalNode() NodeInfo {
	for _, n := range d.Nodes() {
		if n.Self {
			return n
		}
	}
	// Not registered yet.
	n := NodeInfo{HostID: d.local.HostID, DataCenter: d.opts.DataCenter, Rack: d.opts.Rack, Self: true, Up: true,
		NativeAddr: d.opts.Address.String(), NativePort: d.opts.Port}
	if d.proxy != nil {
		n.Address = d.proxy.self
	}
	return n
}

// Ready reports whether the node serves its share of the data: a single
// node, or a cluster node in the ring (NORMAL).
func (d *DB) Ready() bool {
	return d.proxy == nil || d.LocalNode().Status == cluster.StatusNormal
}

// RangeInfo is a range of the ring, (Start, End], with its replicas for a
// keyspace.
type RangeInfo struct {
	Start    int64    `json:"start,string"`
	End      int64    `json:"end,string"`
	Replicas []string `json:"replicas"` // internode addresses
}

// Ring returns the ranges of the ring with their replicas for a keyspace
// (nodetool describering).
func (d *DB) Ring(keyspace string) ([]RangeInfo, error) {
	k := d.schema.Load().Keyspaces[keyspace]
	if k == nil {
		return nil, fmt.Errorf("db: keyspace %q does not exist", keyspace)
	}
	if d.proxy == nil {
		return []RangeInfo{{Start: minToken, End: minToken, Replicas: []string{d.opts.Address.String()}}}, nil
	}
	ring := d.proxy.ring.Load()
	var out []RangeInfo
	for _, r := range ring.Ranges() {
		ri := RangeInfo{Start: r[0], End: r[1]}
		for _, n := range ring.Replicas(k.Replication, r[1]) {
			ri.Replicas = append(ri.Replicas, n.Addr)
		}
		out = append(out, ri)
	}
	return out, nil
}

const minToken = -1 << 63

// Flush writes every table's memtable to sstables (nodetool flush).
func (d *DB) Flush() error {
	for _, k := range d.schema.Load().Keyspaces {
		for _, t := range k.Tables {
			if t.virtual != nil {
				continue
			}
			lt, err := d.engine.Table(t.Storage)
			if errors.Is(err, lsm.ErrUnknownTable) {
				continue
			} else if err != nil {
				return err
			}
			if err := lt.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// SnapshotTable is one table's part of a snapshot.
type SnapshotTable struct {
	Keyspace string   `json:"keyspace"`
	Table    string   `json:"table"`
	ID       string   `json:"id"`    // the table's id (schema)
	Files    []string `json:"files"` // the sstables, under the data directory
}

// Snapshot flushes the user tables of the keyspaces (all if none) and
// hard-links their sstables into a snapshot named name (nodetool
// snapshot), returning the files.
func (d *DB) Snapshot(name string, keyspaces []string) ([]SnapshotTable, error) {
	var out []SnapshotTable
	for _, t := range d.userTables() {
		if t.virtual != nil || len(keyspaces) > 0 && !slices.Contains(keyspaces, t.Keyspace) {
			continue
		}
		lt, err := d.engine.Table(t.Storage)
		if errors.Is(err, lsm.ErrUnknownTable) {
			continue
		} else if err != nil {
			return nil, err
		}
		if _, err := lt.SnapshotToDisk(name); err != nil {
			return nil, err
		}
		files, err := lt.SnapshotFiles(name)
		if err != nil {
			return nil, err
		}
		out = append(out, SnapshotTable{Keyspace: t.Keyspace, Table: t.Name, ID: tableIDString(t), Files: files})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Keyspace+"."+out[i].Table < out[j].Keyspace+"."+out[j].Table
	})
	return out, nil
}

// ClearSnapshot deletes a snapshot (nodetool clearsnapshot).
func (d *DB) ClearSnapshot(name string) error {
	return d.engine.ClearSnapshot(name)
}

// Import loads sstables into a table on this node (nodetool import): rows
// reconcile with the table's by timestamp. The files are not modified.
func (d *DB) Import(keyspace, table string, files []string) error {
	t, err := d.schema.Load().table(keyspace, table)
	if err != nil {
		return err
	}
	if t.virtual != nil {
		return fmt.Errorf("db: cannot import into virtual table %s.%s", keyspace, table)
	}
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return err
	}
	return lt.Import(d.fs, files)
}

func tableIDString(t *Table) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", t.ID[0:4], t.ID[4:6], t.ID[6:8], t.ID[8:10], t.ID[10:16])
}
