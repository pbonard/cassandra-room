package db

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/datastax/go-cassandra-native-protocol/primitive"

	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/pbonard/cassandra-room/internal/storage/lsm"
	"github.com/pbonard/cassandra-room/internal/storage/row"
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

// Keyspaces returns the names of the user keyspaces, sorted.
func (d *DB) Keyspaces() []string {
	seen := map[string]bool{}
	for _, t := range d.userTables() {
		seen[t.Keyspace] = true
	}
	for name, k := range d.schema.Load().Keyspaces {
		if !k.virtual && !strings.HasPrefix(name, "system") {
			seen[name] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

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
	ID       string   `json:"id"`      // the table's id (schema)
	Storage  uint64   `json:"storage"` // its storage's id, which names it in the commit log
	Files    []string `json:"files"`   // the sstables, under the data directory
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
		out = append(out, SnapshotTable{Keyspace: t.Keyspace, Table: t.Name, ID: tableIDString(t), Storage: uint64(t.Storage), Files: files})
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

// LoadResult counts what Load wrote.
type LoadResult struct {
	Entries int64 `json:"entries"` // rows, partition headers and range tombstones
	Bytes   int64 `json:"bytes"`
}

// loadBatch is how many entries Load writes in one mutation.
const loadBatch = 256

// Load writes the entries of sstables (of this table, from any node of any
// cluster, for example a backup's) through the coordinator at consistency
// cl, to the replicas that own them in this cluster (Cassandra's
// sstableloader). Unlike Import, the topology may differ from the one that
// wrote the files: each partition goes to its replicas here. Rows keep their
// timestamps and deletions, so loading is idempotent and reconciles with
// the table's rows; counters merge their shards as streaming does.
func (d *DB) Load(keyspace, table string, files []string, cl primitive.ConsistencyLevel) (LoadResult, error) {
	var res LoadResult
	t, err := d.schema.Load().table(keyspace, table)
	if err != nil {
		return res, err
	}
	if t.virtual != nil {
		return res, fmt.Errorf("db: cannot load into virtual table %s.%s", keyspace, table)
	}
	m := &mutation{}
	flush := func() error {
		if len(m.writes) == 0 {
			return nil
		}
		err := d.store.apply(m, cl)
		m = &mutation{}
		return err
	}
	for _, f := range files {
		err := lsm.ReadExternal(d.fs, f, func(k, v []byte) error {
			if len(k) < 8 {
				return fmt.Errorf("db: %s: key %x is not a %s.%s key", f, k, keyspace, table)
			}
			if _, err := row.Decode(v); err != nil {
				return fmt.Errorf("db: %s: %w", f, err)
			}
			m.put(t.Storage, slices.Clone(k), v, tokenOf(k))
			res.Entries++
			res.Bytes += int64(len(k) + len(v))
			if len(m.writes) >= loadBatch {
				return flush()
			}
			return nil
		})
		if err != nil {
			return res, err
		}
	}
	return res, flush()
}

// ParseConsistency parses a consistency level's name (ONE, QUORUM,
// LOCAL_QUORUM, ALL...), in any case.
func ParseConsistency(s string) (primitive.ConsistencyLevel, error) {
	for _, cl := range []primitive.ConsistencyLevel{primitive.ConsistencyLevelAny, primitive.ConsistencyLevelOne,
		primitive.ConsistencyLevelTwo, primitive.ConsistencyLevelThree, primitive.ConsistencyLevelQuorum,
		primitive.ConsistencyLevelAll, primitive.ConsistencyLevelLocalQuorum, primitive.ConsistencyLevelEachQuorum,
		primitive.ConsistencyLevelLocalOne} {
		if strings.EqualFold(clName(cl), s) {
			return cl, nil
		}
	}
	return 0, fmt.Errorf("db: unknown consistency level %q", s)
}

// ArchiveCommitLog closes the commit log's current segment, so that every
// write acknowledged so far is archived, and returns the archived segments
// (oldest first). It needs the engine's WALArchiveDir.
func (d *DB) ArchiveCommitLog() ([]string, error) {
	dir := d.engine.WALArchiveDir()
	if dir == "" {
		return nil, errors.New("db: commit log archiving is off")
	}
	if err := d.engine.ArchiveWAL(); err != nil {
		return nil, err
	}
	names, err := d.fs.List(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range names {
		if strings.HasSuffix(n, ".wal") {
			out = append(out, d.fs.PathJoin(dir, n))
		}
	}
	slices.Sort(out) // fixed-width numbers: oldest first
	return out, nil
}

// ReplayRequest replays archived commit log segments.
type ReplayRequest struct {
	Files []string `json:"files"`
	// Tables maps the storage ids the segments name (as a backup's
	// manifest records them) to "keyspace.table". Writes to other storage
	// (system tables, or a table's storage before a TRUNCATE) are skipped.
	Tables map[string]string `json:"tables"`
	// Until is the point in time, in microseconds since the epoch: cells and
	// deletions written later (by their timestamps) are left out, as
	// Cassandra's commitlog restore_point_in_time does. Zero replays all.
	Until int64 `json:"until,omitempty"`
}

// ReplayResult counts what Replay applied and skipped.
type ReplayResult struct {
	Applied int64 `json:"applied"`
	Skipped int64 `json:"skipped"` // other tables', or entirely after Until
}

// Replay applies archived commit log segments of this node to its tables,
// locally (each node replays its own, as each imports its own backup):
// point-in-time recovery on top of a restored backup. Writes merge with the
// tables' rows by timestamp, so replaying writes the backup already holds is
// harmless.
func (d *DB) Replay(req ReplayRequest) (ReplayResult, error) {
	var res ReplayResult
	s := d.schema.Load()
	to := map[lsm.TableID]lsm.TableID{}
	for from, name := range req.Tables {
		id, err := strconv.ParseUint(from, 10, 64)
		if err != nil {
			return res, fmt.Errorf("db: storage id %q: %w", from, err)
		}
		ks, tb, ok := strings.Cut(name, ".")
		if !ok {
			return res, fmt.Errorf("db: table %q is not keyspace.table", name)
		}
		t, err := s.table(ks, tb)
		if err != nil {
			return res, err
		}
		to[lsm.TableID(id)] = t.Storage
	}
	var batch []write
	flush := func() error {
		err := d.applyLocal(batch)
		batch = batch[:0]
		return err
	}
	for _, f := range req.Files {
		err := lsm.ReadWAL(d.fs, f, func(w lsm.WALEntry) error {
			id, ok := to[w.Table]
			if !ok || len(w.Key) < 8 {
				res.Skipped++
				return nil
			}
			v := w.Value
			if req.Until != 0 {
				r, err := row.Decode(v)
				if err != nil {
					return fmt.Errorf("db: %s: %w", f, err)
				}
				if !untilPointInTime(r, req.Until) {
					res.Skipped++
					return nil
				}
				v = row.Append(nil, r)
			}
			batch = append(batch, write{table: id, key: slices.Clone(w.Key), value: slices.Clone(v), token: tokenOf(w.Key)})
			res.Applied++
			if len(batch) >= loadBatch {
				return flush()
			}
			return nil
		})
		if err != nil {
			return res, err
		}
	}
	return res, flush()
}

// untilPointInTime drops from r the cells and deletion written after until
// (microseconds), reporting whether anything is left.
func untilPointInTime(r *row.Row, until int64) bool {
	if r.Deletion.MarkedForDeleteAt > until {
		r.Deletion = row.Deletion{MarkedForDeleteAt: row.NoTimestamp}
	}
	cells := r.Cells[:0]
	for _, c := range r.Cells {
		if c.Timestamp <= until {
			cells = append(cells, c)
		}
	}
	r.Cells = cells
	return !r.Deletion.IsLive() || len(r.Cells) > 0
}
