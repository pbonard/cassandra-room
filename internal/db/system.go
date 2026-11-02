package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
)

// The system keyspaces drivers read on connection (SystemKeyspace,
// SchemaKeyspace): system.local and system.peers describe the cluster,
// system_schema the schema. They are virtual: their rows are generated
// from the node's state when read.

// ReleaseVersion is the Cassandra version this server reports.
const ReleaseVersion = "5.0.4"

var startTime = time.Now()

var systemTables = map[string]map[string]string{
	"system": {
		"local": `CREATE TABLE local (key text, bootstrapped text, broadcast_address inet, broadcast_port int,
			cluster_name text, cql_version text, data_center text, gossip_generation int, host_id uuid,
			listen_address inet, listen_port int, native_protocol_version text, partitioner text, rack text,
			release_version text, rpc_address inet, rpc_port int, schema_version uuid, tokens set<varchar>,
			truncated_at map<uuid, blob>, PRIMARY KEY ((key)))`,
		"size_estimates": `CREATE TABLE size_estimates (keyspace_name text, table_name text, range_start text,
			range_end text, mean_partition_size bigint, partitions_count bigint,
			PRIMARY KEY ((keyspace_name), table_name, range_start, range_end))`,
		"table_estimates": `CREATE TABLE table_estimates (keyspace_name text, table_name text, range_type text,
			range_start text, range_end text, mean_partition_size bigint, partitions_count bigint,
			PRIMARY KEY ((keyspace_name), table_name, range_type, range_start, range_end))`,
		"peers": `CREATE TABLE peers (peer inet, data_center text, host_id uuid, preferred_ip inet, rack text,
			release_version text, rpc_address inet, schema_version uuid, tokens set<varchar>, PRIMARY KEY ((peer)))`,
		"peers_v2": `CREATE TABLE peers_v2 (peer inet, peer_port int, data_center text, host_id uuid, preferred_ip inet,
			preferred_port int, rack text, release_version text, native_address inet, native_port int,
			schema_version uuid, tokens set<varchar>, PRIMARY KEY ((peer), peer_port))`,
	},
	"system_traces": {
		"sessions": `CREATE TABLE sessions (session_id uuid, client inet, command text, coordinator inet,
			coordinator_port int, duration int, parameters map<text, text>, request text, started_at timestamp,
			PRIMARY KEY ((session_id)))`,
		"events": `CREATE TABLE events (session_id uuid, event_id timeuuid, activity text, source inet,
			source_elapsed int, source_port int, thread text, PRIMARY KEY ((session_id), event_id))`,
	},
	"system_views": {
		"settings":     `CREATE TABLE settings (name text, value text, PRIMARY KEY ((name)))`,
		"disk_usage":   `CREATE TABLE disk_usage (keyspace_name text, table_name text, mebibytes bigint, PRIMARY KEY ((keyspace_name), table_name))`,
		"thread_pools": `CREATE TABLE thread_pools (name text, active_tasks int, active_tasks_limit int, dropped_messages bigint, PRIMARY KEY ((name)))`,
		"cluster_metadata_log": `CREATE TABLE cluster_metadata_log (epoch bigint, entry_id text, entry_time timestamp,
			kind text, transformation text, PRIMARY KEY ((epoch)))`,
		"cluster_metadata_directory": `CREATE TABLE cluster_metadata_directory (host_id uuid, address text,
			native_address text, state text, data_center text, rack text, release_version text,
			tokens set<text>, cms_member boolean, replaces uuid, PRIMARY KEY ((host_id)))`,
	},
	"system_schema": {
		"keyspaces": `CREATE TABLE keyspaces (keyspace_name text, durable_writes boolean,
			replication frozen<map<text, text>>, PRIMARY KEY ((keyspace_name)))`,
		"tables": `CREATE TABLE tables (keyspace_name text, table_name text, additional_write_policy text,
			allow_auto_snapshot boolean, bloom_filter_fp_chance double, caching frozen<map<text, text>>, cdc boolean,
			comment text, compaction frozen<map<text, text>>, compression frozen<map<text, text>>,
			crc_check_chance double, dclocal_read_repair_chance double, default_time_to_live int,
			extensions frozen<map<text, blob>>, flags frozen<set<text>>, gc_grace_seconds int, id uuid,
			incremental_backups boolean, max_index_interval int, memtable text, memtable_flush_period_in_ms int,
			min_index_interval int, read_repair text, read_repair_chance double, speculative_retry text,
			PRIMARY KEY ((keyspace_name), table_name))`,
		"columns": `CREATE TABLE columns (keyspace_name text, table_name text, column_name text,
			clustering_order text, column_name_bytes blob, kind text, position int, type text,
			PRIMARY KEY ((keyspace_name), table_name, column_name))`,
		"dropped_columns": `CREATE TABLE dropped_columns (keyspace_name text, table_name text, column_name text,
			dropped_time timestamp, kind text, type text, PRIMARY KEY ((keyspace_name), table_name, column_name))`,
		"triggers": `CREATE TABLE triggers (keyspace_name text, table_name text, trigger_name text,
			options frozen<map<text, text>>, PRIMARY KEY ((keyspace_name), table_name, trigger_name))`,
		"views": `CREATE TABLE views (keyspace_name text, view_name text, base_table_id uuid, base_table_name text,
			bloom_filter_fp_chance double, caching frozen<map<text, text>>, cdc boolean, comment text,
			compaction frozen<map<text, text>>, compression frozen<map<text, text>>, crc_check_chance double,
			dclocal_read_repair_chance double, default_time_to_live int, extensions frozen<map<text, blob>>,
			gc_grace_seconds int, id uuid, include_all_columns boolean, max_index_interval int,
			memtable_flush_period_in_ms int, min_index_interval int, read_repair_chance double,
			speculative_retry text, where_clause text, PRIMARY KEY ((keyspace_name), view_name))`,
		"types": `CREATE TABLE types (keyspace_name text, type_name text, field_names frozen<list<text>>,
			field_types frozen<list<text>>, PRIMARY KEY ((keyspace_name), type_name))`,
		"functions": `CREATE TABLE functions (keyspace_name text, function_name text,
			argument_types frozen<list<text>>, argument_names frozen<list<text>>, body text, called_on_null_input boolean,
			language text, return_type text, PRIMARY KEY ((keyspace_name), function_name, argument_types))`,
		"aggregates": `CREATE TABLE aggregates (keyspace_name text, aggregate_name text,
			argument_types frozen<list<text>>, final_func text, initcond text, return_type text, state_func text,
			state_type text, PRIMARY KEY ((keyspace_name), aggregate_name, argument_types))`,
		"indexes": `CREATE TABLE indexes (keyspace_name text, table_name text, index_name text, kind text,
			options frozen<map<text, text>>, PRIMARY KEY ((keyspace_name), table_name, index_name))`,
	},
}

func addSystemKeyspaces(s *Schema) {
	for ksName, tables := range systemTables {
		k := &Keyspace{Name: ksName, DurableWrites: true, Tables: map[string]*Table{}, virtual: true,
			Replication: map[string]string{"class": "org.apache.cassandra.locator.LocalStrategy"}}
		for name, ddl := range tables {
			p, err := cql.Parse(ddl)
			if err != nil {
				panic(err)
			}
			t, err := buildTable(ksName, nil, p.Statement.(*cql.CreateTable), true)
			if err != nil {
				panic(err)
			}
			id := md5UUID(ksName + "." + name)
			t.ID = id
			t.virtual = virtualRows(ksName, name)
			k.Tables[name] = t
		}
		s.Keyspaces[ksName] = k
	}
}

func md5UUID(s string) [16]byte {
	var u [16]byte
	h := StatementID("", s)
	copy(u[:], h)
	u[6] = u[6]&0x0f | 0x30
	u[8] = u[8]&0x3f | 0x80
	return u
}

func virtualRows(ks, table string) func(d *DB) []map[string][]byte {
	switch ks + "." + table {
	case "system.local":
		return localRows
	case "system.peers":
		return func(d *DB) []map[string][]byte { return peerRows(d, false) }
	case "system.peers_v2":
		return func(d *DB) []map[string][]byte { return peerRows(d, true) }
	case "system_schema.keyspaces":
		return keyspaceRows
	case "system_schema.tables":
		return tableRows
	case "system_schema.columns":
		return columnRows
	case "system_schema.types":
		return typeRows
	case "system_schema.indexes":
		return indexRows
	case "system.size_estimates":
		return func(d *DB) []map[string][]byte { return estimateRows(d, false) }
	case "system.table_estimates":
		return func(d *DB) []map[string][]byte { return estimateRows(d, true) }
	case "system_traces.sessions":
		return traceSessionRows
	case "system_traces.events":
		return traceEventRows
	case "system_views.settings":
		return settingRows
	case "system_views.disk_usage":
		return diskUsageRows
	case "system_views.thread_pools":
		return threadPoolRows
	case "system_views.cluster_metadata_log":
		return metadataLogRows
	case "system_views.cluster_metadata_directory":
		return directoryRows
	}
	return func(*DB) []map[string][]byte { return nil }
}

// Value encoders.
func textV(s string) []byte { return []byte(s) }
func intV(n int) []byte     { return binary.BigEndian.AppendUint32(nil, uint32(int32(n))) }
func boolV(b bool) []byte   { return []byte{boolByte(b)} }
func doubleV(f float64) []byte {
	b, _ := types.FromString(types.DoubleType, strconv.FormatFloat(f, 'g', -1, 64))
	return b
}
func uuidV(u []byte) []byte { return slices.Clone(u) }

func textSetV(elems ...string) []byte {
	sort.Strings(elems)
	vals := make([][]byte, len(elems))
	for i, e := range elems {
		vals[i] = []byte(e)
	}
	return types.BuildCollection(len(vals), vals)
}

func textMapV(m map[string]string) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var vals [][]byte
	for _, k := range keys {
		vals = append(vals, []byte(k), []byte(m[k]))
	}
	return types.BuildCollection(len(keys), vals)
}

func localRows(d *DB) []map[string][]byte {
	addr := d.opts.Address.To4()
	if addr == nil {
		addr = d.opts.Address.To16()
	}
	hostID, _ := types.ParseUUID(d.local.HostID)
	v := d.schema.Load().Version
	return []map[string][]byte{{
		"key":                     textV("local"),
		"bootstrapped":            textV("COMPLETED"),
		"broadcast_address":       addr,
		"broadcast_port":          intV(7000),
		"cluster_name":            textV(d.opts.ClusterName),
		"cql_version":             textV("3.4.7"),
		"data_center":             textV(d.opts.DataCenter),
		"gossip_generation":       intV(int(startTime.Unix())),
		"host_id":                 uuidV(hostID[:]),
		"listen_address":          addr,
		"listen_port":             intV(7000),
		"native_protocol_version": textV("5"),
		"partitioner":             textV("org.apache.cassandra.dht.Murmur3Partitioner"),
		"rack":                    textV(d.opts.Rack),
		"release_version":         textV(ReleaseVersion),
		"rpc_address":             addr,
		"rpc_port":                intV(d.opts.Port),
		"schema_version":          uuidV(v[:]),
		"tokens":                  textSetV(d.localTokens()...),
	}}
}

// localTokens are the node's tokens; a single node owns the whole ring.
func (d *DB) localTokens() []string {
	if d.proxy == nil {
		return []string{"-9223372036854775808"}
	}
	return tokenStrings(d.local.Tokens)
}

func peerRows(d *DB, v2 bool) []map[string][]byte {
	if d.proxy == nil {
		return nil
	}
	var out []map[string][]byte
	// With transactional metadata every node converges on the same schema.
	schemaID := d.schema.Load().Version
	for _, s := range d.proxy.peers() {
		hostID, _ := types.ParseUUID(s.HostID)
		r := map[string][]byte{
			"peer":            ipBytes(s.Addr),
			"data_center":     textV(s.DataCenter),
			"host_id":         uuidV(hostID[:]),
			"rack":            textV(s.Rack),
			"release_version": textV(s.ReleaseVersion),
			"schema_version":  uuidV(schemaID[:]),
			"tokens":          textSetV(tokenStrings(s.Tokens)...),
		}
		if v2 {
			_, port, _ := net.SplitHostPort(s.Addr)
			pn, _ := strconv.Atoi(port)
			r["peer_port"] = intV(pn)
			r["native_address"] = ipBytes(s.NativeAddr)
			r["native_port"] = intV(s.NativePort)
			r["preferred_ip"] = ipBytes(s.Addr)
			r["preferred_port"] = intV(pn)
		} else {
			r["rpc_address"] = ipBytes(s.NativeAddr)
			r["preferred_ip"] = ipBytes(s.Addr)
		}
		out = append(out, r)
	}
	return out
}

func sortedKeyspaces(d *DB) []*Keyspace {
	s := d.schema.Load()
	out := make([]*Keyspace, 0, len(s.Keyspaces))
	for _, k := range s.Keyspaces {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func keyspaceRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, k := range sortedKeyspaces(d) {
		out = append(out, map[string][]byte{
			"keyspace_name":  textV(k.Name),
			"durable_writes": boolV(k.DurableWrites),
			"replication":    textMapV(k.Replication),
		})
	}
	return out
}

func tableRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, k := range sortedKeyspaces(d) {
		for _, t := range k.Tables {
			p := t.Params
			out = append(out, map[string][]byte{
				"keyspace_name":               textV(k.Name),
				"table_name":                  textV(t.Name),
				"additional_write_policy":     textV(p.AdditionalWrite),
				"allow_auto_snapshot":         boolV(true),
				"bloom_filter_fp_chance":      doubleV(p.BloomFilterFPChance),
				"caching":                     textMapV(p.Caching),
				"cdc":                         boolV(p.CDC),
				"comment":                     textV(p.Comment),
				"compaction":                  textMapV(p.Compaction),
				"compression":                 textMapV(p.Compression),
				"crc_check_chance":            doubleV(p.CRCCheckChance),
				"dclocal_read_repair_chance":  doubleV(0),
				"default_time_to_live":        intV(p.DefaultTTL),
				"extensions":                  types.BuildCollection(0, nil),
				"flags":                       textSetV("compound"),
				"gc_grace_seconds":            intV(p.GCGraceSeconds),
				"id":                          uuidV(t.ID[:]),
				"incremental_backups":         boolV(true),
				"max_index_interval":          intV(p.MaxIndexInterval),
				"memtable":                    textV("default"),
				"memtable_flush_period_in_ms": intV(p.MemtableFlushPeriod),
				"min_index_interval":          intV(p.MinIndexInterval),
				"read_repair":                 textV(p.ReadRepair),
				"read_repair_chance":          doubleV(0),
				"speculative_retry":           textV(p.SpeculativeRetry),
			})
		}
	}
	return out
}

func columnRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, k := range sortedKeyspaces(d) {
		for _, t := range k.Tables {
			for _, c := range t.AllColumns() {
				order := "none"
				if c.Kind == Clustering {
					order = "asc"
					if c.Desc {
						order = "desc"
					}
				}
				pos := c.Position
				if c.Kind == Regular || c.Kind == Static {
					pos = -1
				}
				out = append(out, map[string][]byte{
					"keyspace_name":     textV(k.Name),
					"table_name":        textV(t.Name),
					"column_name":       textV(c.Name),
					"clustering_order":  textV(order),
					"column_name_bytes": []byte(c.Name),
					"kind":              textV(c.Kind.String()),
					"position":          intV(pos),
					"type":              textV(c.Type.String()),
				})
			}
		}
	}
	return out
}

// virtualSource serves a virtual table's rows, encoded as stored rows, to
// the read path.
type virtualSource struct {
	keys, vals [][]byte
}

func newVirtualSource(d *DB, t *Table) *virtualSource {
	type entry struct{ k, v []byte }
	var entries []entry
	for _, m := range t.virtual(d) {
		pkVals := make([][]byte, len(t.PartitionKey))
		for i, c := range t.PartitionKey {
			pkVals[i] = m[c.Name]
		}
		key := partitionPrefix(serializePartitionKey(pkVals))
		if len(t.Clustering) > 0 {
			ckVals := make([][]byte, len(t.Clustering))
			for i, c := range t.Clustering {
				ckVals[i] = m[c.Name]
			}
			var err error
			if key, err = appendClustering(key, t, ckVals); err != nil {
				continue
			}
		}
		r := &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
			Cells: []row.Cell{{Column: 0, LocalDeletionTime: row.NoDeletionTime}}}
		for _, c := range t.Columns {
			v, ok := m[c.Name]
			if !ok || v == nil {
				continue
			}
			if c.Type.IsMultiCell() {
				cells, err := d.elementCells(c, v, 0, false, func(p, x []byte) row.Cell {
					return row.Cell{Column: c.ID, Path: p, LocalDeletionTime: row.NoDeletionTime, Value: x}
				})
				if err == nil {
					r.Cells = append(r.Cells, cells...)
				}
				continue
			}
			r.Cells = append(r.Cells, row.Cell{Column: c.ID, LocalDeletionTime: row.NoDeletionTime, Value: v})
		}
		sortCells(r.Cells)
		entries = append(entries, entry{key, row.Append(nil, r)})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].k, entries[j].k) < 0 })
	s := &virtualSource{}
	for _, e := range entries {
		s.keys, s.vals = append(s.keys, e.k), append(s.vals, e.v)
	}
	return s
}

func (s *virtualSource) get(key []byte) ([]byte, error) {
	i := sort.Search(len(s.keys), func(i int) bool { return bytes.Compare(s.keys[i], key) >= 0 })
	if i < len(s.keys) && bytes.Equal(s.keys[i], key) {
		return s.vals[i], nil
	}
	return nil, errNotFound
}

func (s *virtualSource) iter(lo, hi []byte) (iterator, error) {
	return &sliceIter{s: s, lo: lo, hi: hi, i: -1}, nil
}

type sliceIter struct {
	s      *virtualSource
	lo, hi []byte
	i      int
}

func (it *sliceIter) inRange() bool {
	return it.i >= 0 && it.i < len(it.s.keys) && (it.hi == nil || bytes.Compare(it.s.keys[it.i], it.hi) < 0)
}
func (it *sliceIter) First() bool {
	if it.lo == nil {
		it.i = 0
		return it.inRange()
	}
	return it.SeekGE(it.lo)
}
func (it *sliceIter) SeekGE(key []byte) bool {
	if it.lo != nil && bytes.Compare(key, it.lo) < 0 {
		key = it.lo
	}
	it.i = sort.Search(len(it.s.keys), func(i int) bool { return bytes.Compare(it.s.keys[i], key) >= 0 })
	return it.inRange()
}
func (it *sliceIter) Next() bool {
	it.i++
	return it.inRange()
}
func (it *sliceIter) Valid() bool   { return it.inRange() }
func (it *sliceIter) Key() []byte   { return it.s.keys[it.i] }
func (it *sliceIter) Value() []byte { return it.s.vals[it.i] }
func (it *sliceIter) Error() error  { return nil }
func (it *sliceIter) Close() error  { return nil }

var _ = strings.ToLower

func textListV(elems []string) []byte {
	vals := make([][]byte, len(elems))
	for i, e := range elems {
		vals[i] = []byte(e)
	}
	return types.BuildCollection(len(vals), vals)
}

func typeRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, k := range sortedKeyspaces(d) {
		for _, u := range k.UserTypes {
			t := k.types[u.Name]
			var ft []string
			for _, e := range t.Elems {
				ft = append(ft, e.String())
			}
			out = append(out, map[string][]byte{
				"keyspace_name": textV(k.Name),
				"type_name":     textV(u.Name),
				"field_names":   textListV(u.Fields),
				"field_types":   textListV(ft),
			})
		}
	}
	return out
}

func indexRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, k := range sortedKeyspaces(d) {
		for _, t := range k.Tables {
			for _, ix := range t.Indexes {
				kind, opts := "COMPOSITES", map[string]string{"target": ix.targetString()}
				if ix.Class != "" {
					kind = "CUSTOM"
					opts["class_name"] = ix.Class
				}
				for k, v := range ix.Options {
					opts[k] = v
				}
				out = append(out, map[string][]byte{
					"keyspace_name": textV(k.Name),
					"table_name":    textV(t.Name),
					"index_name":    textV(ix.Name),
					"kind":          textV(kind),
					"options":       textMapV(opts),
				})
			}
		}
	}
	return out
}

func bigintV(n int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(n)) }

func metadataLogRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, e := range d.schema.Load().Log {
		out = append(out, map[string][]byte{"epoch": bigintV(e.Epoch), "entry_id": textV(e.ID),
			"entry_time": bigintV(e.Time), "kind": textV(e.Kind), "transformation": textV(e.Detail)})
	}
	return out
}

func directoryRows(d *DB) []map[string][]byte {
	s := d.schema.Load()
	var out []map[string][]byte
	for _, n := range s.Nodes {
		id, _ := types.ParseUUID(n.HostID)
		r := map[string][]byte{"host_id": uuidV(id[:]), "address": textV(n.Addr),
			"native_address": textV(fmt.Sprintf("%s:%d", n.NativeAddr, n.NativePort)), "state": textV(n.Status),
			"data_center": textV(n.DataCenter), "rack": textV(n.Rack), "release_version": textV(n.ReleaseVersion),
			"tokens": textSetV(tokenStrings(n.Tokens)...), "cms_member": boolV(slices.Contains(s.CMS, n.Addr))}
		if n.Replaces != "" {
			old, _ := types.ParseUUID(n.Replaces)
			r["replaces"] = uuidV(old[:])
		}
		out = append(out, r)
	}
	return out
}

// ---- estimates and node views ----

// primaryRanges are the token ranges this node is the first replica of
// for a keyspace (the whole ring on a single node).
func (d *DB) primaryRanges(k *Keyspace) [][2]int64 {
	p := d.proxy
	if p == nil {
		return [][2]int64{{math.MinInt64, math.MaxInt64}}
	}
	var out [][2]int64
	for _, sg := range p.tokenSegments() {
		if reps := p.ring.Load().Replicas(k.Replication, sg[1]); len(reps) > 0 && reps[0].Addr == p.self {
			out = append(out, sg)
		}
	}
	return out
}

// tableEstimate estimates a table's partitions and mean partition size:
// rows from the engine's summaries, partitions per row from a sample of
// the first keys for tables with clustering columns.
func (d *DB) tableEstimate(t *Table) (partitions, meanSize int64) {
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return 0, 0
	}
	sz := lt.Size()
	partitions = sz.Rows
	if len(t.Clustering) > 0 && sz.Rows > 0 {
		it, err := lt.NewIter(nil, nil)
		if err == nil {
			keys, parts := 0, 0
			var last []byte
			for ok := it.First(); ok && keys < 4096; ok = it.Next() {
				keys++
				_, _, rest, err := parsePartition(it.Key())
				if err != nil {
					continue
				}
				p := it.Key()[:len(it.Key())-len(rest)]
				if !bytes.Equal(p, last) {
					parts++
					last = append(last[:0], p...)
				}
			}
			it.Close()
			if keys > 0 {
				partitions = max(1, sz.Rows*int64(parts)/int64(keys))
			}
		}
	}
	if partitions > 0 {
		meanSize = sz.Bytes / partitions
	}
	return partitions, meanSize
}

// estimateRows are system.size_estimates (or, with rangeType,
// system.table_estimates' primary ranges): per table and primary range,
// the share of the table's partitions the range's token span holds.
func estimateRows(d *DB, rangeType bool) []map[string][]byte {
	var out []map[string][]byte
	for _, t := range d.userTables() {
		k := d.schema.Load().Keyspaces[t.Keyspace]
		parts, mean := d.tableEstimate(t)
		for _, r := range d.primaryRanges(k) {
			share := (float64(r[1]) - float64(r[0])) / math.Pow(2, 64)
			row := map[string][]byte{"keyspace_name": textV(t.Keyspace), "table_name": textV(t.Name),
				"range_start": textV(strconv.FormatInt(r[0], 10)), "range_end": textV(strconv.FormatInt(r[1], 10)),
				"mean_partition_size": bigintV(mean), "partitions_count": bigintV(int64(float64(parts) * share))}
			if rangeType {
				row["range_type"] = textV("primary")
			}
			out = append(out, row)
		}
	}
	return out
}

func settingRows(d *DB) []map[string][]byte {
	o := d.opts
	set := map[string]string{
		"cluster_name": o.ClusterName, "native_transport_port": strconv.Itoa(o.Port),
		"rpc_address": o.Address.String(), "authenticator": map[bool]string{true: "PasswordAuthenticator", false: "AllowAllAuthenticator"}[o.Authenticator],
		"authorizer":      map[bool]string{true: "CassandraAuthorizer", false: "AllowAllAuthorizer"}[o.Authorizer],
		"commitlog_sync":  map[bool]string{true: "batch", false: "periodic"}[o.SyncWrites],
		"endpoint_snitch": "GossipingPropertyFileSnitch",
		"partitioner":     "org.apache.cassandra.dht.Murmur3Partitioner",
		"data_center":     o.DataCenter,
		"rack":            o.Rack,
	}
	if p := d.proxy; p != nil {
		set["listen_address"] = p.self
		set["num_tokens"] = strconv.Itoa(len(d.local.Tokens))
		set["seed_provider"] = strings.Join(p.opts.Seeds, ",")
		set["write_request_timeout"] = p.opts.WriteTimeout.String()
		set["read_request_timeout"] = p.opts.ReadTimeout.String()
		set["max_hint_window"] = p.opts.MaxHintWindow.String()
		set["hinted_handoff_enabled"] = strconv.FormatBool(!p.opts.DisableHints)
		set["internode_compression"] = map[bool]string{true: "all", false: "none"}[p.opts.Messaging.Compress]
		set["server_encryption_options.internode_encryption"] = map[bool]string{true: "all", false: "none"}[p.opts.Messaging.TLS != nil]
	}
	var out []map[string][]byte
	for n, v := range set {
		out = append(out, map[string][]byte{"name": textV(n), "value": textV(v)})
	}
	return out
}

func diskUsageRows(d *DB) []map[string][]byte {
	var out []map[string][]byte
	for _, t := range d.userTables() {
		lt, err := d.engine.Table(t.Storage)
		if err != nil {
			continue
		}
		b := lt.Size().Bytes
		for _, ix := range t.Indexes {
			if it, err := d.engine.Table(ix.Storage); err == nil {
				b += it.Size().Bytes
			}
		}
		out = append(out, map[string][]byte{"keyspace_name": textV(t.Keyspace), "table_name": textV(t.Name), "mebibytes": bigintV(b >> 20)})
	}
	return out
}

func threadPoolRows(d *DB) []map[string][]byte {
	if d.proxy == nil {
		return nil
	}
	var out []map[string][]byte
	for _, s := range d.proxy.m.Stages() {
		out = append(out, map[string][]byte{"name": textV(s.Name), "active_tasks": intV(s.Active),
			"active_tasks_limit": intV(s.Limit), "dropped_messages": bigintV(int64(s.Dropped))})
	}
	return out
}
