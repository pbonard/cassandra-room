// Package db is the execution layer: it keeps the schema, prepares CQL
// statements against it and executes them on the storage engine, as
// Cassandra's cql3 statements, schema and db packages do on one node.
package db

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/cockroachdb/pebble/vfs"
)

// Options configure a DB.
type Options struct {
	// FS defaults to the OS file system.
	FS vfs.FS
	// Engine options; FS, Merger and TableOptions are set by the DB.
	Engine lsm.Options
	// SyncWrites fsyncs the commit log before acknowledging each write
	// (commitlog_sync: batch). Otherwise writes are synced periodically
	// (commitlog_sync: periodic, every Engine.WALSyncPeriod).
	SyncWrites bool

	ClusterName string
	DataCenter  string
	Rack        string
	// Address is the node's address in system.local and the address
	// clients connect to; Port its native transport port.
	Address net.IP
	Port    int
	// Cluster, if set, makes the database a node of a cluster.
	Cluster *ClusterOptions
	// Authenticator requires clients to log in (PasswordAuthenticator);
	// Authorizer checks permissions (CassandraAuthorizer).
	Authenticator, Authorizer bool
	// Now overrides the clock (tests).
	Now func() time.Time
	// StreamHook, in tests, runs before each range a resumable streaming
	// operation streams; an error interrupts it.
	StreamHook func(op, unit string) error
}

// DB is a single-node Cassandra database.
type DB struct {
	opts   Options
	fs     vfs.FS
	dir    string
	engine *lsm.Engine
	wo     *lsm.WriteOptions

	// ddlMu serializes schema changes; tableMu is held shared by every
	// statement touching storage and exclusively by DROP and TRUNCATE,
	// which must not run concurrently with reads of the dropped table.
	ddlMu   sync.Mutex
	tableMu sync.RWMutex
	schema  atomic.Pointer[Schema]
	local   localState

	lastTS atomic.Int64
	// installing is the metadata being installed (for engineTableOptions).
	installing atomic.Pointer[Schema]

	// hostID is the node's id, which owns its shard of counters.
	hostID [16]byte
	// listSeq and nodeSuffix make list element paths unique.
	listSeq    atomic.Uint32
	nodeSuffix [4]byte
	// counterMu serializes counter updates, which read the node's shard
	// before writing it (Cassandra's counter locks).
	counterMu sync.Mutex
	// vecs holds the open vector indexes.
	vecs vectorIndexes
	// traces keeps recent traced requests (system_traces).
	traces traces
	// prog records the progress of resumable streaming operations.
	progressOnce sync.Once
	prog         *streamProgress
	// streamHook, in tests, runs before each range a resumable operation
	// streams.
	streamHook func(op, unit string) error

	// store runs statements' reads and writes: localStorage, or the
	// cluster's coordinator.
	store storage
	proxy *proxy
}

// localState is persisted with the schema.
type localState struct {
	HostID string
	// NextStorageID is read from older schema files only.
	NextStorageID uint64 `json:",omitempty"`
	Bootstrapped  bool
	Tokens        []int64 `json:",omitempty"`
}

// schemaFile is the durable form of the schema.
type schemaFile struct {
	// Epoch numbers the metadata (TCM's epoch).
	Epoch         int64
	NextStorageID uint64
	// Local is this node's own state (not part of the metadata).
	Local     localState `json:",omitempty"`
	Keyspaces []*Keyspace
	Tables    []json.RawMessage
	Roles     []*Role         `json:",omitempty"`
	Nodes     []*cluster.Node `json:",omitempty"`
	CMS       []string        `json:",omitempty"`
	Log       []LogEntry      `json:",omitempty"`
}

const schemaFileName = "schema.json"

// Open opens or creates a database in dir.
func Open(dir string, opts Options) (*DB, error) {
	if opts.FS == nil {
		opts.FS = vfs.Default
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.ClusterName == "" {
		opts.ClusterName = "Test Cluster"
	}
	if opts.DataCenter == "" {
		opts.DataCenter = "datacenter1"
	}
	if opts.Rack == "" {
		opts.Rack = "rack1"
	}
	if opts.Address == nil {
		opts.Address = net.IPv4(127, 0, 0, 1)
	}
	if opts.Port == 0 {
		opts.Port = 9042
	}
	d := &DB{opts: opts, fs: opts.FS, dir: dir, wo: lsm.NoSync, streamHook: opts.StreamHook}
	d.store = localStorage{d}
	if opts.SyncWrites {
		d.wo = lsm.Sync
	}
	if err := d.fs.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if err := d.loadSchema(); err != nil {
		return nil, err
	}
	d.hostID, _ = types.ParseUUID(d.local.HostID)
	copy(d.nodeSuffix[:], d.hostID[12:])
	eo := opts.Engine
	eo.FS = d.fs
	eo.Merger = row.Merger{}
	eo.Now = opts.Now
	eo.TableOptions = d.engineTableOptions
	e, err := lsm.Open(d.fs.PathJoin(dir, "data"), &eo)
	if err != nil {
		return nil, err
	}
	d.engine = e
	// Create missing storage (a crash between creating a table's storage
	// and saving the schema), and drop orphans.
	for id := range d.schema.Load().storage {
		if _, err := e.Table(id); errors.Is(err, lsm.ErrUnknownTable) {
			if _, err := e.CreateTable(id); err != nil {
				e.Close()
				return nil, err
			}
		}
	}
	for id := lsm.TableID(1); uint64(id) < d.schema.Load().NextStorageID; id++ {
		if !d.schema.Load().storage[id] {
			if err := e.DropTable(id); err != nil && !errors.Is(err, lsm.ErrUnknownTable) {
				e.Close()
				return nil, err
			}
		}
	}
	if opts.Cluster != nil {
		if err := d.startCluster(opts.Cluster); err != nil {
			e.Close()
			return nil, err
		}
	}
	// In a cluster the default roles are a metadata change like any other.
	if err := d.defaultRoles(); err != nil {
		d.Close()
		return nil, err
	}
	if !d.local.Bootstrapped {
		d.local.Bootstrapped = true
		if err := d.saveSchema(d.schema.Load()); err != nil {
			e.Close()
			return nil, err
		}
	}
	return d, nil
}

// Close closes the database.
func (d *DB) Close() error {
	if d.proxy != nil {
		d.proxy.stop()
	}
	d.tableMu.Lock()
	defer d.tableMu.Unlock()
	return d.engine.Close()
}

// Engine returns the storage engine.
func (d *DB) Engine() *lsm.Engine { return d.engine }

func (d *DB) engineTableOptions(id lsm.TableID) lsm.TableOptions {
	for _, s := range []*Schema{d.installing.Load(), d.schema.Load()} {
		if s == nil {
			continue
		}
		if t, ok := s.byStorage[id]; ok {
			o, err := compactionOptions(t.Params.Compaction, t.Params.GCGraceSeconds)
			if err == nil {
				if len(t.Clustering) > 0 {
					o.RowFilter = newDeletionFilter
				}
				return o
			}
		}
	}
	return lsm.TableOptions{}
}

func (d *DB) loadSchema() error {
	f, err := d.fs.Open(d.fs.PathJoin(d.dir, schemaFileName))
	switch {
	case err == nil:
		b, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
		s, sf, err := parseSchemaFile(b)
		if err != nil {
			return err
		}
		d.local = sf.Local
		if s.NextStorageID == 0 { // an older schema file
			s.NextStorageID = max(1, sf.Local.NextStorageID)
		}
		d.schema.Store(s)
		return nil
	case oserrorIsNotExist(err):
		var id [16]byte
		_, _ = rand.Read(id[:])
		id[6] = id[6]&0x0f | 0x40
		id[8] = id[8]&0x3f | 0x80
		d.local = localState{HostID: types.FormatUUID(id[:])}
		s := &Schema{Keyspaces: map[string]*Keyspace{}, NextStorageID: 1, Roles: map[string]*Role{}, Nodes: map[string]*cluster.Node{}}
		addSystemKeyspaces(s)
		s.finish()
		d.schema.Store(s)
		return nil
	}
	return err
}

// parseSchemaFile decodes a schema file.
func parseSchemaFile(b []byte) (*Schema, *schemaFile, error) {
	var sf schemaFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return nil, nil, fmt.Errorf("db: corrupt schema file: %w", err)
	}
	s := &Schema{Keyspaces: map[string]*Keyspace{}, Epoch: sf.Epoch, NextStorageID: sf.NextStorageID,
		Roles: map[string]*Role{}, Nodes: map[string]*cluster.Node{}, CMS: sf.CMS, Log: sf.Log}
	for _, r := range sf.Roles {
		s.Roles[r.Name] = r
	}
	for _, n := range sf.Nodes {
		s.Nodes[n.HostID] = n
	}
	for _, k := range sf.Keyspaces {
		k.Tables = map[string]*Table{}
		if err := k.buildTypes(); err != nil {
			return nil, nil, fmt.Errorf("db: corrupt schema file: %w", err)
		}
		s.Keyspaces[k.Name] = k
	}
	for _, raw := range sf.Tables {
		var head struct{ Keyspace string }
		if err := json.Unmarshal(raw, &head); err != nil {
			return nil, nil, fmt.Errorf("db: corrupt schema file: %w", err)
		}
		k, ok := s.Keyspaces[head.Keyspace]
		if !ok {
			return nil, nil, fmt.Errorf("db: corrupt schema file: table without keyspace %s", head.Keyspace)
		}
		t, err := unmarshalTable(raw, k.resolve)
		if err != nil {
			return nil, nil, fmt.Errorf("db: corrupt schema file: %w", err)
		}
		k.Tables[t.Name] = t
	}
	addSystemKeyspaces(s)
	s.finish()
	return s, &sf, nil
}

func oserrorIsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }

// saveSchema writes the schema file atomically: a new file, synced, then
// renamed over the old one.
func (d *DB) saveSchema(s *Schema) error {
	sf := s.file()
	sf.Local = d.local
	b, err := json.MarshalIndent(sf, "", " ")
	if err != nil {
		return err
	}
	return d.writeFileAtomic(schemaFileName, b)
}

// writeFileAtomic replaces a file of the data directory durably: a synced
// temporary file renamed over it, then the directory synced.
func (d *DB) writeFileAtomic(name string, b []byte) error {
	tmp := d.fs.PathJoin(d.dir, name+".tmp")
	f, err := d.fs.Create(tmp, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := d.fs.Rename(tmp, d.fs.PathJoin(d.dir, name)); err != nil {
		return err
	}
	dir, err := d.fs.OpenDir(d.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// timestamp returns a new write timestamp in microseconds, strictly
// increasing (ClientState.getTimestamp).
func (d *DB) timestamp() int64 {
	now := d.opts.Now().UnixMicro()
	for {
		last := d.lastTS.Load()
		ts := max(now, last+1)
		if d.lastTS.CompareAndSwap(last, ts) {
			return ts
		}
	}
}

// file returns the durable form of the metadata.
func (s *Schema) file() *schemaFile {
	sf := &schemaFile{Epoch: s.Epoch, NextStorageID: s.NextStorageID, CMS: s.CMS, Log: s.Log}
	for _, r := range s.Roles {
		sf.Roles = append(sf.Roles, r)
	}
	sort.Slice(sf.Roles, func(i, j int) bool { return sf.Roles[i].Name < sf.Roles[j].Name })
	for _, n := range s.Nodes {
		sf.Nodes = append(sf.Nodes, n)
	}
	sort.Slice(sf.Nodes, func(i, j int) bool { return sf.Nodes[i].HostID < sf.Nodes[j].HostID })
	var names []string
	for n, k := range s.Keyspaces {
		if !k.virtual {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		k := s.Keyspaces[n]
		sf.Keyspaces = append(sf.Keyspaces, k)
		var tnames []string
		for tn := range k.Tables {
			tnames = append(tnames, tn)
		}
		sort.Strings(tnames)
		for _, tn := range tnames {
			b, _ := marshalTable(k.Tables[tn])
			sf.Tables = append(sf.Tables, b)
		}
	}
	return sf
}

// metadataBytes serializes the metadata for other nodes.
func metadataBytes(s *Schema) []byte {
	b, _ := json.Marshal(s.file())
	return b
}
