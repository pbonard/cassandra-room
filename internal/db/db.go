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
	"sync"
	"sync/atomic"
	"time"

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
	// Now overrides the clock (tests).
	Now func() time.Time
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
	// pending is a table whose storage is being created, before it is in
	// the schema (for engineTableOptions).
	pending atomic.Pointer[Table]

	lastTS atomic.Int64
}

// localState is persisted with the schema.
type localState struct {
	HostID        string
	NextStorageID uint64
	Bootstrapped  bool
}

// schemaFile is the durable form of the schema.
type schemaFile struct {
	Local     localState
	Keyspaces []*Keyspace
	Tables    []json.RawMessage
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
	d := &DB{opts: opts, fs: opts.FS, dir: dir, wo: lsm.NoSync}
	if opts.SyncWrites {
		d.wo = lsm.Sync
	}
	if err := d.fs.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if err := d.loadSchema(); err != nil {
		return nil, err
	}
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
	for _, t := range d.schema.Load().byStorage {
		if _, err := e.Table(t.Storage); errors.Is(err, lsm.ErrUnknownTable) {
			if _, err := e.CreateTable(t.Storage); err != nil {
				e.Close()
				return nil, err
			}
		}
	}
	for id := lsm.TableID(1); uint64(id) < d.local.NextStorageID; id++ {
		if _, ok := d.schema.Load().byStorage[id]; !ok {
			if err := e.DropTable(id); err != nil && !errors.Is(err, lsm.ErrUnknownTable) {
				e.Close()
				return nil, err
			}
		}
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
	d.tableMu.Lock()
	defer d.tableMu.Unlock()
	return d.engine.Close()
}

// Engine returns the storage engine.
func (d *DB) Engine() *lsm.Engine { return d.engine }

func (d *DB) engineTableOptions(id lsm.TableID) lsm.TableOptions {
	if t := d.pending.Load(); t != nil && t.Storage == id {
		o, err := compactionOptions(t.Params.Compaction, t.Params.GCGraceSeconds)
		if err == nil {
			return o
		}
	}
	if s := d.schema.Load(); s != nil {
		if t, ok := s.byStorage[id]; ok {
			o, err := compactionOptions(t.Params.Compaction, t.Params.GCGraceSeconds)
			if err == nil {
				return o
			}
		}
	}
	return lsm.TableOptions{}
}

func (d *DB) loadSchema() error {
	s := &Schema{Keyspaces: map[string]*Keyspace{}}
	f, err := d.fs.Open(d.fs.PathJoin(d.dir, schemaFileName))
	switch {
	case err == nil:
		b, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
		var sf schemaFile
		if err := json.Unmarshal(b, &sf); err != nil {
			return fmt.Errorf("db: corrupt schema file: %w", err)
		}
		d.local = sf.Local
		for _, k := range sf.Keyspaces {
			k.Tables = map[string]*Table{}
			s.Keyspaces[k.Name] = k
		}
		for _, raw := range sf.Tables {
			t, err := unmarshalTable(raw)
			if err != nil {
				return fmt.Errorf("db: corrupt schema file: %w", err)
			}
			k, ok := s.Keyspaces[t.Keyspace]
			if !ok {
				return fmt.Errorf("db: corrupt schema file: table %s.%s without keyspace", t.Keyspace, t.Name)
			}
			k.Tables[t.Name] = t
		}
	case oserrorIsNotExist(err):
		var id [16]byte
		_, _ = rand.Read(id[:])
		id[6] = id[6]&0x0f | 0x40
		id[8] = id[8]&0x3f | 0x80
		d.local = localState{HostID: types.FormatUUID(id[:]), NextStorageID: 1}
	default:
		return err
	}
	addSystemKeyspaces(s)
	s.finish()
	d.schema.Store(s)
	return nil
}

func oserrorIsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }

// saveSchema writes the schema file atomically: a new file, synced, then
// renamed over the old one.
func (d *DB) saveSchema(s *Schema) error {
	sf := schemaFile{Local: d.local}
	for _, k := range s.Keyspaces {
		if k.virtual {
			continue
		}
		sf.Keyspaces = append(sf.Keyspaces, k)
		for _, t := range k.Tables {
			b, err := marshalTable(t)
			if err != nil {
				return err
			}
			sf.Tables = append(sf.Tables, b)
		}
	}
	b, err := json.MarshalIndent(sf, "", " ")
	if err != nil {
		return err
	}
	tmp := d.fs.PathJoin(d.dir, schemaFileName+".tmp")
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
	if err := d.fs.Rename(tmp, d.fs.PathJoin(d.dir, schemaFileName)); err != nil {
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
