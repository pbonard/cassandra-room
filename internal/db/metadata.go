package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/pbonard/cassandra-room/internal/storage/lsm"
)

// Cluster metadata changes (Transactional Cluster Metadata, CEP-21): every
// change to the schema, the roles or the node directory is a
// transformation planned on a copy of the current metadata. On a single
// node the result is installed directly; in a cluster it is committed as
// the next epoch through the Cluster Metadata Service, which installs it
// on every node in epoch order, as an entry holding only the change (see
// metalog.go). Installing metadata is the only place with side effects: storage is created for new tables and indexes,
// storage of dropped ones is removed, new indexes are built.

// maxLog is how many changes the metadata log keeps.
const maxLog = 256

// errNoChange stops a commit whose plan changed nothing.
var errNoChange = errors.New("db: no change")

// change plans and applies a metadata change of a kind (a TCM
// transformation's name). plan modifies next and returns the statement's
// result; changed false makes it a no-op.
func (d *DB) change(kind string, plan func(next *Schema) (message.Message, bool, error)) (message.Message, error) {
	log := func(next *Schema, id string, res message.Message) {
		detail := ""
		if sc, ok := res.(*message.SchemaChangeResult); ok {
			detail = fmt.Sprintf("%s %s %s", sc.ChangeType, sc.Target, sc.Keyspace)
			if sc.Object != "" {
				detail += "." + sc.Object
			}
		}
		if next.logDetail != "" {
			detail, next.logDetail = next.logDetail, ""
		}
		next.Log = append(next.Log, LogEntry{Epoch: next.Epoch, ID: id, Kind: kind, Detail: detail, Time: time.Now().UnixMilli()})
		if len(next.Log) > maxLog {
			next.Log = slices.Clone(next.Log[len(next.Log)-maxLog:])
		}
	}
	if d.proxy == nil {
		d.ddlMu.Lock()
		defer d.ddlMu.Unlock()
		next := d.schema.Load().clone()
		res, changed, err := plan(next)
		if err != nil || !changed {
			return res, err
		}
		next.Epoch++
		log(next, "", res)
		next.finish()
		return res, d.installLocked(next)
	}
	var res message.Message
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	epoch, err := d.proxy.cms.Commit(ctx, func(cur []byte, id string) ([]byte, error) {
		next, _, err := parseSchemaFile(cur)
		if err != nil {
			return nil, err
		}
		r, changed, err := plan(next)
		if err != nil {
			return nil, err
		}
		res = r
		if !changed {
			return nil, errNoChange
		}
		next.Epoch++
		log(next, id, r)
		next.finish()
		return diffMetadata(cur, next)
	})
	if errors.Is(err, errNoChange) {
		return res, nil
	}
	if err != nil {
		return nil, err
	}
	d.proxy.disseminate(epoch)
	return res, nil
}

// installLocked makes next the current metadata (holding ddlMu).
func (d *DB) installLocked(next *Schema) error {
	old := d.schema.Load()
	d.installing.Store(next)
	for id := range next.storage {
		if _, err := d.engine.Table(id); errors.Is(err, lsm.ErrUnknownTable) {
			if _, err := d.engine.CreateTable(id); err != nil {
				d.installing.Store(nil)
				return err
			}
		}
	}
	d.installing.Store(nil)
	if err := d.saveSchema(next); err != nil {
		return err
	}
	d.setSchema(next)
	// Options changed by ALTER TABLE take effect now.
	for id, t := range next.byStorage {
		if ot := old.byStorage[id]; ot != nil && !reflect.DeepEqual(ot.Params, t.Params) {
			if lt, err := d.engine.Table(id); err == nil {
				lt.SetOptions(d.engineTableOptions(id))
			}
		}
	}
	var gone []lsm.TableID
	for id := range old.storage {
		if !next.storage[id] {
			gone = append(gone, id)
		}
	}
	if err := d.dropStorage(gone...); err != nil {
		return err
	}
	if d.proxy != nil {
		d.proxy.metadataChanged(next)
	}
	return nil
}

// metadataInfo is the CMS's view of a snapshot: its epoch, and whether it
// contains the change with entry id.
func metadataInfo(b []byte, id string) (int64, bool, error) {
	s, _, err := parseSchemaFile(b)
	if err != nil {
		return 0, false, err
	}
	return s.Epoch, slices.ContainsFunc(s.Log, func(e LogEntry) bool { return e.ID == id }), nil
}
