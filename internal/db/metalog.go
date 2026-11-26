package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/pbonard/cassandra-room/internal/cluster"
)

// The metadata log (TCM's LocalLog): in a cluster, the entry of each epoch
// is the change that epoch made, a metadataDelta, and every node applies
// the entries in epoch order. A node keeps its recent entries
// (metadataLogFile, at most maxLog) to serve peers that fall behind and to
// replay them at startup: an entry is made durable before the change is
// installed, so a node that crashes in between applies it again when it
// restarts.

const metadataLogFile = "metadata-log.json"

// errDiverged means an entry did not produce the metadata it was planned
// to: the node's metadata differs from the proposer's, and it must start
// again from a snapshot.
var errDiverged = errors.New("db: metadata diverged from the log")

// metadataDelta is the change of one epoch: the scalars of the metadata,
// the elements of its collections that were added, changed (their new
// encoding) or removed (null), the log entry describing the change, and a
// hash of the resulting metadata.
type metadataDelta struct {
	Epoch         int64
	NextStorageID uint64
	CMS           []string
	// Changes maps a collection (Keyspaces, Tables, Roles, Nodes) to the
	// changed elements by key.
	Changes map[string]map[string]json.RawMessage `json:",omitempty"`
	Log     LogEntry
	Hash    []byte
}

// rawMetadata is metadataBytes' form with the collections' elements left
// encoded.
type rawMetadata struct {
	Epoch         int64
	NextStorageID uint64
	Keyspaces     []json.RawMessage
	Tables        []json.RawMessage
	Roles         []json.RawMessage `json:",omitempty"`
	Nodes         []json.RawMessage `json:",omitempty"`
	CMS           []string          `json:",omitempty"`
	Log           []LogEntry        `json:",omitempty"`
}

func (r *rawMetadata) collections() map[string]*[]json.RawMessage {
	return map[string]*[]json.RawMessage{"Keyspaces": &r.Keyspaces, "Tables": &r.Tables, "Roles": &r.Roles, "Nodes": &r.Nodes}
}

// elementKey identifies an element of a collection.
func elementKey(collection string, b json.RawMessage) (string, error) {
	var k struct{ Name, Keyspace, HostID string }
	if err := json.Unmarshal(b, &k); err != nil {
		return "", err
	}
	switch collection {
	case "Tables":
		return k.Keyspace + "\x00" + k.Name, nil
	case "Nodes":
		return k.HostID, nil
	}
	return k.Name, nil
}

func keyed(collection string, list []json.RawMessage) (map[string]json.RawMessage, error) {
	m := make(map[string]json.RawMessage, len(list))
	for _, b := range list {
		k, err := elementKey(collection, b)
		if err != nil {
			return nil, err
		}
		m[k] = b
	}
	return m, nil
}

func metadataHash(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

// diffMetadata returns the delta from the metadata prev (metadataBytes)
// to next.
func diffMetadata(prev []byte, next *Schema) ([]byte, error) {
	nb := metadataBytes(next)
	var a, b rawMetadata
	if err := json.Unmarshal(prev, &a); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(nb, &b); err != nil {
		return nil, err
	}
	d := metadataDelta{Epoch: b.Epoch, NextStorageID: b.NextStorageID, CMS: b.CMS, Hash: metadataHash(nb)}
	if len(b.Log) > 0 {
		d.Log = b.Log[len(b.Log)-1]
	}
	ac, bc := a.collections(), b.collections()
	for name := range ac {
		am, err := keyed(name, *ac[name])
		if err != nil {
			return nil, err
		}
		bm, err := keyed(name, *bc[name])
		if err != nil {
			return nil, err
		}
		changes := map[string]json.RawMessage{}
		for k, v := range bm {
			if !bytes.Equal(am[k], v) {
				changes[k] = v
			}
		}
		for k := range am {
			if _, ok := bm[k]; !ok {
				changes[k] = json.RawMessage("null")
			}
		}
		if len(changes) > 0 {
			if d.Changes == nil {
				d.Changes = map[string]map[string]json.RawMessage{}
			}
			d.Changes[name] = changes
		}
	}
	return json.Marshal(d)
}

// applyDelta applies an entry's delta to the metadata cur.
func applyDelta(cur *Schema, value []byte) (*Schema, error) {
	var d metadataDelta
	if err := json.Unmarshal(value, &d); err != nil {
		return nil, err
	}
	if d.Epoch != cur.Epoch+1 {
		return nil, fmt.Errorf("db: metadata entry for epoch %d applied at epoch %d", d.Epoch, cur.Epoch)
	}
	var r rawMetadata
	if err := json.Unmarshal(metadataBytes(cur), &r); err != nil {
		return nil, err
	}
	for name, list := range r.collections() {
		changes := d.Changes[name]
		if len(changes) == 0 {
			continue
		}
		m, err := keyed(name, *list)
		if err != nil {
			return nil, err
		}
		for k, v := range changes {
			if bytes.Equal(v, []byte("null")) {
				delete(m, k)
			} else {
				m[k] = v
			}
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]json.RawMessage, 0, len(keys))
		for _, k := range keys {
			out = append(out, m[k])
		}
		*list = out
	}
	r.Epoch, r.NextStorageID, r.CMS = d.Epoch, d.NextStorageID, d.CMS
	r.Log = append(r.Log, d.Log)
	if len(r.Log) > maxLog {
		r.Log = r.Log[len(r.Log)-maxLog:]
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	next, _, err := parseSchemaFile(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(metadataHash(metadataBytes(next)), d.Hash) {
		return nil, fmt.Errorf("%w at epoch %d", errDiverged, d.Epoch)
	}
	return next, nil
}

// metaStore is the node's metadata as the metadata service sees it
// (cluster.Store).
type metaStore struct{ p *proxy }

func (m metaStore) Current() (int64, []byte, []string) {
	s := m.p.d.schema.Load()
	return s.Epoch, metadataBytes(s), m.p.cmsMembers()
}

func (m metaStore) Contains(meta []byte, id string) (bool, error) {
	_, ok, err := metadataInfo(meta, id)
	return ok, err
}

func (m metaStore) Append(e cluster.Entry) error {
	d := m.p.d
	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()
	return d.appendLocked(e)
}

func (m metaStore) Since(epoch int64) ([]cluster.Entry, bool) {
	d := m.p.d
	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()
	cur := d.schema.Load().Epoch
	if epoch >= cur {
		return nil, true
	}
	i := slices.IndexFunc(d.metaLog, func(e cluster.Entry) bool { return e.Epoch == epoch+1 })
	if i < 0 || d.metaLog[len(d.metaLog)-1].Epoch != cur {
		return nil, false
	}
	return slices.Clone(d.metaLog[i:]), true
}

func (m metaStore) Snapshot(b []byte) error {
	d := m.p.d
	next, _, err := parseSchemaFile(b)
	if err != nil {
		return err
	}
	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()
	if next.Epoch <= d.schema.Load().Epoch {
		return nil
	}
	// The log restarts after the snapshot.
	d.metaLog = nil
	if err := d.saveMetadataLog(); err != nil {
		return err
	}
	return d.installLocked(next)
}

// appendLocked applies the entry of the next epoch (holding ddlMu): it is
// logged durably, then installed.
func (d *DB) appendLocked(e cluster.Entry) error {
	cur := d.schema.Load()
	switch {
	case e.Epoch <= cur.Epoch:
		return nil
	case e.Epoch > cur.Epoch+1:
		return cluster.ErrGap
	}
	next, err := applyDelta(cur, e.Value)
	if err != nil {
		return err
	}
	// The log in memory changes only once saved: a failed save leaves it
	// as it was, and the entry can be appended again.
	prev := d.metaLog
	d.metaLog = append(slices.Clip(prev), e)
	if len(d.metaLog) > maxLog {
		d.metaLog = slices.Clone(d.metaLog[len(d.metaLog)-maxLog:])
	}
	if err := d.saveMetadataLog(); err != nil {
		d.metaLog = prev
		return err
	}
	return d.installLocked(next)
}

// entryAt returns the logged entry of an epoch.
func (d *DB) entryAt(epoch int64) (cluster.Entry, bool) {
	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()
	for _, e := range d.metaLog {
		if e.Epoch == epoch {
			return e, true
		}
	}
	return cluster.Entry{}, false
}

func (d *DB) saveMetadataLog() error {
	b, err := json.Marshal(d.metaLog)
	if err != nil {
		return err
	}
	return d.writeFileAtomic(metadataLogFile, b)
}

// replayMetadataLog loads the node's log and applies the entries past the
// installed metadata (logged before a crash stopped their installation).
func (d *DB) replayMetadataLog() error {
	b, err := d.readFile(metadataLogFile)
	if err != nil || len(b) == 0 {
		return err
	}
	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()
	var entries []cluster.Entry
	if err := json.Unmarshal(b, &entries); err != nil {
		return fmt.Errorf("db: corrupt metadata log: %w", err)
	}
	cur := d.schema.Load().Epoch
	for _, e := range entries {
		if e.Epoch <= cur {
			d.metaLog = append(d.metaLog, e)
		}
	}
	for _, e := range entries {
		if e.Epoch <= cur {
			continue
		}
		if err := d.appendLocked(e); err != nil {
			return fmt.Errorf("db: replaying the metadata log at epoch %d: %w", e.Epoch, err)
		}
	}
	return nil
}
