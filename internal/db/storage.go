package db

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// mutation is a set of row fragment writes, applied atomically on each
// replica (Mutation). Each write carries the token of the partition it
// belongs to, which places it on the ring: for index entries, the token
// of the indexed row.
type mutation struct {
	trace  *Trace
	writes []write
}

type write struct {
	table      lsm.TableID
	key, value []byte
	token      int64
}

func (m *mutation) put(table lsm.TableID, key, value []byte, token int64) {
	m.writes = append(m.writes, write{table: table, key: key, value: append([]byte(nil), value...), token: token})
}

// storage executes reads and writes for statements: on this node, or in a
// cluster on each partition's replicas at a consistency level
// (StorageProxy).
type storage interface {
	apply(m *mutation, cl primitive.ConsistencyLevel) error
	source(t *Table, storageID lsm.TableID, cl primitive.ConsistencyLevel) (source, error)
}

// localStorage runs on this node's engine.
type localStorage struct{ d *DB }

func (l localStorage) apply(m *mutation, _ primitive.ConsistencyLevel) error {
	m.trace.event("Applying %d writes locally (commit log, memtable)", len(m.writes))
	return l.d.applyLocal(m.writes)
}

func (l localStorage) source(t *Table, id lsm.TableID, _ primitive.ConsistencyLevel) (source, error) {
	lt, err := l.d.engine.Table(id)
	if err != nil {
		return nil, invalidf("table %s.%s was dropped", t.Keyspace, t.Name)
	}
	return engineSource{lt}, nil
}

// applyLocal applies writes to this node's engine in one batch.
func (d *DB) applyLocal(writes []write) error {
	if len(writes) == 0 {
		return nil
	}
	b := lsm.NewBatch()
	for _, w := range writes {
		b.Put(w.table, w.key, w.value)
	}
	d.tableMu.RLock()
	err := d.engine.Apply(b, d.wo)
	d.tableMu.RUnlock()
	if err != nil {
		if strings.Contains(err.Error(), lsm.ErrUnknownTable.Error()) {
			return invalidf("a table was dropped while it was written")
		}
		return err
	}
	d.updateVectorIndexes(writes)
	return nil
}

// applyCounterDeltas turns counter deltas (8-byte cell values) into this
// node's counter shards, reading its current shard locally, and writes the
// result at cl (the leader's part of CounterMutation.apply). Deltas on the
// same counter in one mutation build on each other.
func (d *DB) applyCounterDeltas(ws []write, cl primitive.ConsistencyLevel) error {
	d.counterMu.Lock()
	defer d.counterMu.Unlock()
	type shard struct{ clock, count int64 }
	cur := map[string]shard{}
	out := make([]write, 0, len(ws))
	for _, w := range ws {
		r, err := row.Decode(w.value)
		if err != nil {
			return err
		}
		changed := false
		for i := range r.Cells {
			c := &r.Cells[i]
			if !c.Counter || len(c.Value) != 8 {
				continue
			}
			id := string(w.key) + "/" + strconv.FormatUint(uint64(c.Column), 10) + "/" + string(c.Path)
			sh, ok := cur[id]
			if !ok {
				if lt, err := d.engine.Table(w.table); err == nil {
					if v, err := lt.Get(w.key); err == nil {
						if old, err := row.Decode(v); err == nil {
							for _, oc := range old.Cells {
								if oc.Column == c.Column && oc.Counter && bytes.Equal(oc.Path, c.Path) {
									sh.clock, sh.count, _ = row.CounterShard(oc.Value, d.hostID)
								}
							}
						}
					}
				}
			}
			sh.clock++
			sh.count += int64(binary.BigEndian.Uint64(c.Value))
			cur[id] = sh
			c.Value = row.CounterContext(d.hostID, sh.clock, sh.count)
			changed = true
		}
		if changed {
			w.value = row.Append(nil, r)
		}
		out = append(out, w)
	}
	return d.store.apply(&mutation{writes: out}, cl)
}
