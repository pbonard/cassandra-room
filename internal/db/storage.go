package db

import (
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// mutation is a set of row fragment writes, applied atomically on each
// replica (Mutation). Each write carries the token of the partition it
// belongs to, which places it on the ring: for index entries, the token
// of the indexed row.
type mutation struct {
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
	defer d.tableMu.RUnlock()
	if err := d.engine.Apply(b, d.wo); err != nil {
		if strings.Contains(err.Error(), lsm.ErrUnknownTable.Error()) {
			return invalidf("a table was dropped while it was written")
		}
		return err
	}
	return nil
}
