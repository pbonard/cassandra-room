package db

import (
	"bytes"
	"encoding/json"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Schema change events (the native protocol's SCHEMA_CHANGE events):
// every node, as it installs a new metadata version, tells the clients
// registered on it what changed, so that drivers caching the schema
// (the Java driver, gocql v2) refresh their metadata.

// OnSchemaChange sets the function told the schema changes of each
// metadata version this node installs (nil: none). It runs outside the
// metadata lock (drivers only refresh their schema on an event, so the
// order of two versions' events does not matter).
func (d *DB) OnSchemaChange(f func([]*message.SchemaChangeEvent)) {
	d.schemaEventsMu.Lock()
	d.schemaEvents = f
	d.schemaEventsMu.Unlock()
}

// notifySchemaChanges tells the schema changes from old to next.
func (d *DB) notifySchemaChanges(old, next *Schema) {
	d.schemaEventsMu.Lock()
	f := d.schemaEvents
	d.schemaEventsMu.Unlock()
	if f == nil || old == nil {
		return
	}
	if evs := schemaChanges(old, next); len(evs) > 0 {
		go f(evs)
	}
}

// schemaChanges lists the user keyspaces, tables and types created,
// changed or dropped from old to next. A dropped keyspace is one event
// (as Cassandra: its tables and types go with it).
func schemaChanges(old, next *Schema) []*message.SchemaChangeEvent {
	var out []*message.SchemaChangeEvent
	ev := func(t primitive.SchemaChangeType, target primitive.SchemaChangeTarget, ks, obj string) {
		out = append(out, &message.SchemaChangeEvent{ChangeType: t, Target: target, Keyspace: ks, Object: obj})
	}
	for name, nk := range next.Keyspaces {
		if nk.virtual {
			continue
		}
		ok := old.Keyspaces[name]
		if ok == nil {
			ev(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetKeyspace, name, "")
			for tn := range nk.Tables {
				ev(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetTable, name, tn)
			}
			for _, ut := range nk.UserTypes {
				ev(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetType, name, ut.Name)
			}
			continue
		}
		if !sameJSON(ok.Replication, nk.Replication) || ok.DurableWrites != nk.DurableWrites {
			ev(primitive.SchemaChangeTypeUpdated, primitive.SchemaChangeTargetKeyspace, name, "")
		}
		for tn, nt := range nk.Tables {
			switch ot := ok.Tables[tn]; {
			case ot == nil:
				ev(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetTable, name, tn)
			case ot != nt && !sameJSON(ot, nt):
				ev(primitive.SchemaChangeTypeUpdated, primitive.SchemaChangeTargetTable, name, tn)
			}
		}
		for tn := range ok.Tables {
			if nk.Tables[tn] == nil {
				ev(primitive.SchemaChangeTypeDropped, primitive.SchemaChangeTargetTable, name, tn)
			}
		}
		oldTypes := map[string]UserType{}
		for _, ut := range ok.UserTypes {
			oldTypes[ut.Name] = ut
		}
		for _, ut := range nk.UserTypes {
			o, found := oldTypes[ut.Name]
			switch {
			case !found:
				ev(primitive.SchemaChangeTypeCreated, primitive.SchemaChangeTargetType, name, ut.Name)
			case !sameJSON(o, ut):
				ev(primitive.SchemaChangeTypeUpdated, primitive.SchemaChangeTargetType, name, ut.Name)
			}
			delete(oldTypes, ut.Name)
		}
		for tn := range oldTypes {
			ev(primitive.SchemaChangeTypeDropped, primitive.SchemaChangeTargetType, name, tn)
		}
	}
	for name, ok := range old.Keyspaces {
		if !ok.virtual && next.Keyspaces[name] == nil {
			ev(primitive.SchemaChangeTypeDropped, primitive.SchemaChangeTargetKeyspace, name, "")
		}
	}
	return out
}

// sameJSON compares two definitions by their stored form, an empty list
// the same as none (a table altered here holds one where the same table
// read back holds none).
func sameJSON(a, b any) bool {
	ja, err1 := json.Marshal(a)
	jb, err2 := json.Marshal(b)
	norm := func(j []byte) []byte { return bytes.ReplaceAll(j, []byte(":[]"), []byte(":null")) }
	return err1 == nil && err2 == nil && bytes.Equal(norm(ja), norm(jb))
}
