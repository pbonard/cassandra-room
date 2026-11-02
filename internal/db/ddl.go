package db

import (
	"errors"
	"maps"
	"slices"
	"sort"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// ddlStmt is a schema statement (AlterSchemaStatement). It is checked
// against the schema when executed, not when prepared, as Cassandra does.
type ddlStmt struct {
	raw     cql.Statement
	session string
}

func (d *DB) prepareDDL(session string, st cql.Statement) (*ddlStmt, error) {
	switch s := st.(type) {
	case *cql.CreateTable:
		if _, err := keyspaceOf(s.Keyspace, session); err != nil {
			return nil, err
		}
	case *cql.AlterTable:
		if _, err := keyspaceOf(s.Keyspace, session); err != nil {
			return nil, err
		}
	case *cql.DropTable:
		if _, err := keyspaceOf(s.Keyspace, session); err != nil {
			return nil, err
		}
	case *cql.Truncate:
		if _, err := keyspaceOf(s.Keyspace, session); err != nil {
			return nil, err
		}
	case *cql.CreateKeyspace, *cql.AlterKeyspace, *cql.DropKeyspace:
	default:
		return nil, invalidf("unsupported statement %T", st)
	}
	return &ddlStmt{raw: st, session: session}, nil
}

const keyspaceNameLength = 48

func schemaChange(change primitive.SchemaChangeType, ks, table string) *message.SchemaChangeResult {
	r := &message.SchemaChangeResult{ChangeType: change, Target: primitive.SchemaChangeTargetKeyspace, Keyspace: ks}
	if table != "" {
		r.Target, r.Object = primitive.SchemaChangeTargetTable, table
	}
	return r
}

func (s *ddlStmt) execute(e *execCtx) (message.Message, error) {
	d := e.d
	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()
	cur := d.schema.Load()
	next := cur.clone()
	var res message.Message
	var after func() error // engine work once the schema is saved
	switch st := s.raw.(type) {
	case *cql.CreateKeyspace:
		if !validName(st.Keyspace) || len(st.Keyspace) > keyspaceNameLength {
			return nil, invalidf("Keyspace name must not be empty, more than %d characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", keyspaceNameLength, st.Keyspace)
		}
		if _, ok := cur.Keyspaces[st.Keyspace]; ok {
			if st.IfNotExists {
				return &message.VoidResult{}, nil
			}
			return nil, alreadyExists(st.Keyspace, "")
		}
		repl, durable, err := replication(st.Properties)
		if err != nil {
			return nil, err
		}
		next.Keyspaces[st.Keyspace] = &Keyspace{Name: st.Keyspace, Replication: repl, DurableWrites: durable, Tables: map[string]*Table{}}
		res = schemaChange(primitive.SchemaChangeTypeCreated, st.Keyspace, "")
	case *cql.AlterKeyspace:
		k, ok := cur.Keyspaces[st.Keyspace]
		if !ok {
			if st.IfExists {
				return &message.VoidResult{}, nil
			}
			return nil, invalidf("Keyspace '%s' doesn't exist", st.Keyspace)
		}
		if k.virtual {
			return nil, unauthorizedf("%s keyspace is not user-modifiable", st.Keyspace)
		}
		nk := *k
		for _, p := range st.Properties {
			switch p.Name {
			case "replication":
				repl, _, err := replication([]cql.Property{p})
				if err != nil {
					return nil, err
				}
				nk.Replication = repl
			case "durable_writes":
				v, err := stringProperty(p)
				if err != nil {
					return nil, err
				}
				nk.DurableWrites = v != "false"
			default:
				return nil, syntaxf("Unknown property '%s'", p.Name)
			}
		}
		next.Keyspaces[st.Keyspace] = &nk
		res = schemaChange(primitive.SchemaChangeTypeUpdated, st.Keyspace, "")
	case *cql.DropKeyspace:
		k, ok := cur.Keyspaces[st.Keyspace]
		if !ok {
			if st.IfExists {
				return &message.VoidResult{}, nil
			}
			return nil, invalidf("Keyspace '%s' doesn't exist", st.Keyspace)
		}
		if k.virtual {
			return nil, unauthorizedf("%s keyspace is not user-modifiable", st.Keyspace)
		}
		delete(next.Keyspaces, st.Keyspace)
		var ids []lsm.TableID
		for _, t := range k.Tables {
			ids = append(ids, t.Storage)
		}
		after = func() error { return d.dropStorage(ids...) }
		res = schemaChange(primitive.SchemaChangeTypeDropped, st.Keyspace, "")
	case *cql.CreateTable:
		ksName, _ := keyspaceOf(st.Keyspace, s.session)
		k, ok := cur.Keyspaces[ksName]
		if !ok {
			return nil, invalidf("Keyspace '%s' doesn't exist", ksName)
		}
		if k.virtual {
			return nil, unauthorizedf("%s keyspace is not user-modifiable", ksName)
		}
		if _, ok := k.Tables[st.Table]; ok {
			if st.IfNotExists {
				return &message.VoidResult{}, nil
			}
			return nil, alreadyExists(ksName, st.Table)
		}
		t, err := newTable(ksName, st)
		if err != nil {
			return nil, err
		}
		copy(t.ID[:], newTimeUUID(e.now))
		t.Storage = lsm.TableID(d.local.NextStorageID)
		d.local.NextStorageID++
		d.pending.Store(t)
		defer d.pending.Store(nil)
		if _, err := d.engine.CreateTable(t.Storage); err != nil {
			if !errors.Is(err, lsm.ErrTableExists) {
				return nil, err
			}
			// Left by a crash before the schema was saved: start empty.
			if err := d.dropStorage(t.Storage); err != nil {
				return nil, err
			}
			if _, err := d.engine.CreateTable(t.Storage); err != nil {
				return nil, err
			}
		}
		next.Keyspaces[ksName].Tables[st.Table] = t
		res = schemaChange(primitive.SchemaChangeTypeCreated, ksName, st.Table)
	case *cql.AlterTable:
		ksName, _ := keyspaceOf(st.Keyspace, s.session)
		t, err := cur.table(ksName, st.Table)
		if err != nil {
			if st.IfExists {
				return &message.VoidResult{}, nil
			}
			return nil, err
		}
		if t.virtual != nil {
			return nil, unauthorizedf("%s keyspace is not user-modifiable", ksName)
		}
		nt, err := alterTable(t, st)
		if err != nil {
			return nil, err
		}
		next.Keyspaces[ksName].Tables[st.Table] = nt
		res = schemaChange(primitive.SchemaChangeTypeUpdated, ksName, st.Table)
	case *cql.DropTable:
		ksName, _ := keyspaceOf(st.Keyspace, s.session)
		t, err := cur.table(ksName, st.Table)
		if err != nil {
			if st.IfExists {
				return &message.VoidResult{}, nil
			}
			return nil, err
		}
		if t.virtual != nil {
			return nil, unauthorizedf("%s keyspace is not user-modifiable", ksName)
		}
		delete(next.Keyspaces[ksName].Tables, st.Table)
		after = func() error { return d.dropStorage(t.Storage) }
		res = schemaChange(primitive.SchemaChangeTypeDropped, ksName, st.Table)
	case *cql.Truncate:
		ksName, _ := keyspaceOf(st.Keyspace, s.session)
		t, err := cur.table(ksName, st.Table)
		if err != nil {
			return nil, err
		}
		if t.virtual != nil {
			return nil, invalidf("Cannot truncate virtual table %s.%s", ksName, st.Table)
		}
		// Truncation switches the table to new, empty storage.
		nt := *t
		nt.Storage = lsm.TableID(d.local.NextStorageID)
		d.local.NextStorageID++
		d.pending.Store(&nt)
		defer d.pending.Store(nil)
		if _, err := d.engine.CreateTable(nt.Storage); err != nil {
			return nil, err
		}
		nt.index()
		next.Keyspaces[ksName].Tables[st.Table] = &nt
		after = func() error { return d.dropStorage(t.Storage) }
		res = &message.VoidResult{}
	}
	next.finish()
	if err := d.saveSchema(next); err != nil {
		return nil, err
	}
	d.schema.Store(next)
	if after != nil {
		if err := after(); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func unauthorizedf(format string, args ...any) error {
	e := invalidf(format, args...).(*requestError)
	e.kind = errUnauthorized
	return e
}

// dropStorage drops tables' storage once no statement uses it.
func (d *DB) dropStorage(ids ...lsm.TableID) error {
	d.tableMu.Lock()
	defer d.tableMu.Unlock()
	for _, id := range ids {
		if err := d.engine.DropTable(id); err != nil && !errors.Is(err, lsm.ErrUnknownTable) {
			return err
		}
	}
	return nil
}

// alterTable applies ALTER TABLE to a copy of t (AlterTableStatement).
func alterTable(t *Table, st *cql.AlterTable) (*Table, error) {
	nt := *t
	nt.PartitionKey = cloneColumns(t.PartitionKey)
	nt.Clustering = cloneColumns(t.Clustering)
	nt.Columns = cloneColumns(t.Columns)
	nt.Params.Compaction = maps.Clone(t.Params.Compaction)
	nt.Params.Compression = maps.Clone(t.Params.Compression)
	nt.Params.Caching = maps.Clone(t.Params.Caching)
	nt.index()
	for _, def := range st.Add {
		if nt.byName[def.Name] != nil {
			return nil, invalidf("Column with name '%s' already exists", def.Name)
		}
		typ, err := types.Parse(def.Type)
		if err != nil {
			return nil, invalidf("%v", err)
		}
		if err := supportedColumnType(typ, def.Name); err != nil {
			return nil, err
		}
		kind := Regular
		if def.Static {
			if len(nt.Clustering) == 0 {
				return nil, invalidf("Static columns are only useful (and thus allowed) if the table has at least one clustering column")
			}
			kind = Static
		}
		nt.Columns = append(nt.Columns, &Column{Name: def.Name, Type: typ, Kind: kind, Position: -1, ID: nt.NextColumnID})
		nt.NextColumnID++
		nt.index()
	}
	for _, name := range st.Drop {
		c := nt.byName[name]
		if c == nil {
			return nil, invalidf("Column %s was not found in table %s", name, t.Name)
		}
		if c.Kind == PartitionKey || c.Kind == Clustering {
			return nil, invalidf("Cannot drop PRIMARY KEY column %s", name)
		}
		// Dropped columns' cells are ignored from now on: a re-added column
		// gets a new id.
		nt.Columns = slices.DeleteFunc(nt.Columns, func(x *Column) bool { return x.Name == name })
		nt.index()
	}
	for _, rn := range st.Rename {
		c := nt.byName[rn[0]]
		if c == nil {
			return nil, invalidf("Column %s was not found in table %s", rn[0], t.Name)
		}
		if c.Kind != PartitionKey && c.Kind != Clustering {
			return nil, invalidf("Cannot rename non PRIMARY KEY column %s", rn[0])
		}
		if nt.byName[rn[1]] != nil {
			return nil, invalidf("Cannot rename column %s to %s in table '%s'; another column with that name already exists", rn[0], rn[1], t.Name)
		}
		c.Name = rn[1]
		nt.index()
	}
	if len(st.Properties) > 0 {
		if err := applyTableProperties(&nt.Params, st.Properties); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(nt.Columns, func(i, j int) bool { return nt.Columns[i].Name < nt.Columns[j].Name })
	nt.index()
	return &nt, nil
}

func cloneColumns(cs []*Column) []*Column {
	out := make([]*Column, len(cs))
	for i, c := range cs {
		cc := *c
		out[i] = &cc
	}
	return out
}
