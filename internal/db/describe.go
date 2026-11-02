package db

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// describeStmt is DESCRIBE (DescribeStatement): cqlsh's DESCRIBE commands
// are executed by the server since Cassandra 4.0.
type describeStmt struct {
	raw     *cql.Describe
	session string
}

func describeColumns(withStatement bool) []*message.ColumnMetadata {
	names := []string{"keyspace_name", "type", "name"}
	if withStatement {
		names = append(names, "create_statement")
	}
	out := make([]*message.ColumnMetadata, len(names))
	for i, n := range names {
		out[i] = &message.ColumnMetadata{Keyspace: "system", Table: "describe", Name: n, Index: int32(i), Type: types.TextType.DataType()}
	}
	return out
}

func (d *describeStmt) metadata() []*message.ColumnMetadata {
	switch d.raw.Kind {
	case "KEYSPACES", "TABLES", "TYPES", "FUNCTIONS", "AGGREGATES":
		return describeColumns(false)
	case "CLUSTER":
		out := make([]*message.ColumnMetadata, 3)
		for i, n := range []string{"cluster", "partitioner", "snitch"} {
			out[i] = &message.ColumnMetadata{Keyspace: "system", Table: "describe", Name: n, Index: int32(i), Type: types.TextType.DataType()}
		}
		return out
	}
	return describeColumns(true)
}

func (d *describeStmt) execute(e *execCtx) (message.Message, error) {
	s := e.d.schema.Load()
	r := d.raw
	md := &message.RowsMetadata{Columns: d.metadata()}
	md.ColumnCount = int32(len(md.Columns))
	var rows message.RowSet
	keyspaces := func() []*Keyspace {
		var out []*Keyspace
		for _, k := range s.Keyspaces {
			out = append(out, k)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out
	}
	tables := func(k *Keyspace) []*Table {
		var out []*Table
		for _, t := range k.Tables {
			out = append(out, t)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out
	}
	list := func(ks, typ, name string) {
		rows = append(rows, message.Row{[]byte(quoteName(ks)), []byte(typ), []byte(quoteName(name))})
	}
	element := func(ks, typ, name, stmt string) {
		rows = append(rows, message.Row{[]byte(quoteName(ks)), []byte(typ), []byte(quoteName(name)), []byte(stmt)})
	}
	describeKeyspace := func(k *Keyspace, only bool) {
		element(k.Name, "keyspace", k.Name, keyspaceCQL(k))
		if only {
			return
		}
		for _, u := range k.UserTypes {
			element(k.Name, "type", u.Name, typeCQL(k, u))
		}
		for _, t := range tables(k) {
			element(k.Name, "table", t.Name, tableCQL(t))
		}
	}
	keyspace := func(name string) (*Keyspace, error) {
		if name == "" {
			name = d.session
		}
		if name == "" {
			return nil, invalidf("No keyspace specified and no current keyspace")
		}
		k, ok := s.Keyspaces[name]
		if !ok {
			return nil, invalidf("'%s' not found in keyspaces", name)
		}
		return k, nil
	}
	switch r.Kind {
	case "KEYSPACES":
		for _, k := range keyspaces() {
			list(k.Name, "keyspace", k.Name)
		}
	case "TABLES":
		ks := keyspaces()
		if d.session != "" {
			k, err := keyspace(d.session)
			if err != nil {
				return nil, err
			}
			ks = []*Keyspace{k}
		}
		for _, k := range ks {
			for _, t := range tables(k) {
				list(k.Name, "table", t.Name)
			}
		}
	case "TYPES":
		for _, k := range keyspaces() {
			if d.session != "" && k.Name != d.session {
				continue
			}
			for _, u := range k.UserTypes {
				list(k.Name, "type", u.Name)
			}
		}
	case "FUNCTIONS", "AGGREGATES":
	case "SCHEMA":
		for _, k := range keyspaces() {
			if k.virtual && !r.Full {
				continue
			}
			describeKeyspace(k, false)
		}
	case "KEYSPACE":
		k, err := keyspace(r.Keyspace)
		if err != nil {
			return nil, err
		}
		describeKeyspace(k, r.Only)
	case "TABLE":
		ks := r.Keyspace
		if ks == "" {
			ks = d.session
		}
		t, err := s.table(ks, r.Name)
		if err != nil {
			return nil, invalidf("Table '%s' not found in keyspace '%s'", r.Name, ks)
		}
		element(ks, "table", t.Name, tableCQL(t))
	case "CLUSTER":
		rows = append(rows, message.Row{[]byte(e.d.opts.ClusterName), []byte("org.apache.cassandra.dht.Murmur3Partitioner"),
			[]byte("org.apache.cassandra.locator.SimpleSnitch")})
	default:
		// DESCRIBE name: a keyspace, or a table of the current keyspace.
		if r.Keyspace == "" {
			if k, ok := s.Keyspaces[r.Name]; ok {
				describeKeyspace(k, false)
				break
			}
		}
		ks := r.Keyspace
		if ks == "" {
			ks = d.session
		}
		t, err := s.table(ks, r.Name)
		if err != nil {
			return nil, invalidf("'%s' not found in keyspace '%s'", r.Name, ks)
		}
		element(ks, "table", t.Name, tableCQL(t))
	}
	if rows == nil {
		rows = message.RowSet{}
	}
	return &message.RowsResult{Metadata: md, Data: rows}, nil
}

var plainName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// quoteName is ColumnIdentifier.maybeQuote.
func quoteName(n string) string {
	if plainName.MatchString(n) && !cqlReserved[n] {
		return n
	}
	return `"` + strings.ReplaceAll(n, `"`, `""`) + `"`
}

var cqlReserved = map[string]bool{
	"add": true, "allow": true, "alter": true, "and": true, "apply": true, "asc": true, "authorize": true,
	"batch": true, "begin": true, "by": true, "columnfamily": true, "create": true, "delete": true, "desc": true,
	"describe": true, "drop": true, "entries": true, "execute": true, "from": true, "full": true, "grant": true,
	"if": true, "in": true, "index": true, "infinity": true, "insert": true, "into": true, "keyspace": true,
	"limit": true, "modify": true, "nan": true, "norecursive": true, "not": true, "null": true, "of": true,
	"on": true, "or": true, "order": true, "primary": true, "rename": true, "revoke": true, "schema": true,
	"select": true, "set": true, "table": true, "to": true, "token": true, "truncate": true, "unlogged": true,
	"update": true, "use": true, "using": true, "where": true, "with": true,
}

func cqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func cqlMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = cqlString(k) + ": " + cqlString(m[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// keyspaceCQL is KeyspaceMetadata.toCqlString.
func keyspaceCQL(k *Keyspace) string {
	return fmt.Sprintf("CREATE KEYSPACE %s WITH replication = %s  AND durable_writes = %t;", quoteName(k.Name), cqlMap(k.Replication), k.DurableWrites)
}

// tableCQL is TableMetadata.toCqlString.
func tableCQL(t *Table) string {
	var b strings.Builder
	prefix := "CREATE TABLE "
	if t.virtual != nil {
		prefix = "/*\nWarning: Table " + t.Keyspace + "." + t.Name + " is a virtual table and cannot be recreated with CQL.\nStructure, for reference:\nVIRTUAL TABLE "
	}
	fmt.Fprintf(&b, "%s%s.%s (\n", prefix, quoteName(t.Keyspace), quoteName(t.Name))
	for _, c := range t.AllColumns() {
		fmt.Fprintf(&b, "    %s %s", quoteName(c.Name), c.Type)
		if c.Kind == Static {
			b.WriteString(" static")
		}
		b.WriteString(",\n")
	}
	pk := make([]string, len(t.PartitionKey))
	for i, c := range t.PartitionKey {
		pk[i] = quoteName(c.Name)
	}
	key := strings.Join(pk, ", ")
	if len(pk) > 1 {
		key = "(" + key + ")"
	}
	for _, c := range t.Clustering {
		key += ", " + quoteName(c.Name)
	}
	fmt.Fprintf(&b, "    PRIMARY KEY (%s)\n)", key)
	if t.virtual != nil {
		return b.String() + ";\n*/"
	}
	var opts []string
	if len(t.Clustering) > 0 {
		var order []string
		for _, c := range t.Clustering {
			dir := "ASC"
			if c.Desc {
				dir = "DESC"
			}
			order = append(order, quoteName(c.Name)+" "+dir)
		}
		opts = append(opts, "CLUSTERING ORDER BY ("+strings.Join(order, ", ")+")")
	}
	p := t.Params
	opts = append(opts,
		"additional_write_policy = "+cqlString(p.AdditionalWrite),
		"allow_auto_snapshot = true",
		"bloom_filter_fp_chance = "+strconv.FormatFloat(p.BloomFilterFPChance, 'f', -1, 64),
		"caching = "+cqlMap(p.Caching),
		"cdc = "+strconv.FormatBool(p.CDC),
		"comment = "+cqlString(p.Comment),
		"compaction = "+cqlMap(p.Compaction),
		"compression = "+cqlMap(p.Compression),
		"memtable = 'default'",
		"crc_check_chance = "+formatFloat(p.CRCCheckChance, 64),
		"default_time_to_live = "+strconv.Itoa(p.DefaultTTL),
		"extensions = {}",
		"gc_grace_seconds = "+strconv.Itoa(p.GCGraceSeconds),
		"incremental_backups = true",
		"max_index_interval = "+strconv.Itoa(p.MaxIndexInterval),
		"memtable_flush_period_in_ms = "+strconv.Itoa(p.MemtableFlushPeriod),
		"min_index_interval = "+strconv.Itoa(p.MinIndexInterval),
		"read_repair = "+cqlString(p.ReadRepair),
		"speculative_retry = "+cqlString(p.SpeculativeRetry),
	)
	b.WriteString(" WITH ")
	b.WriteString(strings.Join(opts, "\n    AND "))
	b.WriteString(";")
	return b.String()
}

// typeCQL is UserType.toCqlString.
func typeCQL(k *Keyspace, u UserType) string {
	t := k.types[u.Name]
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TYPE %s.%s (\n", quoteName(k.Name), quoteName(u.Name))
	for i, f := range u.Fields {
		sep := ",\n"
		if i == len(u.Fields)-1 {
			sep = "\n"
		}
		fmt.Fprintf(&b, "    %s %s%s", quoteName(f), t.Elems[i], sep)
	}
	b.WriteString(");")
	return b.String()
}
