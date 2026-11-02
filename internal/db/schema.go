package db

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/types"
)

// ColumnKind is a column's role (ColumnMetadata.Kind).
type ColumnKind uint8

const (
	PartitionKey ColumnKind = iota
	Clustering
	Regular
	Static
)

func (k ColumnKind) String() string {
	return [...]string{"partition_key", "clustering", "regular", "static"}[k]
}

// Column is a column of a table.
type Column struct {
	Name     string
	Type     *types.Type
	Kind     ColumnKind
	Position int // in the partition key or clustering; -1 otherwise
	// ID identifies a regular or static column's cells in storage; 0 is
	// the row's primary key liveness (row marker).
	ID   uint32
	Desc bool // clustering order
}

// Table is a table's metadata (TableMetadata).
type Table struct {
	Keyspace, Name string
	ID             [16]byte
	Storage        lsm.TableID
	PartitionKey   []*Column
	Clustering     []*Column
	Columns        []*Column // regular and static, ordered by name
	byName         map[string]*Column
	byID           map[uint32]*Column
	NextColumnID   uint32
	Params         TableParams
	HasStatic      bool
	Indexes        []*Index

	// virtual tables (system keyspaces) generate their rows on reads.
	virtual func(db *DB) []map[string][]byte
}

// Column returns a column by name.
func (t *Table) Column(name string) *Column { return t.byName[name] }

// AllColumns returns the columns in Cassandra's order: partition key,
// clustering, then the others by name.
func (t *Table) AllColumns() []*Column {
	out := make([]*Column, 0, len(t.PartitionKey)+len(t.Clustering)+len(t.Columns))
	out = append(out, t.PartitionKey...)
	out = append(out, t.Clustering...)
	return append(out, t.Columns...)
}

func (t *Table) index() {
	defer func() {
		for _, ix := range t.Indexes {
			ix.col = t.byName[ix.Column]
		}
	}()
	t.byName = map[string]*Column{}
	t.byID = map[uint32]*Column{}
	t.HasStatic = false
	for _, c := range t.AllColumns() {
		t.byName[c.Name] = c
		if c.Kind == Regular || c.Kind == Static {
			t.byID[c.ID] = c
		}
		if c.Kind == Static {
			t.HasStatic = true
		}
	}
}

// TableParams are a table's options (TableParams).
type TableParams struct {
	Comment             string
	GCGraceSeconds      int
	DefaultTTL          int
	Compaction          map[string]string
	Compression         map[string]string
	Caching             map[string]string
	BloomFilterFPChance float64
	CRCCheckChance      float64
	MaxIndexInterval    int
	MinIndexInterval    int
	MemtableFlushPeriod int
	SpeculativeRetry    string
	AdditionalWrite     string
	ReadRepair          string
	CDC                 bool
}

func defaultParams() TableParams {
	return TableParams{
		GCGraceSeconds:      864000,
		Compaction:          map[string]string{"class": "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy"},
		Compression:         map[string]string{"chunk_length_in_kb": "16", "class": "org.apache.cassandra.io.compress.LZ4Compressor"},
		Caching:             map[string]string{"keys": "ALL", "rows_per_partition": "NONE"},
		BloomFilterFPChance: 0.01,
		CRCCheckChance:      1,
		MaxIndexInterval:    2048,
		MinIndexInterval:    128,
		SpeculativeRetry:    "99p",
		AdditionalWrite:     "99p",
		ReadRepair:          "BLOCKING",
	}
}

// Keyspace is a keyspace's metadata.
type Keyspace struct {
	Name          string
	Replication   map[string]string
	DurableWrites bool
	// UserTypes are the keyspace's user-defined types, in creation order
	// (a type can use the types before it).
	UserTypes []UserType        `json:",omitempty"`
	Tables    map[string]*Table `json:"-"`
	types     map[string]*types.Type
	virtual   bool
}

// UserType is a user-defined type's definition.
type UserType struct {
	Name       string
	Fields     []string
	FieldTypes []string
}

// buildTypes resolves the keyspace's user types.
func (k *Keyspace) buildTypes() error {
	k.types = map[string]*types.Type{}
	for _, u := range k.UserTypes {
		t := &types.Type{Kind: types.UDT, Keyspace: k.Name, Name: u.Name, Fields: u.Fields}
		for _, ft := range u.FieldTypes {
			typ, err := types.ParseWith(ft, k.resolve)
			if err != nil {
				return err
			}
			if typ.IsMultiCell() {
				typ.Frozen = true
			}
			t.Elems = append(t.Elems, typ)
		}
		k.types[u.Name] = t
	}
	return nil
}

func (k *Keyspace) resolve(name string) (*types.Type, error) {
	if t, ok := k.types[name]; ok {
		return t, nil
	}
	return nil, invalidf("Unknown type %s.%s", k.Name, name)
}

// Schema is the cluster metadata (TCM's ClusterMetadata): an immutable
// snapshot of the schema, roles and, in a cluster, the node directory,
// identified by its epoch. Every change produces a new snapshot.
type Schema struct {
	Keyspaces map[string]*Keyspace
	Epoch     int64
	// NextStorageID allocates storage table ids, cluster-wide.
	NextStorageID uint64
	Roles         map[string]*Role
	// Nodes is the directory of a cluster's nodes, by host id.
	Nodes map[string]*cluster.Node
	// Entries are the ids of the latest changes, for idempotent commits.
	Entries   []string
	Version   [16]byte
	byStorage map[lsm.TableID]*Table
	// storage holds every storage table in use: tables' and indexes'.
	storage map[lsm.TableID]bool
}

func (s *Schema) table(ks, name string) (*Table, error) {
	k, ok := s.Keyspaces[ks]
	if !ok {
		return nil, invalidf("Keyspace %s does not exist", ks)
	}
	t, ok := k.Tables[name]
	if !ok {
		return nil, invalidf("table %s does not exist", name)
	}
	return t, nil
}

// clone copies the schema for a DDL change (tables are copied on write by
// the caller).
func (s *Schema) clone() *Schema {
	out := &Schema{Keyspaces: make(map[string]*Keyspace, len(s.Keyspaces)), Epoch: s.Epoch,
		NextStorageID: s.NextStorageID, Roles: cloneRoles(s.Roles), Nodes: map[string]*cluster.Node{},
		Entries: slices.Clone(s.Entries)}
	for id, n := range s.Nodes {
		c := *n
		c.Tokens = slices.Clone(n.Tokens)
		out.Nodes[id] = &c
	}
	for n, k := range s.Keyspaces {
		k2 := *k
		k2.Tables = make(map[string]*Table, len(k.Tables))
		for tn, t := range k.Tables {
			k2.Tables[tn] = t
		}
		out.Keyspaces[n] = &k2
	}
	return out
}

// finish indexes the schema and computes its version, a digest of the user
// keyspaces' definitions (as Cassandra's schema version).
func (s *Schema) finish() {
	s.byStorage = map[lsm.TableID]*Table{}
	s.storage = map[lsm.TableID]bool{}
	h := md5.New()
	names := make([]string, 0, len(s.Keyspaces))
	for n := range s.Keyspaces {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		k := s.Keyspaces[n]
		b, _ := json.Marshal(k)
		h.Write(b)
		tnames := make([]string, 0, len(k.Tables))
		for tn, t := range k.Tables {
			tnames = append(tnames, tn)
			if t.virtual == nil {
				s.byStorage[t.Storage] = t
				s.storage[t.Storage] = true
				for _, ix := range t.Indexes {
					s.storage[ix.Storage] = true
				}
			}
		}
		sort.Strings(tnames)
		for _, tn := range tnames {
			b, _ := marshalTable(k.Tables[tn])
			h.Write(b)
		}
	}
	copy(s.Version[:], h.Sum(nil))
	s.Version[6] = s.Version[6]&0x0f | 0x30 // name-based UUID (v3)
	s.Version[8] = s.Version[8]&0x3f | 0x80
}

var nameRE = regexp.MustCompile(`^\w+$`)

// validName is SchemaConstants.isValidName.
func validName(name string) bool { return nameRE.MatchString(name) && len(name) <= 222 }

// newTable builds a table from CREATE TABLE (CreateTableStatement.builder).
func newTable(k *Keyspace, st *cql.CreateTable) (*Table, error) {
	return buildTable(k.Name, k.resolve, st, false)
}

// buildTable builds a table; virtual tables may use every type.
func buildTable(ks string, resolve func(string) (*types.Type, error), st *cql.CreateTable, virtual bool) (*Table, error) {
	if !validName(st.Table) {
		return nil, invalidf("Table name must not be empty, more than 222 characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", st.Table)
	}
	t := &Table{Keyspace: ks, Name: st.Table, Params: defaultParams(), NextColumnID: 1}
	if err := applyTableProperties(&t.Params, st.Properties); err != nil {
		return nil, err
	}
	defs := map[string]cql.ColumnDef{}
	for _, c := range st.Columns {
		if _, dup := defs[c.Name]; dup {
			return nil, invalidf("Duplicate column '%s' declaration for table '%s'", c.Name, st.Table)
		}
		defs[c.Name] = c
	}
	if len(st.PartitionKey) == 0 {
		return nil, invalidf("No PRIMARY KEY specifed for table '%s' (exactly one required)", st.Table)
	}
	typeOf := func(c cql.ColumnDef) (*types.Type, error) {
		typ, err := types.ParseWith(c.Type, resolve)
		if err != nil {
			return nil, invalidf("%v", err)
		}
		return typ, nil
	}
	pk := map[string]bool{}
	order := map[string]bool{}
	for _, o := range st.ClusteringOrder {
		order[o.Column] = o.Desc
	}
	for i, name := range append(slices.Clone(st.PartitionKey), st.Clustering...) {
		def, ok := defs[name]
		if !ok {
			return nil, invalidf("Unknown column '%s' referenced in PRIMARY KEY for table '%s'", name, st.Table)
		}
		if pk[name] {
			return nil, invalidf("Duplicate column '%s' in PRIMARY KEY clause for table '%s'", name, st.Table)
		}
		pk[name] = true
		typ, err := typeOf(def)
		if err != nil {
			return nil, err
		}
		switch {
		case typ.IsMultiCell():
			return nil, invalidf("Invalid non-frozen collection type %s for PRIMARY KEY column '%s'", typ, name)
		case typ.Kind == types.Counter:
			return nil, invalidf("counter type is not supported for PRIMARY KEY column '%s'", name)
		case referencesDuration(typ):
			return nil, invalidf("duration type is not supported for PRIMARY KEY column '%s'", name)
		case def.Static:
			return nil, invalidf("Static column '%s' cannot be part of the PRIMARY KEY", name)
		}
		if i < len(st.PartitionKey) {
			t.PartitionKey = append(t.PartitionKey, &Column{Name: name, Type: typ, Kind: PartitionKey, Position: i})
		} else {
			p := i - len(st.PartitionKey)
			t.Clustering = append(t.Clustering, &Column{Name: name, Type: typ, Kind: Clustering, Position: p, Desc: order[name]})
		}
	}
	for i, o := range st.ClusteringOrder {
		if !slices.Contains(st.Clustering, o.Column) {
			return nil, invalidf("Only clustering key columns can be defined in CLUSTERING ORDER directive: [%s] are not clustering columns", o.Column)
		}
		if c := st.Clustering[i]; c != o.Column {
			if _, ok := order[c]; ok {
				return nil, invalidf("The order of columns in the CLUSTERING ORDER directive must match that of the clustering columns (%s must appear before %s)", c, o.Column)
			}
			return nil, invalidf("Missing CLUSTERING ORDER for column %s", c)
		}
	}
	if st.CompactStorage {
		return nil, invalidf("COMPACT STORAGE tables are not supported")
	}
	var rest []string
	for name := range defs {
		if !pk[name] {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	for _, name := range rest {
		def := defs[name]
		typ, err := typeOf(def)
		if err != nil {
			return nil, err
		}
		if !virtual {
			if err := supportedColumnType(typ, name); err != nil {
				return nil, err
			}
		}
		kind := Regular
		if def.Static {
			if len(st.Clustering) == 0 {
				return nil, invalidf("Static columns are only useful (and thus allowed) if the table has at least one clustering column")
			}
			kind = Static
		}
		t.Columns = append(t.Columns, &Column{Name: name, Type: typ, Kind: kind, Position: -1, ID: t.NextColumnID})
		t.NextColumnID++
	}
	if err := checkCounters(t); err != nil {
		return nil, err
	}
	t.index()
	return t, nil
}

// checkCounters enforces counter tables' rules: every non-key column a
// counter, and no default TTL.
func checkCounters(t *Table) error {
	counters := 0
	for _, c := range t.Columns {
		if c.Type.Kind == types.Counter {
			counters++
		}
	}
	if counters == 0 {
		return nil
	}
	if counters != len(t.Columns) {
		return invalidf("Cannot mix counter and non counter columns in the same table")
	}
	if t.Params.DefaultTTL > 0 {
		return invalidf("Cannot set default_time_to_live on a table with counters")
	}
	return nil
}

// IsCounter reports whether t is a counter table.
func (t *Table) IsCounter() bool {
	return len(t.Columns) > 0 && t.Columns[0].Type.Kind == types.Counter
}

func referencesDuration(t *types.Type) bool {
	if t.Kind == types.Duration {
		return true
	}
	return slices.ContainsFunc(t.Elems, referencesDuration)
}

// supportedColumnType checks a regular or static column's type.
func supportedColumnType(t *types.Type, name string) error {
	if t.Kind == types.UDT && t.IsMultiCell() {
		for _, f := range t.Elems {
			if f.IsMultiCell() {
				return invalidf("Non-frozen UDTs with nested non-frozen collections are not supported")
			}
		}
	}
	return nil
}

// tableOptions are the options TableAttributes accepts.
var tableOptions = map[string]bool{
	"allow_auto_snapshot": true, "bloom_filter_fp_chance": true, "caching": true, "cdc": true,
	"comment": true, "compaction": true, "compression": true, "crc_check_chance": true,
	"default_time_to_live": true, "extensions": true, "gc_grace_seconds": true,
	"incremental_backups": true, "max_index_interval": true, "memtable": true,
	"memtable_flush_period_in_ms": true, "min_index_interval": true, "read_repair": true,
	"speculative_retry": true, "additional_write_policy": true, "id": true,
	"dclocal_read_repair_chance": true, "read_repair_chance": true,
}

func applyTableProperties(p *TableParams, props []cql.Property) error {
	for _, prop := range props {
		if !tableOptions[prop.Name] {
			return syntaxf("Unknown property '%s'", prop.Name)
		}
		switch prop.Name {
		case "comment", "speculative_retry", "additional_write_policy", "read_repair":
			s, err := stringProperty(prop)
			if err != nil {
				return err
			}
			switch prop.Name {
			case "comment":
				p.Comment = s
			case "speculative_retry":
				p.SpeculativeRetry = s
			case "additional_write_policy":
				p.AdditionalWrite = s
			case "read_repair":
				p.ReadRepair = strings.ToUpper(s)
			}
		case "gc_grace_seconds", "default_time_to_live", "max_index_interval", "min_index_interval", "memtable_flush_period_in_ms":
			s, err := stringProperty(prop)
			if err != nil {
				return err
			}
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 {
				return syntaxf("Invalid value for property '%s': %s", prop.Name, s)
			}
			switch prop.Name {
			case "gc_grace_seconds":
				p.GCGraceSeconds = n
			case "default_time_to_live":
				if n > maxTTL {
					return invalidf("default_time_to_live must be less than or equal to %d (got %d)", maxTTL, n)
				}
				p.DefaultTTL = n
			case "max_index_interval":
				p.MaxIndexInterval = n
			case "min_index_interval":
				p.MinIndexInterval = n
			case "memtable_flush_period_in_ms":
				p.MemtableFlushPeriod = n
			}
		case "bloom_filter_fp_chance", "crc_check_chance":
			s, err := stringProperty(prop)
			if err != nil {
				return err
			}
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return syntaxf("Invalid value for property '%s': %s", prop.Name, s)
			}
			if prop.Name == "crc_check_chance" {
				p.CRCCheckChance = f
			} else {
				p.BloomFilterFPChance = f
			}
		case "cdc":
			s, err := stringProperty(prop)
			if err != nil {
				return err
			}
			p.CDC = strings.EqualFold(s, "true")
		case "compaction":
			m, err := mapProperty(prop)
			if err != nil {
				return err
			}
			if _, err := compactionOptions(m, 0); err != nil {
				return err
			}
			p.Compaction = m
		case "compression", "caching":
			m, err := mapProperty(prop)
			if err != nil {
				return err
			}
			if prop.Name == "compression" {
				p.Compression = m
			} else {
				p.Caching = m
			}
		}
	}
	return nil
}

func stringProperty(p cql.Property) (string, error) {
	l, ok := p.Value.(*cql.Literal)
	if !ok {
		return "", syntaxf("Invalid value for property '%s'", p.Name)
	}
	return l.Text, nil
}

func mapProperty(p cql.Property) (map[string]string, error) {
	m, ok := p.Value.(*cql.MapLiteral)
	if !ok {
		return nil, syntaxf("Invalid value for property '%s'. It should be a map.", p.Name)
	}
	out := map[string]string{}
	for i := range m.Keys {
		k, ok1 := m.Keys[i].(*cql.Literal)
		v, ok2 := m.Values[i].(*cql.Literal)
		if !ok1 || !ok2 {
			return nil, syntaxf("Invalid value for property '%s'", p.Name)
		}
		out[k.Text] = v.Text
	}
	return out, nil
}

// compactionOptions translates a table's compaction map to the storage
// engine's options. UCS options map one to one; TimeWindowCompactionStrategy
// selects the time-window mode; other strategies run as UCS with their
// closest parameters (STCS as T4, LCS as L10).
func compactionOptions(m map[string]string, gcGrace int) (lsm.TableOptions, error) {
	var o lsm.TableOptions
	o.GCGraceSeconds = int64(gcGrace)
	if gcGrace == 0 {
		o.GCGraceSeconds = -1
	}
	class := m["class"]
	short := class[strings.LastIndexByte(class, '.')+1:]
	switch short {
	case "UnifiedCompactionStrategy", "":
	case "SizeTieredCompactionStrategy":
		o.ScalingParameters = []int{2}
	case "LeveledCompactionStrategy":
		o.ScalingParameters = []int{-8}
	case "TimeWindowCompactionStrategy":
		unit := strings.ToUpper(m["compaction_window_unit"])
		size := 1
		if s, ok := m["compaction_window_size"]; ok {
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 {
				return o, invalidf("%s must be a positive integer", "compaction_window_size")
			}
			size = n
		}
		var d int64
		switch unit {
		case "", "DAYS":
			d = 86400
		case "HOURS":
			d = 3600
		case "MINUTES":
			d = 60
		default:
			return o, invalidf("%s is not valid for compaction_window_unit", unit)
		}
		o.TimeWindow = time.Duration(d*int64(size)) * time.Second
		o.UnsafeAggressiveExpiration = strings.EqualFold(m["unsafe_aggressive_sstable_expiration"], "true")
	default:
		return o, invalidf("Unable to find compaction strategy class '%s'", class)
	}
	for k, v := range m {
		var err error
		switch k {
		case "scaling_parameters":
			o.ScalingParameters, err = parseScalingParameters(v)
		case "base_shard_count":
			o.BaseShardCount, err = strconv.Atoi(v)
		case "target_sstable_size":
			o.TargetSSTableSize, err = parseSize(v)
		case "min_sstable_size":
			o.MinSSTableSize, err = parseSize(v)
		case "sstable_growth":
			o.SSTableGrowth, err = strconv.ParseFloat(v, 64)
		case "max_sstables_to_compact":
			o.MaxSSTablesToCompact, err = strconv.Atoi(v)
		case "min_threshold":
			o.MinThreshold, err = strconv.Atoi(v)
		case "max_threshold":
			o.MaxThreshold, err = strconv.Atoi(v)
		case "tombstone_threshold":
			o.TombstoneThreshold, err = strconv.ParseFloat(v, 64)
		case "unchecked_tombstone_compaction":
			o.UncheckedTombstoneCompaction = strings.EqualFold(v, "true")
		}
		if err != nil {
			return o, invalidf("Invalid value %s for compaction option %s", v, k)
		}
	}
	return o, nil
}

// parseScalingParameters parses "T4", "L10, T4" or "N" into W values
// (Controller.parseScalingParameters: Tn is n-2, Ln is 2-n, N is 0).
func parseScalingParameters(s string) ([]int, error) {
	var out []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(strings.ToUpper(p))
		switch {
		case p == "N":
			out = append(out, 0)
		case len(p) > 1 && (p[0] == 'T' || p[0] == 'L'):
			n, err := strconv.Atoi(p[1:])
			if err != nil || n < 2 {
				return nil, fmt.Errorf("bad scaling parameter %q", p)
			}
			if p[0] == 'T' {
				out = append(out, n-2)
			} else {
				out = append(out, 2-n)
			}
		default:
			n, err := strconv.Atoi(p)
			if err != nil {
				return nil, fmt.Errorf("bad scaling parameter %q", p)
			}
			out = append(out, n)
		}
	}
	return out, nil
}

// parseSize parses sizes such as "1GiB", "100MiB" or a byte count.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"B", 1}}
	for _, u := range units {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
			return int64(v * float64(u.mult)), err
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

// replication validates CREATE KEYSPACE's replication map and returns it
// with the full strategy class name.
func replication(props []cql.Property) (map[string]string, bool, error) {
	var repl map[string]string
	durable := true
	for _, p := range props {
		switch p.Name {
		case "replication":
			m, err := mapProperty(p)
			if err != nil {
				return nil, false, err
			}
			repl = m
		case "durable_writes":
			s, err := stringProperty(p)
			if err != nil {
				return nil, false, err
			}
			durable = !strings.EqualFold(s, "false")
		default:
			return nil, false, syntaxf("Unknown property '%s'", p.Name)
		}
	}
	if repl == nil {
		return nil, false, configf("Missing mandatory option 'replication'")
	}
	class := repl["class"]
	switch class[strings.LastIndexByte(class, '.')+1:] {
	case "SimpleStrategy":
		if _, ok := repl["replication_factor"]; !ok {
			return nil, false, configf("SimpleStrategy requires a replication_factor strategy option.")
		}
		repl["class"] = "org.apache.cassandra.locator.SimpleStrategy"
	case "NetworkTopologyStrategy":
		repl["class"] = "org.apache.cassandra.locator.NetworkTopologyStrategy"
	case "":
		return nil, false, configf("Missing replication strategy class")
	default:
		return nil, false, configf("Unable to find replication strategy class '%s'", class)
	}
	for k, v := range repl {
		if k == "class" {
			continue
		}
		if _, err := strconv.Atoi(v); err != nil {
			return nil, false, configf("Replication factor must be numeric; found %s", v)
		}
	}
	return repl, durable, nil
}

// Persisted form of a table.
type tableJSON struct {
	Keyspace, Name string
	ID             string
	Storage        uint64
	PartitionKey   []columnJSON
	Clustering     []columnJSON
	Columns        []columnJSON
	NextColumnID   uint32
	Params         TableParams
	Indexes        []*Index `json:",omitempty"`
}

type columnJSON struct {
	Name   string
	Type   string
	Static bool `json:",omitempty"`
	ID     uint32
	Desc   bool `json:",omitempty"`
}

func marshalTable(t *Table) ([]byte, error) {
	j := tableJSON{Keyspace: t.Keyspace, Name: t.Name, ID: types.FormatUUID(t.ID[:]), Storage: uint64(t.Storage),
		NextColumnID: t.NextColumnID, Params: t.Params, Indexes: t.Indexes}
	conv := func(cs []*Column) []columnJSON {
		var out []columnJSON
		for _, c := range cs {
			out = append(out, columnJSON{Name: c.Name, Type: c.Type.String(), Static: c.Kind == Static, ID: c.ID, Desc: c.Desc})
		}
		return out
	}
	j.PartitionKey, j.Clustering, j.Columns = conv(t.PartitionKey), conv(t.Clustering), conv(t.Columns)
	return json.Marshal(j)
}

func unmarshalTable(b []byte, resolve func(string) (*types.Type, error)) (*Table, error) {
	var j tableJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, err
	}
	id, err := types.ParseUUID(j.ID)
	if err != nil {
		return nil, err
	}
	t := &Table{Keyspace: j.Keyspace, Name: j.Name, ID: id, Storage: lsm.TableID(j.Storage), NextColumnID: j.NextColumnID, Params: j.Params, Indexes: j.Indexes}
	conv := func(cs []columnJSON, kind ColumnKind) ([]*Column, error) {
		var out []*Column
		for i, c := range cs {
			typ, err := types.ParseWith(c.Type, resolve)
			if err != nil {
				return nil, err
			}
			col := &Column{Name: c.Name, Type: typ, Kind: kind, Position: i, ID: c.ID, Desc: c.Desc}
			if kind == Regular {
				col.Position = -1
				if c.Static {
					col.Kind = Static
				}
			}
			out = append(out, col)
		}
		return out, nil
	}
	if t.PartitionKey, err = conv(j.PartitionKey, PartitionKey); err != nil {
		return nil, err
	}
	if t.Clustering, err = conv(j.Clustering, Clustering); err != nil {
		return nil, err
	}
	if t.Columns, err = conv(j.Columns, Regular); err != nil {
		return nil, err
	}
	t.index()
	return t, nil
}
