// Package cql parses CQL statements into raw (unprepared) statements, as
// Cassandra's Parser.g does into CQLStatement.Raw. Statements are later
// prepared against the schema by the execution layer.
package cql

import "github.com/cassandra-room/cassandra-room/internal/types"

// Statement is a parsed statement.
type Statement interface{ stmt() }

// Parsed is a statement and its bind markers.
type Parsed struct {
	Statement Statement
	// Markers has one entry per bind marker, in order of appearance: the
	// name of a :name marker, or "" for ?.
	Markers []string
}

// Term is a value in a statement.
type Term interface{ term() }

// Literal is a constant.
type Literal struct {
	Kind types.LitKind
	Text string
}

// Null is the NULL constant.
type Null struct{}

// Marker is a bind marker; Index is its position in Parsed.Markers.
type Marker struct{ Index int }

// FunctionCall is a function applied to terms, such as now() or
// toTimestamp(now()).
type FunctionCall struct {
	Name string
	Args []Term
}

// ListLiteral is [a, b].
type ListLiteral struct{ Elems []Term }

// SetLiteral is {a, b}; an empty {} parses as an empty MapLiteral.
type SetLiteral struct{ Elems []Term }

// MapLiteral is {k: v, ...}.
type MapLiteral struct{ Keys, Values []Term }

// UDTLiteral is {field: value, ...} for a user-defined type.
type UDTLiteral struct {
	Fields []string
	Values []Term
}

// TupleLiteral is (a, b) in a value position.
type TupleLiteral struct{ Elems []Term }

// Cast is (type) term.
type Cast struct {
	Type string
	Term Term
}

func (*Literal) term()      {}
func (*Null) term()         {}
func (*Marker) term()       {}
func (*FunctionCall) term() {}
func (*ListLiteral) term()  {}
func (*SetLiteral) term()   {}
func (*MapLiteral) term()   {}
func (*TupleLiteral) term() {}
func (*UDTLiteral) term()   {}
func (*Cast) term()         {}

// Property is name = value in a WITH clause.
type Property struct {
	Name  string
	Value Term
}

// Ordering is a column and direction in ORDER BY or CLUSTERING ORDER BY.
type Ordering struct {
	Column string
	Desc   bool
}

// Relation is a WHERE clause predicate.
type Relation struct {
	// Columns has one column, or several for a multi-column relation
	// such as (a, b) > (1, 2), or the arguments of token(...).
	Columns []string
	Token   bool
	Multi   bool
	// Op is one of = < <= > >= != IN CONTAINS "CONTAINS KEY" LIKE
	// "IS NOT NULL".
	Op    string
	Value Term // for IN: a ListLiteral of the values, or a Marker
}

// Using is a USING clause.
type Using struct {
	Timestamp Term
	TTL       Term
}

// Condition is a column condition of a lightweight transaction.
type Condition struct {
	Column string
	Op     string
	Value  Term
}

// Use is USE keyspace.
type Use struct{ Keyspace string }

// Selector is one item of a SELECT clause.
type Selector struct {
	Expr  Selectable
	Alias string
}

// Selectable is a selector expression.
type Selectable interface{ selectable() }

// ColumnSel selects a column.
type ColumnSel struct{ Name string }

// FuncSel applies a function (count, writetime, ttl, token, cast, min,
// max, ...) to selectors; Star is count(*).
type FuncSel struct {
	Name string
	Args []Selectable
	Star bool
	Type string // the target of CAST
}

// TermSel selects a constant or bind marker.
type TermSel struct{ Term Term }

// OpSel is arithmetic on selectors: Op is one of + - * / %, or 'n' for a
// negation (of Left).
type OpSel struct {
	Op          byte
	Left, Right Selectable
}

func (*OpSel) selectable() {}

func (*ColumnSel) selectable() {}
func (*FuncSel) selectable()   {}
func (*TermSel) selectable()   {}

// Select is a SELECT statement.
type Select struct {
	Keyspace, Table   string
	Distinct, JSON    bool
	Selectors         []Selector // nil for *
	Where             []Relation
	GroupBy           []string
	OrderBy           []Ordering
	PerPartitionLimit Term
	Limit             Term
	AllowFiltering    bool
}

// Insert is INSERT INTO ... VALUES or INSERT INTO ... JSON.
type Insert struct {
	Keyspace, Table string
	Columns         []string
	Values          []Term
	JSON            Term
	DefaultUnset    bool
	IfNotExists     bool
	Using           Using
}

// Operation is one SET item of an UPDATE.
type Operation struct {
	Column string
	// Key is set for c[key] = v; Field for c.field = v.
	Key   Term
	Field string
	// Op is "=", "+" (c = c + v), "-" (c = c - v), "prepend" (c = v + c),
	// "+=" or "-=".
	Op    string
	Value Term
}

// Update is an UPDATE statement.
type Update struct {
	Keyspace, Table string
	Using           Using
	Operations      []Operation
	Where           []Relation
	IfExists        bool
	Conditions      []Condition
}

// Deletion is one column (or element) of a DELETE.
type Deletion struct {
	Column string
	Key    Term
	Field  string
}

// Delete is a DELETE statement.
type Delete struct {
	Keyspace, Table string
	Columns         []Deletion
	Using           Using
	Where           []Relation
	IfExists        bool
	Conditions      []Condition
}

// Batch is BEGIN [UNLOGGED|COUNTER] BATCH ... APPLY BATCH.
type Batch struct {
	Type       string // LOGGED, UNLOGGED or COUNTER
	Using      Using
	Statements []Statement // *Insert, *Update or *Delete
}

// CreateKeyspace is CREATE KEYSPACE.
type CreateKeyspace struct {
	Keyspace    string
	IfNotExists bool
	Properties  []Property
}

// AlterKeyspace is ALTER KEYSPACE.
type AlterKeyspace struct {
	Keyspace   string
	IfExists   bool
	Properties []Property
}

// DropKeyspace is DROP KEYSPACE.
type DropKeyspace struct {
	Keyspace string
	IfExists bool
}

// ColumnDef is a column of CREATE TABLE or ALTER TABLE ADD.
type ColumnDef struct {
	Name   string
	Type   string
	Static bool
}

// CreateTable is CREATE TABLE.
type CreateTable struct {
	Keyspace, Table string
	IfNotExists     bool
	Columns         []ColumnDef
	PartitionKey    []string
	Clustering      []string
	ClusteringOrder []Ordering
	CompactStorage  bool
	Properties      []Property
}

// AlterTable is ALTER TABLE.
type AlterTable struct {
	Keyspace, Table string
	IfExists        bool
	Add             []ColumnDef
	Drop            []string
	Rename          [][2]string
	Properties      []Property
}

// DropTable is DROP TABLE.
type DropTable struct {
	Keyspace, Table string
	IfExists        bool
}

// CreateType is CREATE TYPE.
type CreateType struct {
	Keyspace, Name string
	IfNotExists    bool
	Fields         []ColumnDef
}

// AlterType is ALTER TYPE ... ADD or RENAME.
type AlterType struct {
	Keyspace, Name string
	IfExists       bool
	Add            []ColumnDef
	Rename         [][2]string
}

// DropType is DROP TYPE.
type DropType struct {
	Keyspace, Name string
	IfExists       bool
}

// CreateIndex is CREATE [CUSTOM] INDEX.
type CreateIndex struct {
	Keyspace, Table, Name string
	IfNotExists           bool
	Column                string
	// Target is "values", "keys", "entries", "full", or "" (the column's
	// default).
	Target  string
	Class   string // USING 'class'
	Options map[string]string
}

// DropIndex is DROP INDEX.
type DropIndex struct {
	Keyspace, Name string
	IfExists       bool
}

func (*CreateIndex) stmt() {}
func (*DropIndex) stmt()   {}
func (*CreateType) stmt()  {}
func (*AlterType) stmt()   {}
func (*DropType) stmt()    {}

// Truncate is TRUNCATE.
type Truncate struct{ Keyspace, Table string }

// Describe is a DESCRIBE statement (DescribeStatement). Kind is one of
// SCHEMA, KEYSPACES, TABLES, TYPES, FUNCTIONS, AGGREGATES, KEYSPACE, TABLE,
// CLUSTER, or "" for DESCRIBE name (a keyspace or a table).
type Describe struct {
	Kind           string
	Keyspace, Name string
	Full, Only     bool
	Internals      bool
}

func (*Describe) stmt()       {}
func (*Use) stmt()            {}
func (*Select) stmt()         {}
func (*Insert) stmt()         {}
func (*Update) stmt()         {}
func (*Delete) stmt()         {}
func (*Batch) stmt()          {}
func (*CreateKeyspace) stmt() {}
func (*AlterKeyspace) stmt()  {}
func (*DropKeyspace) stmt()   {}
func (*CreateTable) stmt()    {}
func (*AlterTable) stmt()     {}
func (*DropTable) stmt()      {}
func (*Truncate) stmt()       {}
