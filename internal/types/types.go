// Package types is the CQL type system: type names, the native protocol
// serialization of values, conversion of CQL literals, and an order-preserving
// byte encoding of values used in storage keys. It follows Cassandra's
// db/marshal (AbstractType and subclasses), serializers and
// cql3/terms/Constants.
package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
)

// Kind is a CQL type.
type Kind uint8

const (
	Ascii Kind = iota + 1
	Bigint
	Blob
	Boolean
	Counter
	Date
	Decimal
	Double
	Duration
	Float
	Inet
	Int
	Smallint
	Text
	Time
	Timestamp
	Timeuuid
	Tinyint
	UUID
	Varint
	List
	Set
	Map
	Tuple
	UDT
	Vector
)

var names = map[Kind]string{
	Ascii: "ascii", Bigint: "bigint", Blob: "blob", Boolean: "boolean", Counter: "counter",
	Date: "date", Decimal: "decimal", Double: "double", Duration: "duration", Float: "float",
	Inet: "inet", Int: "int", Smallint: "smallint", Text: "text", Time: "time",
	Timestamp: "timestamp", Timeuuid: "timeuuid", Tinyint: "tinyint", UUID: "uuid",
	Varint: "varint", List: "list", Set: "set", Map: "map", Tuple: "tuple",
}

var byName = func() map[string]Kind {
	m := map[string]Kind{"varchar": Text}
	for k, n := range names {
		if k < List {
			m[n] = k
		}
	}
	return m
}()

// Type is a CQL type. Collections, tuples and user types have element
// types; a collection or user type is Frozen when it is stored as one value
// (tuples always are).
type Type struct {
	Kind   Kind
	Elems  []*Type // list, set: 1; map: key and value; tuple, UDT: the fields
	Frozen bool
	// A user-defined type's keyspace, name and field names.
	Keyspace, Name string
	Fields         []string
	// Dim is a vector's number of elements.
	Dim int
}

// Native types, shared.
var (
	AsciiType     = &Type{Kind: Ascii}
	BigintType    = &Type{Kind: Bigint}
	BlobType      = &Type{Kind: Blob}
	BooleanType   = &Type{Kind: Boolean}
	CounterType   = &Type{Kind: Counter}
	DateType      = &Type{Kind: Date}
	DecimalType   = &Type{Kind: Decimal}
	DoubleType    = &Type{Kind: Double}
	DurationType  = &Type{Kind: Duration}
	FloatType     = &Type{Kind: Float}
	InetType      = &Type{Kind: Inet}
	IntType       = &Type{Kind: Int}
	SmallintType  = &Type{Kind: Smallint}
	TextType      = &Type{Kind: Text}
	TimeType      = &Type{Kind: Time}
	TimestampType = &Type{Kind: Timestamp}
	TimeuuidType  = &Type{Kind: Timeuuid}
	TinyintType   = &Type{Kind: Tinyint}
	UUIDType      = &Type{Kind: UUID}
	VarintType    = &Type{Kind: Varint}
)

// ListOf, SetOf, MapOf build collection types.
func ListOf(e *Type, frozen bool) *Type { return &Type{Kind: List, Elems: []*Type{e}, Frozen: frozen} }
func SetOf(e *Type, frozen bool) *Type  { return &Type{Kind: Set, Elems: []*Type{e}, Frozen: frozen} }
func MapOf(k, v *Type, frozen bool) *Type {
	return &Type{Kind: Map, Elems: []*Type{k, v}, Frozen: frozen}
}

// IsCollection reports whether t is a list, set or map.
func (t *Type) IsCollection() bool { return t.Kind == List || t.Kind == Set || t.Kind == Map }

// IsMultiCell reports whether values of t are stored as several cells: a
// collection or user type that is not frozen.
func (t *Type) IsMultiCell() bool { return (t.IsCollection() || t.Kind == UDT) && !t.Frozen }

// String returns the type as CQL writes it (CQL3Type.toString): frozen
// collections and tuples, at any depth, as frozen<...>.
func (t *Type) String() string {
	var b strings.Builder
	t.write(&b)
	return b.String()
}

func (t *Type) write(b *strings.Builder) {
	if t.Frozen {
		b.WriteString("frozen<")
	}
	if t.Kind == UDT {
		b.WriteString(QuoteName(t.Name))
		if t.Frozen {
			b.WriteByte('>')
		}
		return
	}
	if t.Kind == Vector {
		b.WriteString("vector<")
		t.Elems[0].write(b)
		fmt.Fprintf(b, ", %d>", t.Dim)
		return
	}
	b.WriteString(names[t.Kind])
	if len(t.Elems) > 0 {
		b.WriteByte('<')
		for i, e := range t.Elems {
			if i > 0 {
				b.WriteString(", ")
			}
			e.write(b)
		}
		b.WriteByte('>')
	}
	if t.Frozen {
		b.WriteByte('>')
	}
}

// Equal reports whether two types are the same.
func (t *Type) Equal(o *Type) bool {
	if t.Kind != o.Kind || len(t.Elems) != len(o.Elems) || ((t.IsCollection() || t.Kind == UDT) && t.Frozen != o.Frozen) {
		return false
	}
	if t.Kind == UDT && (t.Keyspace != o.Keyspace || t.Name != o.Name) {
		return false
	}
	if t.Kind == Vector && t.Dim != o.Dim {
		return false
	}
	for i := range t.Elems {
		if !t.Elems[i].Equal(o.Elems[i]) {
			return false
		}
	}
	return true
}

// Parse parses a CQL type such as "int", "frozen<map<text, int>>" or
// "tuple<int, text>". Collections nested in collections or tuples must be
// frozen, as in Cassandra; Parse freezes them.
func Parse(s string) (*Type, error) { return ParseWith(s, nil) }

// ParseWith parses a type, resolving user-defined type names with resolve.
func ParseWith(s string, resolve func(name string) (*Type, error)) (*Type, error) {
	p := typeParser{s: s, resolve: resolve}
	t, err := p.parse(false)
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.i != len(p.s) {
		return nil, fmt.Errorf("invalid type %q", s)
	}
	return t, nil
}

type typeParser struct {
	s       string
	i       int
	resolve func(string) (*Type, error)
	quoted  bool
}

func (p *typeParser) skipSpace() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n' || p.s[p.i] == '\r') {
		p.i++
	}
}

func (p *typeParser) ident() string {
	p.quoted = false
	p.skipSpace()
	if p.i < len(p.s) && p.s[p.i] == '"' {
		j := strings.IndexByte(p.s[p.i+1:], '"')
		if j < 0 {
			return ""
		}
		id := p.s[p.i+1 : p.i+1+j]
		p.i += j + 2
		p.quoted = true
		return id
	}
	j := p.i
	for p.i < len(p.s) && (p.s[p.i] == '_' || p.s[p.i] >= 'a' && p.s[p.i] <= 'z' ||
		p.s[p.i] >= 'A' && p.s[p.i] <= 'Z' || p.s[p.i] >= '0' && p.s[p.i] <= '9') {
		p.i++
	}
	return strings.ToLower(p.s[j:p.i])
}

func (p *typeParser) expect(c byte) error {
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != c {
		return fmt.Errorf("invalid type %q: expected '%c'", p.s, c)
	}
	p.i++
	return nil
}

func (p *typeParser) parse(nested bool) (*Type, error) {
	name := p.ident()
	if k, ok := byName[name]; ok && !p.quoted {
		return &Type{Kind: k}, nil
	}
	if p.quoted || (byName[name] == 0 && name != "frozen" && name != "list" && name != "set" && name != "map" && name != "tuple" && name != "vector" && name != "") {
		if p.resolve == nil {
			return nil, fmt.Errorf("unknown type %s", name)
		}
		u, err := p.resolve(name)
		if err != nil {
			return nil, err
		}
		cp := *u
		cp.Frozen = nested
		return &cp, nil
	}
	var t *Type
	switch name {
	case "frozen":
		if err := p.expect('<'); err != nil {
			return nil, err
		}
		inner, err := p.parse(true)
		if err != nil {
			return nil, err
		}
		if err := p.expect('>'); err != nil {
			return nil, err
		}
		inner.freeze()
		return inner, nil
	case "list", "set":
		if err := p.expect('<'); err != nil {
			return nil, err
		}
		e, err := p.parse(true)
		if err != nil {
			return nil, err
		}
		if err := p.expect('>'); err != nil {
			return nil, err
		}
		t = &Type{Kind: List, Elems: []*Type{e}}
		if name == "set" {
			t.Kind = Set
		}
	case "map":
		if err := p.expect('<'); err != nil {
			return nil, err
		}
		k, err := p.parse(true)
		if err != nil {
			return nil, err
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
		v, err := p.parse(true)
		if err != nil {
			return nil, err
		}
		if err := p.expect('>'); err != nil {
			return nil, err
		}
		t = &Type{Kind: Map, Elems: []*Type{k, v}}
	case "vector":
		if err := p.expect('<'); err != nil {
			return nil, err
		}
		e, err := p.parse(true)
		if err != nil {
			return nil, err
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
		p.skipSpace()
		start := p.i
		for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
			p.i++
		}
		dim, err := strconv.Atoi(p.s[start:p.i])
		if err != nil || dim <= 0 {
			return nil, fmt.Errorf("vectors must have a dimension greater than 0")
		}
		if err := p.expect('>'); err != nil {
			return nil, err
		}
		if e.Kind == Counter || e.Kind == Duration {
			return nil, fmt.Errorf("vectors may not contain %s", e)
		}
		return VectorOf(e, dim), nil
	case "tuple":
		if err := p.expect('<'); err != nil {
			return nil, err
		}
		t = &Type{Kind: Tuple, Frozen: true}
		for {
			e, err := p.parse(true)
			if err != nil {
				return nil, err
			}
			t.Elems = append(t.Elems, e)
			p.skipSpace()
			if p.i < len(p.s) && p.s[p.i] == ',' {
				p.i++
				continue
			}
			break
		}
		if err := p.expect('>'); err != nil {
			return nil, err
		}
	case "":
		return nil, fmt.Errorf("invalid type %q", p.s)
	default:
		return nil, fmt.Errorf("unknown type %s", name)
	}
	if nested {
		t.freeze()
	}
	for _, e := range t.Elems {
		if e.Kind == Counter {
			return nil, errors.New("counters are not allowed inside collections or tuples")
		}
	}
	return t, nil
}

// freeze marks t and every collection inside it frozen.
func (t *Type) freeze() {
	t.Frozen = t.IsCollection() || t.Kind == Tuple || t.Kind == UDT || t.Kind == Vector
	for _, e := range t.Elems {
		e.freeze()
	}
}

// DataType returns the native protocol type.
func (t *Type) DataType() datatype.DataType {
	switch t.Kind {
	case Ascii:
		return datatype.Ascii
	case Bigint:
		return datatype.Bigint
	case Blob:
		return datatype.Blob
	case Boolean:
		return datatype.Boolean
	case Counter:
		return datatype.Counter
	case Date:
		return datatype.Date
	case Decimal:
		return datatype.Decimal
	case Double:
		return datatype.Double
	case Duration:
		return datatype.Duration
	case Float:
		return datatype.Float
	case Inet:
		return datatype.Inet
	case Int:
		return datatype.Int
	case Smallint:
		return datatype.Smallint
	case Text:
		return datatype.Varchar
	case Time:
		return datatype.Time
	case Timestamp:
		return datatype.Timestamp
	case Timeuuid:
		return datatype.Timeuuid
	case Tinyint:
		return datatype.Tinyint
	case UUID:
		return datatype.Uuid
	case Varint:
		return datatype.Varint
	case List:
		return datatype.NewList(t.Elems[0].DataType())
	case Set:
		return datatype.NewSet(t.Elems[0].DataType())
	case Map:
		return datatype.NewMap(t.Elems[0].DataType(), t.Elems[1].DataType())
	case Tuple:
		dts := make([]datatype.DataType, len(t.Elems))
		for i, e := range t.Elems {
			dts[i] = e.DataType()
		}
		return datatype.NewTuple(dts...)
	case UDT:
		dts := make([]datatype.DataType, len(t.Elems))
		for i, e := range t.Elems {
			dts[i] = e.DataType()
		}
		dt, _ := datatype.NewUserDefined(t.Keyspace, t.Name, t.Fields, dts)
		return dt
	case Vector:
		return datatype.NewCustom(t.MarshalClass())
	}
	panic(fmt.Sprintf("types: unknown kind %d", t.Kind))
}

// fixedSize returns the serialized size of fixed-size types, or -1.
func (t *Type) fixedSize() int {
	switch t.Kind {
	case Boolean, Tinyint:
		return 1
	case Smallint:
		return 2
	case Int, Float, Date:
		return 4
	case Bigint, Counter, Double, Time, Timestamp:
		return 8
	case UUID, Timeuuid:
		return 16
	}
	return -1
}

// Validate checks that v is a valid serialized value of t, as each type's
// TypeSerializer.validate. An empty value is valid for every type except
// collections, as in Cassandra (empty values of fixed-size types are the
// legacy "empty" value).
func (t *Type) Validate(v []byte) error {
	if len(v) == 0 && !t.IsCollection() {
		return nil
	}
	if n := t.fixedSize(); n > 0 {
		if len(v) != n {
			return fmt.Errorf("expected %d bytes for a %s value, got %d", n, t, len(v))
		}
		switch t.Kind {
		case Timeuuid:
			if v[6]>>4 != 1 {
				return errors.New("unsupported UUID version for timeuuid")
			}
		case Time:
			if ns := int64(binary.BigEndian.Uint64(v)); ns < 0 || ns >= 24*3600*1e9 {
				return fmt.Errorf("invalid time value %d", ns)
			}
		}
		return nil
	}
	switch t.Kind {
	case Ascii:
		for _, c := range v {
			if c >= 0x80 {
				return errors.New("invalid ASCII bytes")
			}
		}
	case Text:
		if !utf8.Valid(v) {
			return errors.New("invalid UTF-8 bytes")
		}
	case Inet:
		if len(v) != 4 && len(v) != 16 {
			return fmt.Errorf("invalid inet value of %d bytes", len(v))
		}
	case Decimal:
		if len(v) < 4 {
			return errors.New("invalid decimal value")
		}
	case Duration:
		if _, _, _, err := decodeDuration(v); err != nil {
			return err
		}
	case List, Set:
		elems, err := SplitCollection(v, 1)
		if err != nil {
			return err
		}
		for _, e := range elems {
			if e == nil {
				return errors.New("null is not supported inside collections")
			}
			if err := t.Elems[0].Validate(e); err != nil {
				return err
			}
		}
	case Map:
		elems, err := SplitCollection(v, 2)
		if err != nil {
			return err
		}
		for i, e := range elems {
			if e == nil {
				return errors.New("null is not supported inside collections")
			}
			if err := t.Elems[i%2].Validate(e); err != nil {
				return err
			}
		}
	case Vector:
		elems, err := SplitVector(t, v)
		if err != nil {
			return err
		}
		for _, e := range elems {
			if err := t.Elems[0].Validate(e); err != nil {
				return err
			}
		}
	case Tuple, UDT:
		fields, err := SplitTuple(v)
		if err != nil {
			return err
		}
		if len(fields) > len(t.Elems) {
			return fmt.Errorf("tuple value has %d fields, type has %d", len(fields), len(t.Elems))
		}
		for i, f := range fields {
			if f != nil {
				if err := t.Elems[i].Validate(f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// SplitCollection splits a serialized list or set (perEntry 1) or map
// (perEntry 2, keys and values alternating), in the protocol v3+ format:
// int32 count, then int32-length-prefixed elements.
func SplitCollection(v []byte, perEntry int) ([][]byte, error) {
	if len(v) < 4 {
		return nil, errors.New("invalid collection value")
	}
	n := int(int32(binary.BigEndian.Uint32(v)))
	v = v[4:]
	if n < 0 || n*4*perEntry > len(v) {
		return nil, errors.New("invalid collection value")
	}
	out := make([][]byte, 0, n*perEntry)
	for i := 0; i < n*perEntry; i++ {
		e, rest, err := readValue(v)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
		v = rest
	}
	if len(v) != 0 {
		return nil, errors.New("invalid collection value: trailing bytes")
	}
	return out, nil
}

func readValue(v []byte) ([]byte, []byte, error) {
	if len(v) < 4 {
		return nil, nil, errors.New("invalid value length")
	}
	l := int32(binary.BigEndian.Uint32(v))
	v = v[4:]
	if l < 0 {
		return nil, v, nil
	}
	if int(l) > len(v) {
		return nil, nil, errors.New("invalid value length")
	}
	return v[:l:l], v[l:], nil
}

// SplitTuple splits a serialized tuple; null fields are nil.
func SplitTuple(v []byte) ([][]byte, error) {
	var out [][]byte
	for len(v) > 0 {
		e, rest, err := readValue(v)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
		v = rest
	}
	return out, nil
}

// BuildCollection serializes elements (for a map, keys and values
// alternating) as a list, set or map; count is the number of entries.
func BuildCollection(count int, elems [][]byte) []byte {
	n := 4
	for _, e := range elems {
		n += 4 + len(e)
	}
	out := make([]byte, 0, n)
	out = binary.BigEndian.AppendUint32(out, uint32(count))
	for _, e := range elems {
		out = binary.BigEndian.AppendUint32(out, uint32(len(e)))
		out = append(out, e...)
	}
	return out
}

// BuildTuple serializes tuple fields; nil fields are null.
func BuildTuple(fields [][]byte) []byte {
	var out []byte
	for _, f := range fields {
		if f == nil {
			out = binary.BigEndian.AppendUint32(out, math.MaxUint32)
			continue
		}
		out = binary.BigEndian.AppendUint32(out, uint32(len(f)))
		out = append(out, f...)
	}
	return out
}

// QuoteName quotes an identifier if CQL needs it (ColumnIdentifier.maybeQuote).
func QuoteName(n string) string {
	plain := n != ""
	for i := 0; i < len(n); i++ {
		c := n[i]
		if !(c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
			plain = false
		}
	}
	if plain && !reservedWords[n] {
		return n
	}
	return `"` + strings.ReplaceAll(n, `"`, `""`) + `"`
}

var reservedWords = map[string]bool{
	"add": true, "allow": true, "alter": true, "and": true, "apply": true, "asc": true, "authorize": true,
	"batch": true, "begin": true, "by": true, "columnfamily": true, "create": true, "delete": true, "desc": true,
	"describe": true, "drop": true, "entries": true, "execute": true, "from": true, "full": true, "grant": true,
	"if": true, "in": true, "index": true, "infinity": true, "insert": true, "into": true, "keyspace": true,
	"limit": true, "modify": true, "nan": true, "norecursive": true, "not": true, "null": true, "of": true,
	"on": true, "or": true, "order": true, "primary": true, "rename": true, "revoke": true, "schema": true,
	"select": true, "set": true, "table": true, "to": true, "token": true, "truncate": true, "unlogged": true,
	"update": true, "use": true, "using": true, "where": true, "with": true,
}
