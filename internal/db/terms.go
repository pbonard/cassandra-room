package db

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Terms: a statement's values, prepared against their receivers' types
// (Term.Raw.prepare) and bound to the request's values at execution
// (Term.bind).

type valState uint8

const (
	vSet valState = iota
	vNull
	vUnset
)

type term interface {
	bind(e *execCtx) ([]byte, valState, error)
}

// spec is a bind variable's name and type (ColumnSpecification).
type spec struct {
	ks, table, name string
	typ             *types.Type
}

// vars collects a statement's bind variables while it is prepared.
type vars struct {
	names []string // from the parser: "" for ?
	specs []spec
	ks    string
	table string
}

func newVars(markers []string) *vars {
	return &vars{names: markers, specs: make([]spec, len(markers))}
}

type constTerm struct {
	v  []byte
	st valState
}

func (c *constTerm) bind(*execCtx) ([]byte, valState, error) { return c.v, c.st, nil }

type markerTerm struct {
	idx  int
	typ  *types.Type
	name string
}

func (m *markerTerm) bind(e *execCtx) ([]byte, valState, error) {
	if m.idx >= len(e.values) {
		return nil, vNull, invalidf("missing value for bind marker %d", m.idx)
	}
	v := e.values[m.idx]
	switch {
	case v == nil || v.Type == primitive.ValueTypeNull:
		return nil, vNull, nil
	case v.Type == primitive.ValueTypeUnset:
		return nil, vUnset, nil
	}
	if err := m.typ.Validate(v.Contents); err != nil {
		return nil, vSet, invalidf("Exception while binding column %s: %v", m.name, err)
	}
	switch m.typ.Kind {
	case types.Set, types.Map:
		// Sort and deduplicate, as Sets.Value and Maps.Value fromSerialized.
		per := 1
		if m.typ.Kind == types.Map {
			per = 2
		}
		elems, _ := types.SplitCollection(v.Contents, per)
		return buildCollection(m.typ, elems), vSet, nil
	}
	if v.Contents == nil {
		return []byte{}, vSet, nil
	}
	return v.Contents, vSet, nil
}

type funcTerm struct {
	args []term
	fn   func(e *execCtx, args [][]byte) ([]byte, error)
}

func (f *funcTerm) bind(e *execCtx) ([]byte, valState, error) {
	args := make([][]byte, len(f.args))
	for i, a := range f.args {
		v, st, err := a.bind(e)
		if err != nil {
			return nil, vSet, err
		}
		if st != vSet {
			return nil, vNull, nil // functions here return null on null input
		}
		args[i] = v
	}
	v, err := f.fn(e, args)
	if v == nil && err == nil {
		return nil, vNull, nil
	}
	return v, vSet, err
}

// collTerm builds a frozen collection or tuple from element terms.
type collTerm struct {
	typ   *types.Type
	elems []term // map: keys and values alternating
}

func (c *collTerm) bind(e *execCtx) ([]byte, valState, error) {
	vals := make([][]byte, len(c.elems))
	for i, el := range c.elems {
		v, st, err := el.bind(e)
		if err != nil {
			return nil, vSet, err
		}
		switch {
		case st == vUnset:
			return nil, vSet, invalidf("Invalid unset value in %s literal", c.typ)
		case st == vNull && c.typ.Kind != types.Tuple && c.typ.Kind != types.UDT:
			return nil, vSet, invalidf("null is not supported inside collections")
		case st == vNull:
			v = nil
		}
		vals[i] = v
	}
	return buildCollection(c.typ, vals), vSet, nil
}

// buildCollection serializes a collection or tuple value; set elements and
// map entries are sorted by the element (key) type and deduplicated, the
// last duplicate winning, as Sets.Value and Maps.Value do.
func buildCollection(t *types.Type, vals [][]byte) []byte {
	switch t.Kind {
	case types.Tuple, types.UDT:
		return types.BuildTuple(vals)
	case types.List:
		return types.BuildCollection(len(vals), vals)
	case types.Set:
		slices.SortStableFunc(vals, func(a, b []byte) int { return types.Compare(t.Elems[0], a, b) })
		vals = slices.CompactFunc(vals, func(a, b []byte) bool { return types.Compare(t.Elems[0], a, b) == 0 })
		return types.BuildCollection(len(vals), vals)
	case types.Map:
		type kv struct{ k, v []byte }
		entries := make([]kv, len(vals)/2)
		for i := range entries {
			entries[i] = kv{vals[2*i], vals[2*i+1]}
		}
		slices.SortStableFunc(entries, func(a, b kv) int { return types.Compare(t.Elems[0], a.k, b.k) })
		var out [][]byte
		for i, e := range entries {
			if i+1 < len(entries) && types.Compare(t.Elems[0], e.k, entries[i+1].k) == 0 {
				continue
			}
			out = append(out, e.k, e.v)
		}
		return types.BuildCollection(len(out)/2, out)
	}
	panic("not a collection")
}

// prepareTerm prepares a term for a receiver of type typ named name.
func (vs *vars) prepareTerm(raw cql.Term, name string, typ *types.Type) (term, error) {
	switch r := raw.(type) {
	case *cql.Literal:
		if typ.IsCollection() || typ.Kind == types.Tuple {
			return nil, invalidf("Invalid %s constant (%s) for \"%s\" of type %s", r.Kind, r.Text, name, typ)
		}
		v, err := types.FromLiteral(typ, r.Kind, r.Text)
		if err != nil {
			if strings.HasPrefix(err.Error(), "invalid ") {
				return nil, invalidf("Invalid %s constant (%s) for \"%s\" of type %s", r.Kind, r.Text, name, typ)
			}
			return nil, invalidf("%v", err)
		}
		return &constTerm{v: v}, nil
	case *cql.Null:
		return &constTerm{st: vNull}, nil
	case *cql.Marker:
		n := vs.names[r.Index]
		if n == "" {
			n = name
		}
		vs.specs[r.Index] = spec{ks: vs.ks, table: vs.table, name: n, typ: typ}
		return &markerTerm{idx: r.Index, typ: typ, name: name}, nil
	case *cql.Cast:
		return vs.prepareTerm(r.Term, name, typ)
	case *cql.ListLiteral:
		if typ.Kind != types.List {
			if typ.Kind == types.Set && len(r.Elems) == 0 {
				return &constTerm{v: types.BuildCollection(0, nil)}, nil
			}
			return nil, invalidf("Invalid list literal for %s of type %s", name, typ)
		}
		return vs.prepareElems(r.Elems, typ, func(int) *types.Type { return typ.Elems[0] }, name)
	case *cql.SetLiteral:
		if typ.Kind != types.Set {
			return nil, invalidf("Invalid set literal for %s of type %s", name, typ)
		}
		return vs.prepareElems(r.Elems, typ, func(int) *types.Type { return typ.Elems[0] }, name)
	case *cql.MapLiteral:
		if len(r.Keys) == 0 && (typ.Kind == types.Set || typ.Kind == types.List) {
			return &constTerm{v: types.BuildCollection(0, nil)}, nil
		}
		if typ.Kind != types.Map {
			return nil, invalidf("Invalid map literal for %s of type %s", name, typ)
		}
		var elems []cql.Term
		for i := range r.Keys {
			elems = append(elems, r.Keys[i], r.Values[i])
		}
		return vs.prepareElems(elems, typ, func(i int) *types.Type { return typ.Elems[i%2] }, name)
	case *cql.UDTLiteral:
		if typ.Kind != types.UDT {
			return nil, invalidf("Invalid user type literal for %s of type %s", name, typ)
		}
		elems := make([]cql.Term, len(typ.Elems))
		for i := range elems {
			elems[i] = &cql.Null{}
		}
		for i, f := range r.Fields {
			j := slices.Index(typ.Fields, f)
			if j < 0 {
				return nil, invalidf("Unknown field '%s' in value of user defined type %s", f, typ.Name)
			}
			elems[j] = r.Values[i]
		}
		return vs.prepareElems(elems, typ, func(i int) *types.Type { return typ.Elems[i] }, name)
	case *cql.TupleLiteral:
		if typ.Kind != types.Tuple {
			return nil, invalidf("Invalid tuple literal for %s of type %s", name, typ)
		}
		if len(r.Elems) > len(typ.Elems) {
			return nil, invalidf("Invalid tuple literal for %s: too many elements. Type %s expects %d but got %d", name, typ, len(typ.Elems), len(r.Elems))
		}
		return vs.prepareElems(r.Elems, typ, func(i int) *types.Type { return typ.Elems[i] }, name)
	case *cql.FunctionCall:
		return vs.prepareFunction(r, name, typ)
	}
	return nil, invalidf("unsupported term %T", raw)
}

func (vs *vars) prepareElems(raw []cql.Term, typ *types.Type, elemType func(int) *types.Type, name string) (term, error) {
	c := &collTerm{typ: typ}
	allConst := true
	for i, r := range raw {
		et, err := vs.prepareTerm(r, name, elemType(i))
		if err != nil {
			return nil, err
		}
		if _, ok := et.(*constTerm); !ok {
			allConst = false
		}
		c.elems = append(c.elems, et)
	}
	if allConst {
		v, st, err := c.bind(nil)
		if err != nil {
			return nil, err
		}
		return &constTerm{v: v, st: st}, nil
	}
	return c, nil
}

// prepareFunction supports Cassandra's native scalar functions that make
// sense in a write or a restriction: the time functions and the blob
// conversions (TimeFcts, UuidFcts, BytesConversionFcts).
func (vs *vars) prepareFunction(f *cql.FunctionCall, name string, typ *types.Type) (term, error) {
	fname := strings.TrimPrefix(strings.ToLower(f.Name), "system.")
	arg := func(t *types.Type) ([]term, error) {
		if len(f.Args) != 1 {
			return nil, invalidf("Invalid number of arguments in call to function %s: 1 required but %d provided", fname, len(f.Args))
		}
		a, err := vs.prepareTerm(f.Args[0], name, t)
		return []term{a}, err
	}
	noArgs := func() error {
		if len(f.Args) != 0 {
			return invalidf("Invalid number of arguments in call to function %s: 0 required but %d provided", fname, len(f.Args))
		}
		return nil
	}
	returns := func(rt *types.Type) error {
		if rt.Kind != typ.Kind && !(typ.Kind == types.Blob && rt.Kind != types.Blob) {
			return invalidf("Type error: cannot assign result of function system.%s (type %s) to %s (type %s)", fname, rt, name, typ)
		}
		return nil
	}
	switch fname {
	case "now", "currenttimeuuid":
		if err := noArgs(); err != nil {
			return nil, err
		}
		if err := returns(types.TimeuuidType); err != nil {
			return nil, err
		}
		return &funcTerm{fn: func(e *execCtx, _ [][]byte) ([]byte, error) { return newTimeUUID(e.now), nil }}, nil
	case "uuid":
		if err := noArgs(); err != nil {
			return nil, err
		}
		if err := returns(types.UUIDType); err != nil {
			return nil, err
		}
		return &funcTerm{fn: func(*execCtx, [][]byte) ([]byte, error) { return newRandomUUID(), nil }}, nil
	case "currenttimestamp":
		if err := noArgs(); err != nil {
			return nil, err
		}
		if err := returns(types.TimestampType); err != nil {
			return nil, err
		}
		return &funcTerm{fn: func(e *execCtx, _ [][]byte) ([]byte, error) {
			return binary.BigEndian.AppendUint64(nil, uint64(e.now.UnixMilli())), nil
		}}, nil
	case "currentdate":
		if err := noArgs(); err != nil {
			return nil, err
		}
		if err := returns(types.DateType); err != nil {
			return nil, err
		}
		return &funcTerm{fn: func(e *execCtx, _ [][]byte) ([]byte, error) {
			return dateOf(e.now.UnixMilli()), nil
		}}, nil
	case "currenttime":
		if err := noArgs(); err != nil {
			return nil, err
		}
		if err := returns(types.TimeType); err != nil {
			return nil, err
		}
		return &funcTerm{fn: func(e *execCtx, _ [][]byte) ([]byte, error) {
			u := e.now.UTC()
			ns := int64(u.Hour())*3600e9 + int64(u.Minute())*60e9 + int64(u.Second())*1e9 + int64(u.Nanosecond())
			return binary.BigEndian.AppendUint64(nil, uint64(ns)), nil
		}}, nil
	case "totimestamp", "todate", "tounixtimestamp":
		rt := map[string]*types.Type{"totimestamp": types.TimestampType, "todate": types.DateType, "tounixtimestamp": types.BigintType}[fname]
		if err := returns(rt); err != nil {
			return nil, err
		}
		// The argument is a timeuuid, date or timestamp; prepare it as a
		// timeuuid unless it is a literal of another type.
		at := types.TimeuuidType
		if l, ok := f.Args[0].(*cql.Literal); ok && l.Kind != types.LitUUID {
			at = types.TimestampType
		}
		args, err := arg(at)
		if err != nil {
			return nil, err
		}
		return &funcTerm{args: args, fn: func(_ *execCtx, a [][]byte) ([]byte, error) {
			ms := millisOf(at, a[0])
			switch fname {
			case "totimestamp":
				return binary.BigEndian.AppendUint64(nil, uint64(ms)), nil
			case "todate":
				return dateOf(ms), nil
			}
			return binary.BigEndian.AppendUint64(nil, uint64(ms)), nil
		}}, nil
	case "mintimeuuid", "maxtimeuuid":
		if err := returns(types.TimeuuidType); err != nil {
			return nil, err
		}
		args, err := arg(types.TimestampType)
		if err != nil {
			return nil, err
		}
		return &funcTerm{args: args, fn: func(_ *execCtx, a [][]byte) ([]byte, error) {
			ms := int64(binary.BigEndian.Uint64(a[0]))
			return boundTimeUUID(ms, fname == "maxtimeuuid"), nil
		}}, nil
	}
	if fname == "fromjson" {
		if len(f.Args) != 1 {
			return nil, invalidf("fromJson() takes exactly 1 argument")
		}
		a, err := vs.prepareTerm(f.Args[0], name, types.TextType)
		if err != nil {
			return nil, err
		}
		return &funcTerm{args: []term{a}, fn: func(_ *execCtx, args [][]byte) ([]byte, error) {
			parsed, err := types.ParseJSON(string(args[0]))
			if err != nil {
				return nil, invalidf("Could not decode JSON string '%s': %v", args[0], err)
			}
			v, err := types.FromJSON(typ, parsed)
			if err != nil {
				return nil, invalidf("%v", err)
			}
			return v, nil
		}}, nil
	}
	if src, ok := strings.CutSuffix(fname, "asblob"); ok {
		st, err := types.Parse(src)
		if err != nil {
			return nil, invalidf("Unknown function %s called", f.Name)
		}
		if err := returns(types.BlobType); err != nil {
			return nil, err
		}
		args, err := arg(st)
		if err != nil {
			return nil, err
		}
		return &funcTerm{args: args, fn: func(_ *execCtx, a [][]byte) ([]byte, error) { return a[0], nil }}, nil
	}
	if dst, ok := strings.CutPrefix(fname, "blobas"); ok {
		dt, err := types.Parse(dst)
		if err != nil {
			return nil, invalidf("Unknown function %s called", f.Name)
		}
		if err := returns(dt); err != nil {
			return nil, err
		}
		args, err := arg(types.BlobType)
		if err != nil {
			return nil, err
		}
		return &funcTerm{args: args, fn: func(_ *execCtx, a [][]byte) ([]byte, error) {
			if err := dt.Validate(a[0]); err != nil {
				return nil, invalidf("In call to function system.blobas%s, value 0x%x is not a valid binary representation for type %s", dst, a[0], dt)
			}
			return a[0], nil
		}}, nil
	}
	return nil, invalidf("Unknown function %s called", f.Name)
}

func millisOf(t *types.Type, v []byte) int64 {
	switch t.Kind {
	case types.Timeuuid:
		return uuidMillis(v)
	case types.Date:
		return (int64(binary.BigEndian.Uint32(v)) - 1<<31) * 86400000
	}
	return int64(binary.BigEndian.Uint64(v))
}

func dateOf(ms int64) []byte {
	days := ms / 86400000
	if ms < 0 && ms%86400000 != 0 {
		days--
	}
	return binary.BigEndian.AppendUint32(nil, uint32(days+1<<31))
}

// Time UUIDs (TimeUUID): 100 ns intervals since 1582-10-15.
const uuidEpochOffset = 0x01B21DD213814000

var timeUUIDState struct {
	sync.Mutex
	last  int64
	clock uint16
	node  [6]byte
	init  bool
}

func newTimeUUID(now time.Time) []byte {
	s := &timeUUIDState
	s.Lock()
	if !s.init {
		var b [8]byte
		_, _ = rand.Read(b[:])
		s.clock = binary.BigEndian.Uint16(b[:2]) & 0x3fff
		copy(s.node[:], b[2:])
		s.node[0] |= 1 // multicast bit: a random node id
		s.init = true
	}
	ts := now.UnixNano()/100 + uuidEpochOffset
	if ts <= s.last {
		ts = s.last + 1
	}
	s.last = ts
	clock, node := s.clock, s.node
	s.Unlock()
	return makeTimeUUID(ts, clock, node[:])
}

func makeTimeUUID(ts int64, clock uint16, node []byte) []byte {
	u := make([]byte, 16)
	binary.BigEndian.PutUint32(u[0:], uint32(ts))
	binary.BigEndian.PutUint16(u[4:], uint16(ts>>32))
	binary.BigEndian.PutUint16(u[6:], uint16(ts>>48)&0x0fff|0x1000)
	binary.BigEndian.PutUint16(u[8:], clock&0x3fff|0x8000)
	copy(u[10:], node)
	return u
}

// boundTimeUUID is minTimeuuid/maxTimeuuid: the smallest or largest time
// UUID of a millisecond, in timeuuid order (lsb compared as signed bytes).
func boundTimeUUID(ms int64, max bool) []byte {
	ts := ms*10000 + uuidEpochOffset
	if max {
		ts += 9999
		u := makeTimeUUID(ts, 0x3f7f, []byte{0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f})
		u[8] = 0xbf
		return u
	}
	u := makeTimeUUID(ts, 0, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80})
	u[8], u[9] = 0x80, 0x80
	return u
}

func uuidMillis(u []byte) int64 {
	ts := int64(binary.BigEndian.Uint32(u[0:])) | int64(binary.BigEndian.Uint16(u[4:]))<<32 | int64(binary.BigEndian.Uint16(u[6:])&0x0fff)<<48
	return (ts - uuidEpochOffset) / 10000
}

func newRandomUUID() []byte {
	u := make([]byte, 16)
	_, _ = rand.Read(u)
	u[6] = u[6]&0x0f | 0x40
	u[8] = u[8]&0x3f | 0x80
	return u
}

func formatUUID(u []byte) string { return types.FormatUUID(u) }

// intValue binds a LIMIT, TTL or TIMESTAMP term.
func bindInt(e *execCtx, t term, what string) (int64, bool, error) {
	if t == nil {
		return 0, false, nil
	}
	v, st, err := t.bind(e)
	if err != nil {
		return 0, false, err
	}
	switch st {
	case vNull:
		return 0, false, invalidf("Invalid null value of %s", what)
	case vUnset:
		return 0, false, nil
	}
	switch len(v) {
	case 4:
		return int64(int32(binary.BigEndian.Uint32(v))), true, nil
	case 8:
		return int64(binary.BigEndian.Uint64(v)), true, nil
	}
	return 0, false, invalidf("Invalid value of %s", what)
}

// maxTTL is Attributes.MAX_TTL: 20 years.
const maxTTL = 20 * 365 * 24 * 60 * 60

var _ = math.MaxInt64
var _ = fmt.Sprint
