package db

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
)

// WHERE clause analysis (StatementRestrictions): restrictions on the
// partition key select partitions, restrictions on a clustering prefix
// select row ranges, and everything else is filtering.

// valuesTerm is an EQ or IN restriction's values.
type valuesTerm struct {
	col  *Column
	eq   term
	in   []term
	list term // IN ?
}

func (v *valuesTerm) values(e *execCtx) ([][]byte, error) {
	check := func(b []byte, st valState) ([]byte, error) {
		switch st {
		case vNull:
			return nil, invalidf("Invalid null value in condition for column %s", v.col.Name)
		case vUnset:
			return nil, invalidf("Invalid unset value for column %s", v.col.Name)
		}
		return b, nil
	}
	switch {
	case v.eq != nil:
		b, st, err := v.eq.bind(e)
		if err != nil {
			return nil, err
		}
		b, err = check(b, st)
		return [][]byte{b}, err
	case v.list != nil:
		b, st, err := v.list.bind(e)
		if err != nil {
			return nil, err
		}
		if _, err := check(b, st); err != nil {
			return nil, err
		}
		elems, err := types.SplitCollection(b, 1)
		if err != nil {
			return nil, invalidf("Invalid IN value for column %s", v.col.Name)
		}
		return elems, nil
	}
	out := make([][]byte, 0, len(v.in))
	for _, t := range v.in {
		b, st, err := t.bind(e)
		if err != nil {
			return nil, err
		}
		if b, err = check(b, st); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// bound is one side of a slice.
type bound struct {
	t    term
	incl bool
}

// slice restricts clustering columns cols (one, or several for a
// multi-column relation, whose terms are tuples) to a range.
type slice struct {
	cols         []*Column
	multi        bool
	lower, upper *bound
}

// filter is a restriction evaluated on each row (ALLOW FILTERING).
type filter struct {
	col *Column
	op  string
	t   term
	in  *valuesTerm
	// tuple is a multi-column slice on clustering columns of mixed order,
	// evaluated on each row of the range of its first column (not
	// filtering in Cassandra's sense: no ALLOW FILTERING needed).
	tuple *slice
}

type where struct {
	pk      []*valuesTerm // one per partition key column, nil if unrestricted
	ck      []*valuesTerm // EQ/IN on a clustering prefix
	slice   *slice
	token   *slice // bounds on token(partition key): bigint terms
	filters []filter
}

func (w *where) pkComplete() bool {
	for _, v := range w.pk {
		if v == nil {
			return false
		}
	}
	return true
}

func (w *where) pkAny() bool {
	return slices.ContainsFunc(w.pk, func(v *valuesTerm) bool { return v != nil })
}

// analyzeWhere prepares a WHERE clause. With allowFiltering, restrictions
// that cannot select partitions or row ranges become filters; without it
// they are errors, unless needsFiltering is used by the caller to report
// the ALLOW FILTERING error.
func analyzeWhere(t *Table, rels []cql.Relation, vs *vars) (*where, error) {
	w := &where{pk: make([]*valuesTerm, len(t.PartitionKey))}
	ckEQ := make([]*valuesTerm, len(t.Clustering))
	ckSlice := make([]*slice, len(t.Clustering))
	var extra []filter // non-prefix clustering restrictions, if any
	for _, r := range rels {
		switch {
		case r.Token:
			if len(r.Columns) != len(t.PartitionKey) || !slices.EqualFunc(r.Columns, t.PartitionKey, func(n string, c *Column) bool { return n == c.Name }) {
				names := make([]string, len(t.PartitionKey))
				for i, c := range t.PartitionKey {
					names[i] = c.Name
				}
				return nil, invalidf("The token function arguments must be in the partition key order: %s", strings.Join(names, ", "))
			}
			tt, err := vs.prepareTerm(r.Value, "partition key token", types.BigintType)
			if err != nil {
				return nil, err
			}
			if w.token == nil {
				w.token = &slice{}
			}
			if err := addBound(w.token, r.Op, tt, "token"); err != nil {
				return nil, err
			}
			continue
		case r.Multi:
			cols := make([]*Column, len(r.Columns))
			for i, n := range r.Columns {
				c := t.byName[n]
				if c == nil {
					return nil, invalidf("Undefined column name %s in table %s.%s", n, t.Keyspace, t.Name)
				}
				if c.Kind != Clustering {
					return nil, invalidf("Multi-column relations can only be applied to clustering columns but was applied to: %s", n)
				}
				if i > 0 && c.Position != cols[i-1].Position+1 {
					return nil, invalidf("Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", relationString(r))
				}
				cols[i] = c
			}
			tupleType := &types.Type{Kind: types.Tuple, Frozen: true}
			for _, c := range cols {
				tupleType.Elems = append(tupleType.Elems, c.Type)
			}
			name := "(" + strings.Join(r.Columns, ",") + ")"
			switch r.Op {
			case "=":
				tt, ok := r.Value.(*cql.TupleLiteral)
				if !ok || len(tt.Elems) != len(cols) {
					// A tuple marker: bind it as a tuple, split per column.
					mt, err := vs.prepareTerm(r.Value, name, tupleType)
					if err != nil {
						return nil, err
					}
					for i, c := range cols {
						if ckEQ[c.Position] != nil {
							return nil, invalidf("%s cannot be restricted by more than one relation if it includes an Equal", c.Name)
						}
						ckEQ[c.Position] = &valuesTerm{col: c, eq: &tupleField{t: mt, i: i}}
					}
					continue
				}
				for i, c := range cols {
					et, err := vs.prepareTerm(tt.Elems[i], c.Name, c.Type)
					if err != nil {
						return nil, err
					}
					if ckEQ[c.Position] != nil {
						return nil, invalidf("%s cannot be restricted by more than one relation if it includes an Equal", c.Name)
					}
					ckEQ[c.Position] = &valuesTerm{col: c, eq: et}
				}
			case "<", "<=", ">", ">=":
				tt, err := vs.prepareTerm(r.Value, name, tupleType)
				if err != nil {
					return nil, err
				}
				s := ckSlice[cols[0].Position]
				if s == nil {
					s = &slice{cols: cols, multi: true}
					ckSlice[cols[0].Position] = s
				} else if !s.multi || len(s.cols) != len(cols) {
					return nil, invalidf("Column \"%s\" cannot be restricted by both a tuple notation inequality and a single column inequality (%s)", cols[0].Name, relationString(r))
				}
				if err := addBound(s, r.Op, tt, name); err != nil {
					return nil, err
				}
			default:
				return nil, invalidf("Multi-column relations with operator %s are not supported", r.Op)
			}
			continue
		}
		name := r.Columns[0]
		c := t.byName[name]
		if c == nil {
			return nil, invalidf("Undefined column name %s in table %s.%s", name, t.Keyspace, t.Name)
		}
		switch r.Op {
		case "=", "IN":
			vt, err := prepareValues(vs, c, r)
			if err != nil {
				return nil, err
			}
			switch c.Kind {
			case PartitionKey:
				if w.pk[c.Position] != nil {
					return nil, invalidf("%s cannot be restricted by more than one relation if it includes an Equal", c.Name)
				}
				w.pk[c.Position] = vt
			case Clustering:
				if ckEQ[c.Position] != nil || ckSlice[c.Position] != nil {
					return nil, invalidf("%s cannot be restricted by more than one relation if it includes an Equal", c.Name)
				}
				ckEQ[c.Position] = vt
			default:
				w.filters = append(w.filters, filter{col: c, op: r.Op, in: vt})
			}
		case "<", "<=", ">", ">=":
			tt, err := vs.prepareTerm(r.Value, c.Name, c.Type)
			if err != nil {
				return nil, err
			}
			switch c.Kind {
			case PartitionKey:
				return nil, invalidf("Only EQ and IN relation are supported on the partition key (unless you use the token() function or allow filtering)")
			case Clustering:
				if ckEQ[c.Position] != nil {
					return nil, invalidf("%s cannot be restricted by more than one relation if it includes an Equal", c.Name)
				}
				s := ckSlice[c.Position]
				if s == nil {
					s = &slice{cols: []*Column{c}}
					ckSlice[c.Position] = s
				} else if s.multi {
					return nil, invalidf("Column \"%s\" cannot be restricted by both a tuple notation inequality and a single column inequality (%s)", c.Name, relationString(r))
				}
				if err := addBound(s, r.Op, tt, c.Name); err != nil {
					return nil, err
				}
			default:
				w.filters = append(w.filters, filter{col: c, op: r.Op, t: tt})
			}
		case "!=":
			tt, err := vs.prepareTerm(r.Value, c.Name, c.Type)
			if err != nil {
				return nil, err
			}
			if c.Kind == PartitionKey || c.Kind == Clustering {
				return nil, invalidf("Unsupported \"!=\" relation: %s", relationString(r))
			}
			w.filters = append(w.filters, filter{col: c, op: r.Op, t: tt})
		case "CONTAINS", "CONTAINS KEY":
			if !c.Type.IsCollection() {
				return nil, invalidf("Cannot use %s on non-collection column %s", r.Op, c.Name)
			}
			et := c.Type.Elems[0]
			if c.Type.Kind == types.Map && r.Op == "CONTAINS" {
				et = c.Type.Elems[1]
			}
			if r.Op == "CONTAINS KEY" && c.Type.Kind != types.Map {
				return nil, invalidf("Cannot use CONTAINS KEY on non-map column %s", c.Name)
			}
			tt, err := vs.prepareTerm(r.Value, c.Name, et)
			if err != nil {
				return nil, err
			}
			w.filters = append(w.filters, filter{col: c, op: r.Op, t: tt})
		case "LIKE":
			return nil, invalidf("LIKE restriction is only supported on properly indexed columns. %s is not valid.", relationString(r))
		case "IS NOT NULL":
			return nil, invalidf("Unsupported restriction: %s IS NOT NULL", c.Name)
		}
	}
	if w.token != nil && w.pkAny() {
		return nil, invalidf("Columns \"%s\" cannot be restricted by both a normal relation and a token relation", t.PartitionKey[0].Name)
	}
	// The clustering prefix: EQ/IN restrictions, then at most one slice.
	n := 0
	for n < len(t.Clustering) && ckEQ[n] != nil {
		n++
	}
	w.ck = ckEQ[:n]
	if n < len(t.Clustering) && ckSlice[n] != nil {
		w.slice = ckSlice[n]
		n += len(w.slice.cols)
		if s := w.slice; s.multi && slices.ContainsFunc(s.cols, func(c *Column) bool { return c.Desc != s.cols[0].Desc }) {
			// Mixed orders: select the range of the first column and
			// compare whole tuples on each row.
			first := &slice{cols: s.cols[:1]}
			if s.lower != nil {
				first.lower = &bound{t: &tupleField{t: s.lower.t}, incl: true}
			}
			if s.upper != nil {
				first.upper = &bound{t: &tupleField{t: s.upper.t}, incl: true}
			}
			w.slice = first
			w.filters = append(w.filters, filter{col: s.cols[0], op: "tuple", tuple: s})
		}
	}
	// Anything after the prefix is filtering.
	for i := n; i < len(t.Clustering); i++ {
		c := t.Clustering[i]
		if ckEQ[i] != nil {
			extra = append(extra, filter{col: c, op: "IN", in: ckEQ[i]})
		}
		if s := ckSlice[i]; s != nil {
			if s.multi {
				return nil, invalidf("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted", c.Name, t.Clustering[n].Name)
			}
			if s.lower != nil {
				extra = append(extra, filter{col: c, op: map[bool]string{true: ">=", false: ">"}[s.lower.incl], t: s.lower.t})
			}
			if s.upper != nil {
				extra = append(extra, filter{col: c, op: map[bool]string{true: "<=", false: "<"}[s.upper.incl], t: s.upper.t})
			}
		}
	}
	if len(extra) > 0 {
		w.filters = append(extra, w.filters...)
	}
	return w, nil
}

// tupleField binds one field of a tuple term.
type tupleField struct {
	t term
	i int
}

func (f *tupleField) bind(e *execCtx) ([]byte, valState, error) {
	b, st, err := f.t.bind(e)
	if err != nil || st != vSet {
		return nil, st, err
	}
	fields, err := types.SplitTuple(b)
	if err != nil || f.i >= len(fields) {
		return nil, vSet, invalidf("Invalid tuple value")
	}
	if fields[f.i] == nil {
		return nil, vNull, nil
	}
	return fields[f.i], vSet, nil
}

func prepareValues(vs *vars, c *Column, r cql.Relation) (*valuesTerm, error) {
	vt := &valuesTerm{col: c}
	if r.Op == "=" {
		t, err := vs.prepareTerm(r.Value, c.Name, c.Type)
		vt.eq = t
		return vt, err
	}
	if l, ok := r.Value.(*cql.ListLiteral); ok {
		for _, el := range l.Elems {
			t, err := vs.prepareTerm(el, c.Name, c.Type)
			if err != nil {
				return nil, err
			}
			vt.in = append(vt.in, t)
		}
		return vt, nil
	}
	t, err := vs.prepareTerm(r.Value, "in("+c.Name+")", types.ListOf(c.Type, true))
	vt.list = t
	return vt, err
}

func addBound(s *slice, op string, t term, name string) error {
	b := &bound{t: t, incl: op == "<=" || op == ">=" || op == "="}
	switch op {
	case ">", ">=", "=":
		if s.lower != nil {
			return invalidf("More than one restriction was found for the start bound on %s", name)
		}
		s.lower = b
		if op != "=" {
			return nil
		}
		fallthrough
	case "<", "<=":
		if s.upper != nil {
			return invalidf("More than one restriction was found for the end bound on %s", name)
		}
		s.upper = &bound{t: t, incl: b.incl}
	}
	return nil
}

func relationString(r cql.Relation) string {
	cols := strings.Join(r.Columns, ", ")
	if r.Multi {
		cols = "(" + cols + ")"
	}
	return cols + " " + r.Op + " <value>"
}

// partitionKeys returns the serialized partition keys selected by EQ/IN
// restrictions on every partition key column (the cartesian product),
// sorted and deduplicated.
func (w *where) partitionKeys(e *execCtx, t *Table) ([][]byte, error) {
	combos := [][][]byte{nil}
	for _, vt := range w.pk {
		vals, err := vt.values(e)
		if err != nil {
			return nil, err
		}
		var next [][][]byte
		for _, c := range combos {
			for _, v := range vals {
				next = append(next, append(slices.Clip(c), v))
			}
		}
		combos = next
	}
	out := make([][]byte, 0, len(combos))
	for _, c := range combos {
		for i, v := range c {
			if len(v) > 0xffff {
				return nil, invalidf("Key length of %d is longer than maximum of 65535", len(v))
			}
			if len(v) == 0 && len(c) == 1 {
				return nil, invalidf("Key may not be empty")
			}
			_ = i
		}
		out = append(out, serializePartitionKey(c))
	}
	return out, nil
}

// keyRange is a range of keys within a partition, as suffixes after the
// partition prefix: [start, end), or the single key start if point.
type keyRange struct {
	start, end []byte
	point      bool
}

// clusteringRanges returns the row ranges selected by the clustering
// prefix restrictions, in storage order.
func (w *where) clusteringRanges(e *execCtx, t *Table) ([]keyRange, error) {
	prefixes := [][]byte{nil}
	for _, vt := range w.ck {
		vals, err := vt.values(e)
		if err != nil {
			return nil, err
		}
		var next [][]byte
		for _, p := range prefixes {
			for _, v := range vals {
				if len(v) > 0xffff {
					return nil, invalidf("Clustering key length %d is longer than maximum of 65535", len(v))
				}
				np, err := types.AppendComponent(slices.Clip(p), vt.col.Type, v, vt.col.Desc)
				if err != nil {
					return nil, invalidf("%v", err)
				}
				next = append(next, np)
			}
		}
		prefixes = next
	}
	slices.SortFunc(prefixes, bytes.Compare)
	prefixes = slices.CompactFunc(prefixes, bytes.Equal)
	out := make([]keyRange, 0, len(prefixes))
	full := len(w.ck) == len(t.Clustering)
	for _, p := range prefixes {
		switch {
		case full:
			out = append(out, keyRange{start: append(p, types.Terminator), point: true})
		case w.slice == nil:
			out = append(out, keyRange{start: append(slices.Clip(p), types.LTNext), end: append(slices.Clip(p), types.GTNext)})
		default:
			r, empty, err := w.slice.keyRange(e, p)
			if err != nil {
				return nil, err
			}
			if !empty {
				out = append(out, r)
			}
		}
	}
	return out, nil
}

// keyRange computes a slice's range after the prefix p.
func (s *slice) keyRange(e *execCtx, p []byte) (keyRange, bool, error) {
	r := keyRange{start: append(slices.Clip(p), types.LTNext), end: append(slices.Clip(p), types.GTNext)}
	desc := s.cols[0].Desc
	for _, c := range s.cols[1:] {
		if c.Desc != desc {
			return r, false, invalidf("Multi-column slices on clustering columns of mixed order are not supported")
		}
	}
	encode := func(b *bound) ([]byte, error) {
		v, st, err := b.t.bind(e)
		if err != nil {
			return nil, err
		}
		switch st {
		case vNull:
			return nil, invalidf("Invalid null value in condition for column %s", s.cols[0].Name)
		case vUnset:
			return nil, invalidf("Invalid unset value for column %s", s.cols[0].Name)
		}
		out := slices.Clip(p)
		if !s.multi {
			return types.AppendComponent(out, s.cols[0].Type, v, s.cols[0].Desc)
		}
		fields, err := types.SplitTuple(v)
		if err != nil {
			return nil, invalidf("Invalid tuple value")
		}
		for i, f := range fields {
			if f == nil {
				return nil, invalidf("Invalid null value in condition for column %s", s.cols[i].Name)
			}
			if out, err = types.AppendComponent(out, s.cols[i].Type, f, s.cols[i].Desc); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	// In byte order, an ascending column's lower bound is the start; a
	// descending column's lower bound is the end.
	if s.lower != nil {
		k, err := encode(s.lower)
		if err != nil {
			return r, false, err
		}
		if !desc {
			r.start = append(k, map[bool]byte{true: types.LTNext, false: types.GTNext}[s.lower.incl])
		} else {
			r.end = append(k, map[bool]byte{true: types.GTNext, false: types.LTNext}[s.lower.incl])
		}
	}
	if s.upper != nil {
		k, err := encode(s.upper)
		if err != nil {
			return r, false, err
		}
		if !desc {
			r.end = append(k, map[bool]byte{true: types.GTNext, false: types.LTNext}[s.upper.incl])
		} else {
			r.start = append(k, map[bool]byte{true: types.LTNext, false: types.GTNext}[s.upper.incl])
		}
	}
	return r, bytes.Compare(r.start, r.end) >= 0, nil
}

// tokenRange returns the token range [lo, hi] selected by token()
// restrictions; empty if none.
func (w *where) tokenRange(e *execCtx) (lo, hi int64, empty bool, err error) {
	lo, hi = -1<<63, 1<<63-1
	if w.token == nil {
		return lo, hi, false, nil
	}
	get := func(b *bound) (int64, error) {
		v, st, err := b.t.bind(e)
		if err != nil {
			return 0, err
		}
		if st != vSet || len(v) != 8 {
			return 0, invalidf("Invalid null value for token")
		}
		return int64(binary.BigEndian.Uint64(v)), nil
	}
	if b := w.token.lower; b != nil {
		v, err := get(b)
		if err != nil {
			return 0, 0, false, err
		}
		if !b.incl {
			if v == 1<<63-1 {
				return 0, 0, true, nil
			}
			v++
		}
		lo = v
	}
	if b := w.token.upper; b != nil {
		v, err := get(b)
		if err != nil {
			return 0, 0, false, err
		}
		if !b.incl {
			if v == -1<<63 {
				return 0, 0, true, nil
			}
			v--
		}
		hi = v
	}
	return lo, hi, lo > hi, nil
}
