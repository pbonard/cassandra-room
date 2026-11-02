package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"slices"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// selectStmt is a prepared SELECT (SelectStatement).
type selectStmt struct {
	t              *Table
	where          *where
	sels           []selector
	limit          term
	perPartition   term
	reversed       bool
	distinct       bool
	aggregate      bool
	allowFiltering bool
	sortIN         bool // ORDER BY over several partitions: sort the result
	pkIndices      []uint16
	// partitionFilters are partition key restrictions evaluated as filters
	// (a partially restricted partition key with ALLOW FILTERING).
	partitionFilters []filter
}

type selKind uint8

const (
	selColumn selKind = iota
	selWritetime
	selTTL
	selToken
	selTerm
	selCount // count(*) or count(col)
	selMin
	selMax
	selSum
	selAvg
	selCast
)

type selector struct {
	kind  selKind
	name  string
	typ   *types.Type
	col   *Column
	cols  []*Column // token()
	t     term
	inner *selector // cast, aggregates of non-column arguments
}

func (d *DB) prepareSelect(s *Schema, session string, st *cql.Select, vs *vars) (*selectStmt, error) {
	ks, err := keyspaceOf(st.Keyspace, session)
	if err != nil {
		return nil, err
	}
	t, err := s.table(ks, st.Table)
	if err != nil {
		return nil, err
	}
	vs.ks, vs.table = ks, st.Table
	if st.JSON {
		return nil, invalidf("SELECT JSON is not supported")
	}
	if len(st.GroupBy) > 0 {
		return nil, invalidf("GROUP BY is not supported")
	}
	sel := &selectStmt{t: t, distinct: st.Distinct, allowFiltering: st.AllowFiltering}
	if st.Selectors == nil {
		for _, c := range t.AllColumns() {
			sel.sels = append(sel.sels, selector{kind: selColumn, name: c.Name, typ: c.Type, col: c})
		}
	}
	for _, raw := range st.Selectors {
		s, err := prepareSelector(t, raw.Expr, vs)
		if err != nil {
			return nil, err
		}
		if raw.Alias != "" {
			s.name = raw.Alias
		}
		sel.sels = append(sel.sels, s)
	}
	for _, s := range sel.sels {
		if s.kind >= selCount && s.kind <= selAvg {
			sel.aggregate = true
		}
	}
	if sel.distinct {
		for _, s := range sel.sels {
			if s.kind == selColumn && s.col.Kind != PartitionKey && s.col.Kind != Static {
				return nil, invalidf("SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)", s.col.Name)
			}
		}
	}
	if sel.where, err = analyzeWhere(t, st.Where, vs); err != nil {
		return nil, err
	}
	w := sel.where
	if w.pkAny() && !w.pkComplete() {
		if !st.AllowFiltering {
			return nil, filteringError()
		}
		for _, v := range w.pk {
			if v != nil {
				sel.partitionFilters = append(sel.partitionFilters, filter{col: v.col, op: "IN", in: v})
			}
		}
		w.pk = make([]*valuesTerm, len(t.PartitionKey))
	}
	if !st.AllowFiltering {
		if slices.ContainsFunc(w.filters, func(f filter) bool { return f.tuple == nil }) {
			if w.filters[0].col.Kind == Clustering && len(w.filters) > 0 {
				c := w.filters[0].col
				if c.Position > 0 && (len(w.ck) < c.Position) {
					return nil, invalidf("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted", c.Name, t.Clustering[len(w.ck)].Name)
				}
			}
			return nil, filteringError()
		}
		if !w.pkComplete() && (len(w.ck) > 0 || w.slice != nil) {
			return nil, filteringError()
		}
	}
	if st.Limit != nil {
		if sel.limit, err = vs.prepareTerm(st.Limit, "[limit]", types.IntType); err != nil {
			return nil, err
		}
	}
	if st.PerPartitionLimit != nil {
		if sel.perPartition, err = vs.prepareTerm(st.PerPartitionLimit, "[per_partition_limit]", types.IntType); err != nil {
			return nil, err
		}
	}
	if len(st.OrderBy) > 0 {
		if err := sel.prepareOrdering(st.OrderBy); err != nil {
			return nil, err
		}
	}
	sel.pkIndices = pkIndices(t, vs)
	return sel, nil
}

func filteringError() error {
	return invalidf("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING")
}

// prepareOrdering checks ORDER BY: clustering columns in order, all in the
// table's direction or all reversed (SelectStatement.isReversed).
func (s *selectStmt) prepareOrdering(order []cql.Ordering) error {
	t := s.t
	if !s.where.pkComplete() {
		return invalidf("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.")
	}
	reversed := -1
	for i, o := range order {
		c := t.byName[o.Column]
		if c == nil {
			return invalidf("Undefined column name %s in table %s.%s", o.Column, t.Keyspace, t.Name)
		}
		if c.Kind != Clustering {
			return invalidf("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", o.Column)
		}
		if c.Position != i {
			return invalidf("Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY")
		}
		r := 0
		if o.Desc != c.Desc {
			r = 1
		}
		if reversed >= 0 && r != reversed {
			return invalidf("Unsupported order by relation")
		}
		reversed = r
	}
	s.reversed = reversed == 1
	for _, v := range s.where.pk {
		if v.eq == nil {
			s.sortIN = true
		}
	}
	return nil
}

func prepareSelector(t *Table, raw cql.Selectable, vs *vars) (selector, error) {
	switch r := raw.(type) {
	case *cql.ColumnSel:
		c := t.byName[r.Name]
		if c == nil {
			return selector{}, invalidf("Undefined column name %s in table %s.%s", r.Name, t.Keyspace, t.Name)
		}
		return selector{kind: selColumn, name: c.Name, typ: c.Type, col: c}, nil
	case *cql.TermSel:
		lit, ok := r.Term.(*cql.Literal)
		if !ok {
			return selector{}, invalidf("Bind markers and non-literal terms are not supported in selection clauses")
		}
		typ := map[types.LitKind]*types.Type{types.LitString: types.TextType, types.LitInteger: types.IntType,
			types.LitFloat: types.DoubleType, types.LitBoolean: types.BooleanType, types.LitUUID: types.UUIDType,
			types.LitHex: types.BlobType, types.LitDuration: types.DurationType}[lit.Kind]
		tt, err := vs.prepareTerm(lit, lit.Text, typ)
		if err != nil {
			return selector{}, err
		}
		return selector{kind: selTerm, name: lit.Text, typ: typ, t: tt}, nil
	case *cql.FuncSel:
		name := strings.TrimPrefix(strings.ToLower(r.Name), "system.")
		argColumn := func() (*Column, error) {
			if len(r.Args) != 1 {
				return nil, invalidf("Invalid number of arguments for function %s", name)
			}
			cs, ok := r.Args[0].(*cql.ColumnSel)
			if !ok {
				return nil, invalidf("Only column names are supported as arguments of %s", name)
			}
			c := t.byName[cs.Name]
			if c == nil {
				return nil, invalidf("Undefined column name %s in table %s.%s", cs.Name, t.Keyspace, t.Name)
			}
			return c, nil
		}
		switch name {
		case "count":
			if r.Star {
				return selector{kind: selCount, name: "count", typ: types.BigintType}, nil
			}
			if len(r.Args) == 1 {
				if ts, ok := r.Args[0].(*cql.TermSel); ok {
					if l, ok := ts.Term.(*cql.Literal); ok && l.Text == "1" {
						return selector{kind: selCount, name: "count", typ: types.BigintType}, nil
					}
				}
			}
			c, err := argColumn()
			if err != nil {
				return selector{}, err
			}
			return selector{kind: selCount, name: "system.count(" + c.Name + ")", typ: types.BigintType, col: c}, nil
		case "writetime", "ttl", "maxwritetime":
			c, err := argColumn()
			if err != nil {
				return selector{}, err
			}
			if c.Kind != Regular && c.Kind != Static {
				return selector{}, invalidf("Cannot use selection function %s on PRIMARY KEY part %s", name, c.Name)
			}
			if name == "ttl" {
				return selector{kind: selTTL, name: "ttl(" + c.Name + ")", typ: types.IntType, col: c}, nil
			}
			return selector{kind: selWritetime, name: name + "(" + c.Name + ")", typ: types.BigintType, col: c}, nil
		case "token":
			var cols []*Column
			var names []string
			for _, a := range r.Args {
				cs, ok := a.(*cql.ColumnSel)
				if !ok {
					return selector{}, invalidf("token() only accepts partition key columns")
				}
				cols = append(cols, t.byName[cs.Name])
				names = append(names, cs.Name)
			}
			if len(cols) != len(t.PartitionKey) || !slices.Equal(cols, t.PartitionKey) {
				return selector{}, invalidf("Type error: token(...) requires the partition key columns in order")
			}
			return selector{kind: selToken, name: "system.token(" + strings.Join(names, ", ") + ")", typ: types.BigintType, cols: cols}, nil
		case "min", "max", "sum", "avg":
			c, err := argColumn()
			if err != nil {
				return selector{}, err
			}
			kind := map[string]selKind{"min": selMin, "max": selMax, "sum": selSum, "avg": selAvg}[name]
			if (kind == selSum || kind == selAvg) && !isNumeric(c.Type) {
				return selector{}, invalidf("Invalid call to function %s, none of its type signatures match", name)
			}
			return selector{kind: kind, name: "system." + name + "(" + c.Name + ")", typ: c.Type, col: c}, nil
		case "cast":
			inner, err := prepareSelector(t, r.Args[0], vs)
			if err != nil {
				return selector{}, err
			}
			to, err := types.Parse(r.Type)
			if err != nil {
				return selector{}, invalidf("%v", err)
			}
			if inner.kind != selColumn || !castable(inner.typ, to) {
				return selector{}, invalidf("%s cannot be cast to %s", inner.typ, to)
			}
			return selector{kind: selCast, name: "cast(" + inner.name + " as " + to.String() + ")", typ: to, inner: &inner}, nil
		case "totimestamp", "todate", "tounixtimestamp":
			return selector{}, invalidf("function %s is not supported in selections", name)
		}
		return selector{}, invalidf("Unknown function '%s'", r.Name)
	}
	return selector{}, invalidf("unsupported selector")
}

func isNumeric(t *types.Type) bool {
	switch t.Kind {
	case types.Tinyint, types.Smallint, types.Int, types.Bigint, types.Float, types.Double, types.Varint, types.Decimal, types.Counter:
		return true
	}
	return false
}

func castable(from, to *types.Type) bool {
	if to.Kind == types.Text || to.Kind == types.Ascii {
		return true
	}
	return isNumeric(from) && isNumeric(to)
}

func (s *selectStmt) metadata() []*message.ColumnMetadata {
	out := make([]*message.ColumnMetadata, len(s.sels))
	for i, sel := range s.sels {
		out[i] = &message.ColumnMetadata{Keyspace: s.t.Keyspace, Table: s.t.Name, Name: sel.name, Index: int32(i), Type: sel.typ.DataType()}
	}
	return out
}

// rowData is a row as read: its key values and live cells.
type rowData struct {
	pk      [][]byte
	ck      [][]byte // nil for a partition's static-only row
	cells   []row.Cell
	statics []row.Cell
}

func findCell(cells []row.Cell, id uint32) *row.Cell {
	i, ok := slices.BinarySearchFunc(cells, id, func(c row.Cell, id uint32) int {
		switch {
		case c.Column < id:
			return -1
		case c.Column > id:
			return 1
		}
		return 0
	})
	if !ok {
		return nil
	}
	return &cells[i]
}

// value returns a column's value in r, nil if null.
func (r *rowData) value(c *Column) []byte {
	switch c.Kind {
	case PartitionKey:
		return r.pk[c.Position]
	case Clustering:
		if r.ck == nil {
			return nil
		}
		return r.ck[c.Position]
	case Static:
		if cell := findCell(r.statics, c.ID); cell != nil {
			return cell.Value
		}
	default:
		if cell := findCell(r.cells, c.ID); cell != nil {
			return cell.Value
		}
	}
	return nil
}

func (r *rowData) cell(c *Column) *row.Cell {
	if c.Kind == Static {
		return findCell(r.statics, c.ID)
	}
	return findCell(r.cells, c.ID)
}

// pagingState is the position of a paged query: the storage key of the
// last row returned and the rows left of LIMIT and PER PARTITION LIMIT.
type pagingState struct {
	lastKey      []byte
	remaining    int64
	partRemain   int64
	staticOnlyPK bool // the last row was a static-only row (lastKey is the partition prefix)
}

func (p *pagingState) encode() []byte {
	b := binary.AppendUvarint(nil, uint64(len(p.lastKey)))
	b = append(b, p.lastKey...)
	b = binary.AppendVarint(b, p.remaining)
	b = binary.AppendVarint(b, p.partRemain)
	if p.staticOnlyPK {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b
}

func decodePagingState(b []byte) (*pagingState, error) {
	bad := invalidf("Invalid value for the paging state")
	n, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b)-k) < n {
		return nil, bad
	}
	p := &pagingState{lastKey: b[k : k+int(n)]}
	b = b[k+int(n):]
	var m int
	if p.remaining, m = binary.Varint(b); m <= 0 {
		return nil, bad
	}
	b = b[m:]
	if p.partRemain, m = binary.Varint(b); m <= 0 {
		return nil, bad
	}
	b = b[m:]
	if len(b) != 1 {
		return nil, bad
	}
	p.staticOnlyPK = b[0] == 1
	return p, nil
}

// source reads a table's storage: the engine, or a virtual table's rows.
type source interface {
	get(key []byte) ([]byte, error)
	iter(lo, hi []byte) (iterator, error)
}

type iterator interface {
	First() bool
	SeekGE(key []byte) bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type engineSource struct{ t *lsm.Table }

func (s engineSource) get(key []byte) ([]byte, error) { return s.t.Get(key) }
func (s engineSource) iter(lo, hi []byte) (iterator, error) {
	it, err := s.t.NewIter(lo, hi)
	if err != nil {
		return nil, err
	}
	return it, nil
}

var errNotFound = lsm.ErrNotFound

func (s *selectStmt) execute(e *execCtx) (message.Message, error) {
	d := e.d
	var src source
	if s.t.virtual != nil {
		src = newVirtualSource(d, s.t)
	} else {
		d.tableMu.RLock()
		defer d.tableMu.RUnlock()
		lt, err := d.engine.Table(s.t.Storage)
		if err != nil {
			return nil, invalidf("table %s.%s was dropped", s.t.Keyspace, s.t.Name)
		}
		src = engineSource{lt}
	}
	q := &query{s: s, e: e, src: src}
	return q.run()
}

// query is one execution of a SELECT.
type query struct {
	s   *selectStmt
	e   *execCtx
	src source

	limit, perPartition int64
	pageSize            int
	resume              *pagingState
	ranges              []keyRange // clustering ranges (nil: whole partitions)

	rows      [][][]byte
	count     int64 // rows counted for LIMIT
	agg       *aggState
	lastKey   []byte
	lastStat  bool
	partLeft  int64
	full      bool // the page is full
	exhausted bool
	part      partState
}

func (q *query) run() (message.Message, error) {
	s, e := q.s, q.e
	var err error
	var ok bool
	q.limit, ok, err = bindInt(e, s.limit, "limit")
	if err != nil {
		return nil, err
	}
	if !ok {
		q.limit = math.MaxInt64
	} else if q.limit <= 0 {
		return nil, invalidf("LIMIT must be strictly positive")
	}
	q.perPartition, ok, err = bindInt(e, s.perPartition, "per partition limit")
	if err != nil {
		return nil, err
	}
	if !ok {
		q.perPartition = math.MaxInt64
	} else if q.perPartition <= 0 {
		return nil, invalidf("PER PARTITION LIMIT must be strictly positive")
	}
	q.pageSize = e.page
	if s.aggregate || s.sortIN {
		q.pageSize = 0
	}
	if s.aggregate {
		q.agg = newAggState(s.sels)
	}
	if len(e.paging) > 0 && q.pageSize > 0 {
		if q.resume, err = decodePagingState(e.paging); err != nil {
			return nil, err
		}
		q.limit = q.resume.remaining
	}
	if len(s.t.Clustering) > 0 && (len(s.where.ck) > 0 || s.where.slice != nil) {
		if q.ranges, err = s.where.clusteringRanges(e, s.t); err != nil {
			return nil, err
		}
		if len(q.ranges) == 0 {
			return q.result()
		}
	}
	if s.where.pkComplete() && len(s.t.PartitionKey) > 0 {
		pks, err := s.where.partitionKeys(e, s.t)
		if err != nil {
			return nil, err
		}
		prefixes := make([][]byte, len(pks))
		for i, pk := range pks {
			prefixes[i] = partitionPrefix(pk)
		}
		slices.SortFunc(prefixes, bytes.Compare)
		prefixes = slices.CompactFunc(prefixes, bytes.Equal)
		for _, p := range prefixes {
			if q.resume != nil && bytes.Compare(p, q.resume.lastKey) < 0 && !bytes.HasPrefix(q.resume.lastKey, p) {
				continue
			}
			pk, err := splitPartitionKey(mustPK(p), len(s.t.PartitionKey))
			if err != nil {
				return nil, err
			}
			if err := q.readPartition(p, pk); err != nil {
				return nil, err
			}
			if q.done() {
				break
			}
		}
		if !q.done() {
			q.exhausted = true
		}
	} else {
		if err := q.scan(); err != nil {
			return nil, err
		}
	}
	if s.sortIN {
		q.sortRows()
	}
	return q.result()
}

func mustPK(p []byte) []byte {
	_, pk, _, err := parsePartition(p)
	if err != nil {
		panic(err)
	}
	return pk
}

func (q *query) done() bool { return q.full || q.count >= q.limit }

// scan reads every partition in the token range.
func (q *query) scan() error {
	lo, hi, empty, err := q.s.where.tokenRange(q.e)
	if err != nil {
		return err
	}
	if empty {
		q.exhausted = true
		return nil
	}
	start := appendToken(nil, lo)
	var end []byte
	if hi != 1<<63-1 {
		end = appendToken(nil, hi+1)
	}
	if q.resume != nil {
		_, pk, _, err := parsePartition(q.resume.lastKey)
		if err != nil {
			return invalidf("Invalid value for the paging state")
		}
		p := partitionPrefix(pk)
		if bytes.Compare(p, start) > 0 {
			start = p
		}
	}
	it, err := q.src.iter(start, end)
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; {
		_, pk, _, err := parsePartition(it.Key())
		if err != nil {
			return err
		}
		p := partitionPrefix(pk)
		pkVals, err := splitPartitionKey(pk, len(q.s.t.PartitionKey))
		if err != nil {
			return err
		}
		if err := q.readPartitionIter(it, p, pkVals); err != nil {
			return err
		}
		if q.done() {
			return nil
		}
		// Skip what is left of the partition.
		if it.Valid() && bytes.HasPrefix(it.Key(), p) {
			ok = it.SeekGE(prefixEnd(p))
		} else {
			ok = it.Valid()
		}
	}
	q.exhausted = true
	return it.Error()
}

// readPartition reads one partition by its prefix.
func (q *query) readPartition(p []byte, pk [][]byte) error {
	t := q.s.t
	if len(t.Clustering) == 0 {
		if q.resume != nil && bytes.Compare(p, q.resume.lastKey) <= 0 {
			return nil
		}
		v, err := q.src.get(p)
		if errors.Is(err, errNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return q.emitPartition(p, pk, nil, deletions{}, []kv{{p, v}})
	}
	it, err := q.src.iter(append(slices.Clip(p), headerSuffix), append(slices.Clip(p), types.GTNext))
	if err != nil {
		return err
	}
	defer it.Close()
	if !it.First() {
		return it.Error()
	}
	return q.readPartitionIter(it, p, pk)
}

type kv struct{ k, v []byte }

// deletions are a partition's partition deletion and range tombstones.
type deletions struct {
	partition int64 // timestamp, or NoTimestamp
	ranges    []rangeTombstone
}

type rangeTombstone struct {
	start, end []byte // suffixes
	ts         int64
}

// deletedAt returns the newest deletion covering a row (by key suffix).
func (d *deletions) deletedAt(suffix []byte) int64 {
	ts := d.partition
	for _, r := range d.ranges {
		if r.ts > ts && bytes.Compare(suffix, r.start) >= 0 && bytes.Compare(suffix, r.end) < 0 {
			ts = r.ts
		}
	}
	return ts
}

// readPartitionIter reads the partition with prefix p from it, which is
// positioned at its first key; it stops at the end of the partition (it
// is then positioned after it) or when the query is done.
func (q *query) readPartitionIter(it iterator, p []byte, pk [][]byte) error {
	t := q.s.t
	if len(t.Clustering) == 0 {
		k := slices.Clone(it.Key())
		v := slices.Clone(it.Value())
		it.Next()
		if !bytes.Equal(k, p) {
			return nil
		}
		if q.resume != nil && bytes.Compare(p, q.resume.lastKey) <= 0 {
			return nil
		}
		return q.emitPartition(p, pk, nil, deletions{}, []kv{{k, v}})
	}
	dels := deletions{partition: row.NoTimestamp}
	var static []row.Cell
	var rows []kv
	ri := 0 // current clustering range
	for valid := it.Valid(); valid; {
		k := it.Key()
		if !bytes.HasPrefix(k, p) {
			break
		}
		suffix := k[len(p):]
		switch {
		case len(suffix) == 1 && suffix[0] == headerSuffix:
			r, err := row.Decode(it.Value())
			if err != nil {
				return err
			}
			for _, c := range r.Cells {
				if c.Column == deletionColumn {
					dels.partition = max(dels.partition, c.Timestamp)
				} else if c.Column < rtEndColumn {
					static = append(static, c)
				}
			}
			valid = it.Next()
		case len(suffix) > 0 && suffix[0] == tombstoneSuffix:
			r, err := row.Decode(it.Value())
			if err != nil {
				return err
			}
			var rt rangeTombstone
			for _, c := range r.Cells {
				switch c.Column {
				case rtStartColumn:
					rt.start = slices.Clone(c.Value)
				case rtEndColumn:
					rt.end = slices.Clone(c.Value)
				case deletionColumn:
					rt.ts = c.Timestamp
				}
			}
			dels.ranges = append(dels.ranges, rt)
			valid = it.Next()
		default:
			// A row: skip to the current clustering range.
			if q.ranges != nil {
				for ri < len(q.ranges) && q.rangeEnded(ri, suffix) {
					ri++
				}
				if ri == len(q.ranges) {
					for valid && bytes.HasPrefix(it.Key(), p) {
						valid = it.SeekGE(prefixEnd(p))
					}
					continue
				}
				r := q.ranges[ri]
				if bytes.Compare(suffix, r.start) < 0 {
					valid = it.SeekGE(append(slices.Clip(p), r.start...))
					continue
				}
			}
			rows = append(rows, kv{slices.Clone(k), slices.Clone(it.Value())})
			valid = it.Next()
			if len(rows) >= 1024 && !q.s.reversed {
				// Stream long partitions in chunks.
				if err := q.emitRows(p, pk, static, dels, rows, false); err != nil {
					return err
				}
				rows = rows[:0]
				if q.done() {
					return nil
				}
			}
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	return q.emitPartition(p, pk, static, dels, rows)
}

func (q *query) rangeEnded(ri int, suffix []byte) bool {
	r := q.ranges[ri]
	if r.point {
		return bytes.Compare(suffix, r.start) > 0
	}
	return bytes.Compare(suffix, r.end) >= 0
}

// partState is the state of the partition being emitted.
type partState struct {
	p       []byte
	anyLive bool
	started bool
}

func (q *query) emitPartition(p []byte, pk [][]byte, static []row.Cell, dels deletions, rows []kv) error {
	return q.emitRows(p, pk, static, dels, rows, true)
}

// emitRows turns stored rows into result rows. last says whether they are
// the partition's last rows (static-only rows are emitted then).
func (q *query) emitRows(p []byte, pk [][]byte, static []row.Cell, dels deletions, rows []kv, last bool) error {
	s, e := q.s, q.e
	t := s.t
	if !q.part.started || !bytes.Equal(q.part.p, p) {
		q.part = partState{p: p, started: true}
		q.partLeft = q.perPartition
		if q.resume != nil && bytes.HasPrefix(q.resume.lastKey, p) {
			q.partLeft = q.resume.partRemain
		}
	}
	liveStatic := liveCells(static, dels.partition, e.nowSec)
	if s.reversed {
		slices.Reverse(rows)
	}
	if s.distinct {
		if !last {
			return nil
		}
		any := len(liveStatic) > 0
		for _, r := range rows {
			if any {
				break
			}
			rd, err := q.decodeRow(r, p, pk, liveStatic, &dels)
			if err != nil {
				return err
			}
			any = rd != nil
		}
		if any && !(q.resume != nil && bytes.Compare(p, q.resume.lastKey) <= 0) {
			return q.output(&rowData{pk: pk, statics: liveStatic}, p, true)
		}
		return nil
	}
	for _, r := range rows {
		if q.done() || q.partLeft <= 0 {
			return nil
		}
		if q.resume != nil && bytes.HasPrefix(q.resume.lastKey, p) {
			c := bytes.Compare(r.k, q.resume.lastKey)
			if (!s.reversed && c <= 0) || (s.reversed && c >= 0 && !q.resume.staticOnlyPK) {
				continue
			}
		}
		rd, err := q.decodeRow(r, p, pk, liveStatic, &dels)
		if err != nil {
			return err
		}
		if rd == nil {
			continue
		}
		q.part.anyLive = true
		ok, err := q.filter(rd)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := q.output(rd, r.k, false); err != nil {
			return err
		}
		q.partLeft--
	}
	// A partition with static data but no rows returns one row of its
	// static columns when the clustering is unrestricted.
	if last && !q.part.anyLive && len(liveStatic) > 0 && len(t.Clustering) > 0 && q.ranges == nil && !q.done() && q.partLeft > 0 {
		if q.resume != nil && bytes.HasPrefix(q.resume.lastKey, p) {
			return nil
		}
		rd := &rowData{pk: pk, statics: liveStatic}
		ok, err := q.filter(rd)
		if err != nil || !ok {
			return err
		}
		return q.output(rd, p, true)
	}
	return nil
}

// liveCells returns the live cells not shadowed by a deletion at delTS.
func liveCells(cells []row.Cell, delTS, nowSec int64) []row.Cell {
	var out []row.Cell
	for _, c := range cells {
		if c.Timestamp > delTS && c.IsLive(nowSec) {
			out = append(out, c)
		}
	}
	return out
}

// decodeRow decodes a stored row, applying deletions; nil if the row is
// not live (no live primary key liveness and no live cell).
func (q *query) decodeRow(r kv, p []byte, pk [][]byte, statics []row.Cell, dels *deletions) (*rowData, error) {
	t := q.s.t
	v, err := row.Decode(r.v)
	if err != nil {
		return nil, err
	}
	suffix := r.k[len(p):]
	delTS := v.Deletion.MarkedForDeleteAt
	if len(t.Clustering) > 0 {
		delTS = max(delTS, dels.deletedAt(suffix))
	}
	cells := liveCells(v.Cells, delTS, q.e.nowSec)
	if len(cells) == 0 {
		return nil, nil
	}
	rd := &rowData{pk: pk, cells: cells, statics: statics}
	if len(t.Clustering) > 0 {
		if rd.ck, err = parseClustering(t, suffix); err != nil {
			return nil, err
		}
	} else {
		rd.ck = [][]byte{}
	}
	return rd, nil
}

// filter evaluates the filtering restrictions on a row.
func (q *query) filter(r *rowData) (bool, error) {
	for _, list := range [][]filter{q.s.partitionFilters, q.s.where.filters} {
		for _, f := range list {
			ok, err := f.matches(q.e, r)
			if err != nil || !ok {
				return false, err
			}
		}
	}
	return true, nil
}

func (f *filter) matches(e *execCtx, r *rowData) (bool, error) {
	v := r.value(f.col)
	if f.col.Kind == Clustering && r.ck == nil {
		return false, nil
	}
	if f.tuple != nil {
		return f.tuple.contains(e, r)
	}
	switch f.op {
	case "=", "IN":
		vals, err := f.in.values(e)
		if err != nil {
			return false, err
		}
		if v == nil {
			return false, nil
		}
		for _, x := range vals {
			if types.Compare(f.col.Type, v, x) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	x, st, err := f.t.bind(e)
	if err != nil {
		return false, err
	}
	if st != vSet {
		return false, invalidf("Invalid null value in condition for column %s", f.col.Name)
	}
	if v == nil {
		return false, nil
	}
	switch f.op {
	case "CONTAINS", "CONTAINS KEY":
		per := 1
		if f.col.Type.Kind == types.Map {
			per = 2
		}
		elems, err := types.SplitCollection(v, per)
		if err != nil {
			return false, err
		}
		for i, el := range elems {
			isKey := per == 1 || i%2 == 0
			if (f.op == "CONTAINS KEY") != (per == 2 && isKey) && per == 2 {
				continue
			}
			et := f.col.Type.Elems[0]
			if per == 2 && !isKey {
				et = f.col.Type.Elems[1]
			}
			if types.Compare(et, el, x) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	c := types.Compare(f.col.Type, v, x)
	switch f.op {
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	case "!=":
		return c != 0, nil
	}
	return false, nil
}

// output adds a result row (or folds it into the aggregates).
func (q *query) output(r *rowData, key []byte, staticOnly bool) error {
	q.count++
	q.lastKey, q.lastStat = key, staticOnly
	if q.agg != nil {
		return q.agg.add(q, r)
	}
	out := make([][]byte, len(q.s.sels))
	for i := range q.s.sels {
		v, err := q.evaluate(&q.s.sels[i], r)
		if err != nil {
			return err
		}
		out[i] = v
	}
	q.rows = append(q.rows, out)
	if q.pageSize > 0 && len(q.rows) >= q.pageSize {
		q.full = true
	}
	return nil
}

func (q *query) evaluate(s *selector, r *rowData) ([]byte, error) {
	switch s.kind {
	case selColumn:
		return r.value(s.col), nil
	case selWritetime:
		if c := r.cell(s.col); c != nil {
			return binary.BigEndian.AppendUint64(nil, uint64(c.Timestamp)), nil
		}
		return nil, nil
	case selTTL:
		if c := r.cell(s.col); c != nil && c.TTL > 0 {
			return binary.BigEndian.AppendUint32(nil, uint32(c.LocalDeletionTime-q.e.nowSec)), nil
		}
		return nil, nil
	case selToken:
		return binary.BigEndian.AppendUint64(nil, uint64(Token(serializePartitionKey(r.pk)))), nil
	case selTerm:
		v, _, err := s.t.bind(q.e)
		return v, err
	case selCast:
		v, err := q.evaluate(s.inner, r)
		if err != nil || v == nil {
			return v, err
		}
		return castValue(s.inner.typ, s.typ, v)
	}
	return nil, nil
}

func (q *query) sortRows() {
	// Sort by clustering order: the selection may not include the
	// clustering columns, so sort before projection is not possible here;
	// rows from several partitions are concatenated in partition order,
	// which Cassandra also returns for IN without ORDER BY. With ORDER BY,
	// the clustering columns must be selected for a meaningful sort.
	idx := -1
	for i, s := range q.s.sels {
		if s.kind == selColumn && s.col.Kind == Clustering && s.col.Position == 0 {
			idx = i
		}
	}
	if idx < 0 {
		return
	}
	c := q.s.sels[idx].col
	slices.SortStableFunc(q.rows, func(a, b [][]byte) int {
		r := types.Compare(c.Type, a[idx], b[idx])
		if c.Desc != q.s.reversed {
			r = -r
		}
		return r
	})
	if q.limit < int64(len(q.rows)) {
		q.rows = q.rows[:q.limit]
	}
}

func (q *query) result() (message.Message, error) {
	md := &message.RowsMetadata{ColumnCount: int32(len(q.s.sels)), Columns: q.s.metadata()}
	if q.agg != nil {
		row, err := q.agg.result(q)
		if err != nil {
			return nil, err
		}
		return &message.RowsResult{Metadata: md, Data: message.RowSet{row}}, nil
	}
	if q.full && q.count < q.limit {
		ps := &pagingState{lastKey: q.lastKey, remaining: q.limit - q.count, partRemain: q.partLeft, staticOnlyPK: q.lastStat}
		md.PagingState = ps.encode()
	}
	rows := q.rows
	if rows == nil {
		rows = message.RowSet{}
	}
	return &message.RowsResult{Metadata: md, Data: rows}, nil
}

// castValue converts between numeric types, or to text (CastFcts).
func castValue(from, to *types.Type, v []byte) ([]byte, error) {
	if len(v) == 0 {
		return v, nil
	}
	if to.Kind == types.Text || to.Kind == types.Ascii {
		return []byte(formatValue(from, v)), nil
	}
	r := numericValue(from, v)
	if r == nil {
		return nil, invalidf("cannot cast %s to %s", from, to)
	}
	return fromRat(to, r), nil
}

func numericValue(t *types.Type, v []byte) *big.Rat {
	switch t.Kind {
	case types.Tinyint:
		return new(big.Rat).SetInt64(int64(int8(v[0])))
	case types.Smallint:
		return new(big.Rat).SetInt64(int64(int16(binary.BigEndian.Uint16(v))))
	case types.Int:
		return new(big.Rat).SetInt64(int64(int32(binary.BigEndian.Uint32(v))))
	case types.Bigint, types.Counter:
		return new(big.Rat).SetInt64(int64(binary.BigEndian.Uint64(v)))
	case types.Float:
		f := float64(math.Float32frombits(binary.BigEndian.Uint32(v)))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return new(big.Rat).SetFloat64(f)
	case types.Double:
		f := math.Float64frombits(binary.BigEndian.Uint64(v))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return new(big.Rat).SetFloat64(f)
	case types.Varint:
		return new(big.Rat).SetInt(types.VarintValue(v))
	case types.Decimal:
		scale := int32(binary.BigEndian.Uint32(v))
		r := new(big.Rat).SetInt(types.VarintValue(v[4:]))
		p := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs32(scale))), nil))
		if scale > 0 {
			return r.Quo(r, p)
		}
		return r.Mul(r, p)
	}
	return nil
}

func abs32(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

func fromRat(t *types.Type, r *big.Rat) []byte {
	trunc := func() *big.Int { return new(big.Int).Quo(r.Num(), r.Denom()) }
	switch t.Kind {
	case types.Tinyint:
		return []byte{byte(trunc().Int64())}
	case types.Smallint:
		return binary.BigEndian.AppendUint16(nil, uint16(trunc().Int64()))
	case types.Int:
		return binary.BigEndian.AppendUint32(nil, uint32(trunc().Int64()))
	case types.Bigint, types.Counter:
		return binary.BigEndian.AppendUint64(nil, uint64(trunc().Int64()))
	case types.Float:
		f, _ := r.Float32()
		return binary.BigEndian.AppendUint32(nil, math.Float32bits(f))
	case types.Double:
		f, _ := r.Float64()
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
	case types.Varint:
		return types.VarintBytes(trunc())
	case types.Decimal:
		s := r.FloatString(18)
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
		b, _ := types.FromString(types.DecimalType, s)
		return b
	}
	return nil
}

// contains evaluates a multi-column slice on a row: its clustering tuple
// compared with the bounds in the columns' natural order (not clustering
// order), as tuples compare.
func (s *slice) contains(e *execCtx, r *rowData) (bool, error) {
	cmp := func(b *bound) (int, error) {
		v, st, err := b.t.bind(e)
		if err != nil {
			return 0, err
		}
		if st != vSet {
			return 0, invalidf("Invalid null value in condition for column %s", s.cols[0].Name)
		}
		fields, err := types.SplitTuple(v)
		if err != nil {
			return 0, err
		}
		for i, f := range fields {
			if c := types.Compare(s.cols[i].Type, r.ck[s.cols[i].Position], f); c != 0 {
				return c, nil
			}
		}
		return 0, nil
	}
	if s.lower != nil {
		c, err := cmp(s.lower)
		if err != nil || c < 0 || c == 0 && !s.lower.incl {
			return false, err
		}
	}
	if s.upper != nil {
		c, err := cmp(s.upper)
		if err != nil || c > 0 || c == 0 && !s.upper.incl {
			return false, err
		}
	}
	return true, nil
}
