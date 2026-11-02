package db

import (
	"crypto/md5"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// statement is a prepared statement (CQLStatement).
type statement interface {
	execute(e *execCtx) (message.Message, error)
}

// Prepared is a statement prepared for a keyspace (QueryHandler.Prepared).
// It is re-prepared transparently when the schema changes.
type Prepared struct {
	// ID is the statement id: an MD5 of the keyspace and query string, as
	// QueryProcessor.computeId.
	ID       []byte
	Query    string
	Keyspace string
	state    atomic.Pointer[prepState]
}

type prepState struct {
	version   [16]byte
	stmt      statement
	vars      []spec
	pkIndices []uint16
	result    []*message.ColumnMetadata // nil if the statement returns no rows
	resultID  []byte
	use       string // the keyspace of a USE statement
}

// StatementID computes the id of a query prepared in a keyspace.
func StatementID(keyspace, query string) []byte {
	h := md5.Sum([]byte(keyspace + query))
	return h[:]
}

// Prepare parses and prepares a query in the session's keyspace.
func (d *DB) Prepare(keyspace, query string) (*Prepared, error) {
	st, err := d.prepare(keyspace, query)
	if err != nil {
		return nil, err
	}
	p := &Prepared{ID: StatementID(keyspace, query), Query: query, Keyspace: keyspace}
	p.state.Store(st)
	return p, nil
}

// IsUse reports whether the statement is a USE.
func (p *Prepared) IsUse() bool { return p.state.Load().use != "" }

// Metadata returns the bind variables' and the result's metadata.
func (p *Prepared) Metadata() (*message.VariablesMetadata, *message.RowsMetadata, []byte) {
	s := p.state.Load()
	vm := &message.VariablesMetadata{PkIndices: s.pkIndices}
	for i, v := range s.vars {
		vm.Columns = append(vm.Columns, &message.ColumnMetadata{Keyspace: v.ks, Table: v.table, Name: v.name, Index: int32(i), Type: v.typ.DataType()})
	}
	rm := &message.RowsMetadata{ColumnCount: int32(len(s.result)), Columns: s.result}
	return vm, rm, s.resultID
}

func (d *DB) prepare(keyspace, query string) (*prepState, error) {
	parsed, err := cql.Parse(query)
	if err != nil {
		return nil, err
	}
	s := d.schema.Load()
	vs := newVars(parsed.Markers)
	ps := &prepState{version: s.Version}
	switch st := parsed.Statement.(type) {
	case *cql.Use:
		if _, ok := s.Keyspaces[st.Keyspace]; !ok {
			return nil, invalidf("Keyspace '%s' does not exist", st.Keyspace)
		}
		ps.use = st.Keyspace
		ps.stmt = &useStmt{keyspace: st.Keyspace}
	case *cql.Select:
		sel, err := d.prepareSelect(s, keyspace, st, vs)
		if err != nil {
			return nil, err
		}
		ps.stmt = sel
		ps.result = sel.metadata()
		ps.pkIndices = sel.pkIndices
	case *cql.Insert, *cql.Update, *cql.Delete:
		m, err := d.prepareModification(s, keyspace, st, vs)
		if err != nil {
			return nil, err
		}
		ps.stmt = m
		ps.pkIndices = m.pkIndices
		if m.cond != nil {
			ps.result = m.cond.metadata()
		}
	case *cql.Batch:
		b, err := d.prepareBatch(s, keyspace, st, vs)
		if err != nil {
			return nil, err
		}
		ps.stmt = b
	default:
		ddl, err := d.prepareDDL(keyspace, parsed.Statement)
		if err != nil {
			return nil, err
		}
		ps.stmt = ddl
	}
	for i, sp := range vs.specs {
		if sp.typ == nil {
			return nil, invalidf("bind marker %d is not used in a valid position", i)
		}
	}
	ps.vars = vs.specs
	h := md5.New()
	for _, c := range ps.result {
		fmt.Fprintf(h, "%s.%s.%s:%s;", c.Keyspace, c.Table, c.Name, c.Type.AsCql())
	}
	ps.resultID = h.Sum(nil)
	return ps, nil
}

// keyspaceOf resolves a statement's keyspace.
func keyspaceOf(explicit, session string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if session == "" {
		return "", invalidf("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename")
	}
	return session, nil
}

// execCtx is one execution of a statement (QueryOptions and QueryState).
type execCtx struct {
	d       *DB
	values  []*primitive.Value
	now     time.Time
	nowSec  int64
	ts      int64 // the client's default timestamp, or 0
	page    int   // page size, or <= 0
	paging  []byte
	noMeta  bool
	version primitive.ProtocolVersion
}

func (e *execCtx) timestamp() int64 {
	if e.ts == 0 {
		e.ts = e.d.timestamp()
	}
	return e.ts
}

// RequestOptions are the per-request execution options.
type RequestOptions struct {
	Values      []*primitive.Value
	Named       map[string]*primitive.Value
	PageSize    int32
	PagingState []byte
	Timestamp   *int64
	NowInSec    *int32
	SkipMeta    bool
	// ResultMetadataID is the client's (protocol v5): when it differs from
	// the statement's, the result carries the new metadata.
	ResultMetadataID []byte
	Version          primitive.ProtocolVersion
}

// OptionsOf converts protocol query options.
func OptionsOf(o *message.QueryOptions, v primitive.ProtocolVersion) *RequestOptions {
	out := &RequestOptions{Version: v}
	if o == nil {
		return out
	}
	out.Values, out.Named = o.PositionalValues, o.NamedValues
	out.PageSize, out.PagingState = o.PageSize, o.PagingState
	out.Timestamp, out.NowInSec, out.SkipMeta = o.DefaultTimestamp, o.NowInSeconds, o.SkipMetadata
	return out
}

func (d *DB) newCtx(o *RequestOptions, st *prepState) (*execCtx, error) {
	e := &execCtx{d: d, now: d.opts.Now(), page: int(o.PageSize), paging: o.PagingState, noMeta: o.SkipMeta, version: o.Version}
	e.nowSec = e.now.Unix()
	if o.NowInSec != nil {
		e.nowSec = int64(*o.NowInSec)
	}
	if o.Timestamp != nil {
		e.ts = *o.Timestamp
	}
	switch {
	case len(o.Named) > 0:
		e.values = make([]*primitive.Value, len(st.vars))
		for i, v := range st.vars {
			val, ok := o.Named[v.name]
			if !ok {
				return nil, invalidf("No value has been provided for bind variable %s", v.name)
			}
			e.values[i] = val
		}
	default:
		if len(o.Values) != len(st.vars) {
			return nil, invalidf("There were %d markers(?) in CQL but %d bound variables", len(st.vars), len(o.Values))
		}
		e.values = o.Values
	}
	return e, nil
}

// current returns the statement prepared against the current schema.
func (d *DB) current(p *Prepared) (*prepState, error) {
	st := p.state.Load()
	if st.version == d.schema.Load().Version {
		return st, nil
	}
	nst, err := d.prepare(p.Keyspace, p.Query)
	if err != nil {
		return nil, err
	}
	p.state.CompareAndSwap(st, nst)
	return nst, nil
}

// Execute executes a prepared statement and returns the response message
// (a result or an error).
func (d *DB) Execute(p *Prepared, o *RequestOptions) message.Message {
	st, err := d.current(p)
	if err != nil {
		return ErrorMessage(err)
	}
	e, err := d.newCtx(o, st)
	if err != nil {
		return ErrorMessage(err)
	}
	res, err := st.stmt.execute(e)
	if err != nil {
		return ErrorMessage(err)
	}
	if rows, ok := res.(*message.RowsResult); ok {
		switch {
		case o.ResultMetadataID != nil && string(o.ResultMetadataID) != string(st.resultID):
			rows.Metadata.NewResultMetadataId = st.resultID
		case o.SkipMeta:
			rows.Metadata.Columns = nil
		}
	}
	return res
}

// BatchChild is one statement of a protocol BATCH message.
type BatchChild struct {
	Prepared *Prepared
	Values   []*primitive.Value
}

// ExecuteBatch executes a protocol BATCH message (BatchStatement): every
// child's mutations are applied atomically, with the batch's timestamp.
func (d *DB) ExecuteBatch(typ primitive.BatchType, children []BatchChild, o *RequestOptions) message.Message {
	if len(children) == 0 {
		return &message.VoidResult{}
	}
	var mods []boundModification
	ctx := &execCtx{d: d, now: d.opts.Now()}
	ctx.nowSec = ctx.now.Unix()
	if o.NowInSec != nil {
		ctx.nowSec = int64(*o.NowInSec)
	}
	if o.Timestamp != nil {
		ctx.ts = *o.Timestamp
	}
	for _, c := range children {
		st, err := d.current(c.Prepared)
		if err != nil {
			return ErrorMessage(err)
		}
		m, ok := st.stmt.(*modification)
		if !ok {
			return ErrorMessage(invalidf("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed."))
		}
		if m.cond != nil {
			return ErrorMessage(invalidf("Conditional batches are not supported"))
		}
		if len(c.Values) != len(st.vars) {
			return ErrorMessage(invalidf("There were %d markers(?) in CQL but %d bound variables", len(st.vars), len(c.Values)))
		}
		child := *ctx
		child.values = c.Values
		mods = append(mods, boundModification{m: m, e: &child})
	}
	if err := d.applyModifications(mods, ctx); err != nil {
		return ErrorMessage(err)
	}
	return &message.VoidResult{}
}

type useStmt struct{ keyspace string }

func (u *useStmt) execute(*execCtx) (message.Message, error) {
	return &message.SetKeyspaceResult{Keyspace: u.keyspace}, nil
}
