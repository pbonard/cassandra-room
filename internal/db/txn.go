package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/pbonard/cassandra-room/internal/accord"
	"github.com/pbonard/cassandra-room/internal/cql"
	"github.com/pbonard/cassandra-room/internal/storage/row"
	"github.com/pbonard/cassandra-room/internal/types"
)

// Transactions: BEGIN TRANSACTION ... COMMIT TRANSACTION (CEP-15,
// TransactionStatement). LET assignments read one row each; a SELECT (or
// SELECT of LET references) is returned to the client; IF / ELSE IF / ELSE
// blocks choose updates by conditions on the LET rows; the remaining
// updates always apply. Every table involved must have transactional_mode
// 'full'. In a cluster the whole statement is one Accord transaction over
// every partition it reads or writes: the replicas read the LET rows and
// the SELECT once every earlier transaction on those partitions applied,
// the coordinator evaluates the conditions and the updates, and the writes
// apply at the execution timestamp.

// The messages are TransactionStatement's.
const (
	txnIncompletePartition = "SELECT must specify either all partition key elements. Partition key elements must be always specified with equality operators; %s %s"
	txnIncompletePrimary   = "SELECT must specify either all primary key elements or all partition key elements and LIMIT 1. In both cases partition key elements must be always specified with equality operators; %s %s"
	txnNoConditions        = "Updates within transactions may not specify their own conditions; %s statement %s"
	txnNoTimestamps        = "Updates within transactions may not specify custom timestamps; %s statement %s"
	txnNoTTLs              = "Updates within transactions may not specify custom ttls; %s statement %s"
	txnDisabledOnTable     = "Accord transactions are disabled on table (See transactional_mode in table options); %s statement %s"
	txnNoCounters          = "Counter columns cannot be accessed within a transaction; %s statement %s"
	txnNoAggregation       = "No aggregation functions allowed within a transaction; %s statement %s"
	txnNoOrderBy           = "No ORDER BY clause allowed within a transaction; %s statement %s"
	txnNoGroupBy           = "No GROUP BY clause allowed within a transaction; %s statement %s"
	txnEmpty               = "Transaction contains no reads or writes"
	txnIllegalRange        = "Range queries are not allowed for reads within a transaction; %s %s"
)

// txnRef is a LET row's table and the columns it selected.
type txnRef struct {
	t    *Table
	cols map[string]*Column
}

// refTerm is a LET reference in a term: its column's value in the row
// read, or null.
type refTerm struct {
	tuple, column string
}

func (r *refTerm) bind(e *execCtx) ([]byte, valState, error) {
	row := e.txnRefs[r.tuple]
	if row == nil || row[r.column] == nil {
		return nil, vNull, nil
	}
	return row[r.column], vSet, nil
}

func (vs *vars) prepareReference(r *cql.Reference, name string, typ *types.Type) (term, error) {
	ref := vs.refs[r.Tuple]
	if ref == nil {
		return nil, invalidf("Undefined LET name %s", r.Tuple)
	}
	if r.Column == "" {
		return nil, invalidf("A reference to %s needs a column", r.Tuple)
	}
	c := ref.cols[r.Column]
	if c == nil {
		return nil, invalidf("%s does not select a column %s", r.Tuple, r.Column)
	}
	if typ != nil && c.Type.String() != typ.String() {
		return nil, invalidf("Invalid reference %s.%s of type %s for %s of type %s", r.Tuple, r.Column, c.Type, name, typ)
	}
	return &refTerm{tuple: r.Tuple, column: r.Column}, nil
}

type txnLet struct {
	name string
	sel  *selectStmt
}

type txnCond struct {
	op          string
	left, right term
	typ         *types.Type
	// ref is the reference of an IS [NOT] NULL test (column empty: the
	// row itself).
	ref *refTerm
}

type txnBranch struct {
	conds []txnCond
	els   bool
	mods  []*modification
}

type txnStmt struct {
	lets      []txnLet
	sel       *selectStmt
	returning []*refTerm
	branches  []txnBranch
	updates   []*modification
	md        []*message.ColumnMetadata
}

func (d *DB) prepareTransaction(s *Schema, session string, st *cql.Transaction, vs *vars) (*txnStmt, error) {
	tx := &txnStmt{}
	vs.refs = map[string]*txnRef{}
	if err := d.rewriteIncrements(s, session, st); err != nil {
		return nil, err
	}
	checkTable := func(t *Table, kind, src string) error {
		switch {
		case !t.accordManaged():
			return invalidf(txnDisabledOnTable, kind, src)
		case t.IsCounter():
			return invalidf(txnNoCounters, kind, src)
		}
		return nil
	}
	readCheck := func(raw *cql.Select, sel *selectStmt, kind string, row bool) error {
		src := raw.Keyspace + "." + raw.Table
		if err := checkTable(sel.t, "SELECT", src); err != nil {
			return err
		}
		switch {
		case len(raw.OrderBy) > 0:
			return invalidf(txnNoOrderBy, "SELECT", src)
		case len(raw.GroupBy) > 0:
			return invalidf(txnNoGroupBy, "SELECT", src)
		case sel.aggregate:
			return invalidf(txnNoAggregation, "SELECT", src)
		case !sel.where.pkComplete():
			if sel.where.hasPartitionRestriction() {
				return invalidf(txnIncompletePartition, kind, src)
			}
			return invalidf(txnIllegalRange, kind, src)
		case row && (len(sel.where.ck) < len(sel.t.Clustering) || sel.where.slice != nil) && raw.Limit == nil:
			return invalidf(txnIncompletePrimary, kind, src)
		}
		return nil
	}
	for _, l := range st.Lets {
		sel, err := d.prepareSelect(s, session, l.Select, vs)
		if err != nil {
			return nil, err
		}
		if err := readCheck(l.Select, sel, "LET assignment", true); err != nil {
			return nil, err
		}
		ref := &txnRef{t: sel.t, cols: map[string]*Column{}}
		for _, c := range sel.metadata() {
			if col := sel.t.Column(c.Name); col != nil {
				ref.cols[c.Name] = col
			}
		}
		vs.refs[l.Name] = ref
		tx.lets = append(tx.lets, txnLet{name: l.Name, sel: sel})
	}
	if st.Select != nil {
		sel, err := d.prepareSelect(s, session, st.Select, vs)
		if err != nil {
			return nil, err
		}
		if err := readCheck(st.Select, sel, "returning SELECT", false); err != nil {
			return nil, err
		}
		tx.sel, tx.md = sel, sel.metadata()
	}
	for _, r := range st.Returning {
		if _, err := vs.prepareReference(r, "", nil); err != nil {
			return nil, err
		}
		ref := vs.refs[r.Tuple]
		c := ref.cols[r.Column]
		tx.returning = append(tx.returning, &refTerm{tuple: r.Tuple, column: r.Column})
		tx.md = append(tx.md, &message.ColumnMetadata{Keyspace: ref.t.Keyspace, Table: ref.t.Name, Name: r.Tuple + "." + r.Column, Index: int32(len(tx.md)), Type: c.Type.DataType()})
	}
	prepareMods := func(raws []cql.Statement) ([]*modification, error) {
		var out []*modification
		for _, raw := range raws {
			m, err := d.prepareModification(s, session, raw, vs)
			if err != nil {
				return nil, err
			}
			kind := map[modKind]string{insertKind: "INSERT", updateKind: "UPDATE", deleteKind: "DELETE"}[m.kind]
			src := m.t.Keyspace + "." + m.t.Name
			switch {
			case m.cond != nil:
				return nil, invalidf(txnNoConditions, kind, src)
			case m.timestamp != nil:
				return nil, invalidf(txnNoTimestamps, kind, src)
			case m.ttl != nil:
				return nil, invalidf(txnNoTTLs, kind, src)
			}
			if err := checkTable(m.t, kind, src); err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		return out, nil
	}
	for _, b := range st.Branches {
		br := txnBranch{els: b.Else}
		for _, c := range b.Conditions {
			cond := txnCond{op: c.Op}
			if c.Op == "IS NULL" || c.Op == "IS NOT NULL" {
				r := c.Left.(*cql.Reference)
				if vs.refs[r.Tuple] == nil {
					return nil, invalidf("Undefined LET name %s", r.Tuple)
				}
				if r.Column != "" {
					if _, err := vs.prepareReference(r, "", nil); err != nil {
						return nil, err
					}
				}
				cond.ref = &refTerm{tuple: r.Tuple, column: r.Column}
				br.conds = append(br.conds, cond)
				continue
			}
			// The references' types type the other side.
			cond.typ = vs.termType(c.Left)
			if cond.typ == nil {
				cond.typ = vs.termType(c.Right)
			}
			if cond.typ == nil {
				return nil, invalidf("a transaction condition needs a LET reference to a selected column")
			}
			var err error
			if cond.left, err = vs.prepareTxnTerm(c.Left, cond.typ); err != nil {
				return nil, err
			}
			if cond.right, err = vs.prepareTxnTerm(c.Right, cond.typ); err != nil {
				return nil, err
			}
			br.conds = append(br.conds, cond)
		}
		var err error
		if br.mods, err = prepareMods(b.Updates); err != nil {
			return nil, err
		}
		tx.branches = append(tx.branches, br)
	}
	var err error
	if tx.updates, err = prepareMods(st.Updates); err != nil {
		return nil, err
	}
	if len(tx.lets) == 0 && tx.sel == nil && len(tx.branches) == 0 && len(tx.updates) == 0 {
		return nil, invalidf(txnEmpty)
	}
	return tx, nil
}

func (vs *vars) prepareTxnTerm(raw cql.Term, typ *types.Type) (term, error) {
	if r, ok := raw.(*cql.Reference); ok {
		return vs.prepareReference(r, r.Tuple+"."+r.Column, typ)
	}
	return vs.prepareTerm(raw, "[condition]", typ)
}

// txnReads are the rows a transaction read: per LET, then the SELECT.
type txnReads [][][][]byte

// reads returns the transaction's reads: each LET and the SELECT, with the
// partition they read.
func (tx *txnStmt) reads() []*selectStmt {
	var out []*selectStmt
	for _, l := range tx.lets {
		out = append(out, l.sel)
	}
	if tx.sel != nil {
		out = append(out, tx.sel)
	}
	return out
}

// partitions returns the partitions a read or modification touches.
func stmtPartitions(e *execCtx, t *Table, w *where) ([]accord.Key, error) {
	pks, err := w.partitionKeys(e, t)
	if err != nil {
		return nil, err
	}
	var out []accord.Key
	for _, pk := range pks {
		out = append(out, accordKey(t.Storage, pk))
	}
	return out, nil
}

// keys returns every partition the transaction reads or may write.
func (tx *txnStmt) keys(e *execCtx) ([]accord.Key, bool, error) {
	var keys []accord.Key
	add := func(ks []accord.Key) {
		for _, k := range ks {
			if !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
	}
	for _, s := range tx.reads() {
		ks, err := stmtPartitions(e, s.t, s.where)
		if err != nil {
			return nil, false, err
		}
		add(ks)
	}
	writes := false
	for _, mods := range tx.allMods() {
		for _, m := range mods {
			writes = true
			ks, err := stmtPartitions(e, m.t, m.where)
			if err != nil {
				return nil, false, err
			}
			add(ks)
		}
	}
	slices.SortFunc(keys, accord.Key.Compare)
	return keys, writes, nil
}

func (tx *txnStmt) allMods() [][]*modification {
	out := [][]*modification{tx.updates}
	for _, b := range tx.branches {
		out = append(out, b.mods)
	}
	return out
}

// readLocal runs the reads whose partition is key (all of them if key is
// nil) on this node's data.
func (tx *txnStmt) readLocal(e *execCtx, key *accord.Key) (txnReads, error) {
	out := make(txnReads, len(tx.reads()))
	for i, s := range tx.reads() {
		if key != nil {
			ks, err := stmtPartitions(e, s.t, s.where)
			if err != nil {
				return nil, err
			}
			if !slices.Contains(ks, *key) {
				continue
			}
		}
		re := &execCtx{d: e.d, values: e.values, now: e.now, nowSec: e.nowSec, cl: primitive.ConsistencyLevelOne, local: true}
		if !s.t.accordReads() && e.d.proxy != nil {
			re.cl, re.local = primitive.ConsistencyLevelQuorum, false // interop
		}
		res, err := s.execute(re)
		if err != nil {
			return nil, err
		}
		for _, r := range res.(*message.RowsResult).Data {
			out[i] = append(out[i], r)
		}
	}
	return out, nil
}

// evaluate binds the LET rows, chooses the branch and builds the
// mutation; it returns the result rows.
func (tx *txnStmt) evaluate(e *execCtx, reads txnReads, mu *mutation) ([][][]byte, error) {
	e.txnRefs = map[string]map[string][]byte{}
	for i, l := range tx.lets {
		if len(reads[i]) == 0 {
			continue
		}
		row := map[string][]byte{}
		for j, c := range l.sel.metadata() {
			row[c.Name] = reads[i][0][j]
		}
		e.txnRefs[l.name] = row
	}
	mods := slices.Clone(tx.updates)
	for _, b := range tx.branches {
		ok := true
		for _, c := range b.conds {
			holds, err := c.holds(e)
			if err != nil {
				return nil, err
			}
			if !holds {
				ok = false
				break
			}
		}
		if ok {
			mods = append(b.mods, mods...)
			break
		}
	}
	for _, m := range mods {
		if err := m.addMutations(e, mu); err != nil {
			return nil, err
		}
	}
	if err := laterStatementsWin(mu); err != nil {
		return nil, err
	}
	switch {
	case tx.sel != nil:
		return reads[len(tx.lets)], nil
	case len(tx.returning) > 0:
		row := make([][]byte, len(tx.returning))
		for i, r := range tx.returning {
			row[i], _, _ = r.bind(e)
		}
		return [][][]byte{row}, nil
	}
	return nil, nil
}

// laterStatementsWin combines a transaction's writes to the same row: its
// statements share one timestamp (the executeAt's), so two writes of a
// cell would tie, and the larger value win; the later statement's cell
// replaces the earlier's instead.
func laterStatementsWin(mu *mutation) error {
	at := map[string]int{}
	out := mu.writes[:0]
	for _, w := range mu.writes {
		k := string(binary.BigEndian.AppendUint64(nil, uint64(w.table))) + string(w.key)
		i, seen := at[k]
		if !seen {
			at[k] = len(out)
			out = append(out, w)
			continue
		}
		prev, err := row.Decode(out[i].value)
		if err != nil {
			return err
		}
		next, err := row.Decode(w.value)
		if err != nil {
			return err
		}
		prev.Cells = slices.DeleteFunc(prev.Cells, func(c row.Cell) bool {
			return slices.ContainsFunc(next.Cells, func(n row.Cell) bool {
				return n.Column == c.Column && bytes.Equal(n.Path, c.Path) && n.ComplexDeletion == c.ComplexDeletion
			})
		})
		merged, err := row.Merge(nil, row.Append(nil, prev), w.value)
		if err != nil {
			return err
		}
		out[i].value = merged
	}
	mu.writes = out
	return nil
}

func (c *txnCond) holds(e *execCtx) (bool, error) {
	switch c.op {
	case "IS NULL", "IS NOT NULL":
		row := e.txnRefs[c.ref.tuple]
		null := row == nil || c.ref.column != "" && row[c.ref.column] == nil
		return null == (c.op == "IS NULL"), nil
	}
	a, sa, err := c.left.bind(e)
	if err != nil {
		return false, err
	}
	b, sb, err := c.right.bind(e)
	if err != nil {
		return false, err
	}
	if sa != vSet || sb != vSet {
		// Null compares as nothing but != (as column conditions).
		return c.op == "!=" && (sa == vSet) != (sb == vSet), nil
	}
	cmp := types.Compare(c.typ, a, b)
	switch c.op {
	case "=":
		return cmp == 0, nil
	case "!=":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	}
	return false, fmt.Errorf("db: unknown condition %s", c.op)
}

func (tx *txnStmt) result(rows [][][]byte) message.Message {
	if tx.md == nil {
		return &message.VoidResult{}
	}
	return &message.RowsResult{Metadata: &message.RowsMetadata{ColumnCount: int32(len(tx.md)), Columns: tx.md}, Data: rows}
}

// txnLock serializes transactions on a single node.
var txnLock sync.Mutex

func (tx *txnStmt) execute(e *execCtx) (message.Message, error) {
	d := e.d
	if d.proxy == nil {
		txnLock.Lock()
		defer txnLock.Unlock()
		reads, err := tx.readLocal(e, nil)
		if err != nil {
			return nil, err
		}
		mu := &mutation{}
		e.setTimestamp(d.timestamp())
		rows, err := tx.evaluate(e, reads, mu)
		if err != nil {
			return nil, err
		}
		if err := d.applyLocal(mu.writes); err != nil {
			return nil, err
		}
		return tx.result(rows), nil
	}
	keys, writes, err := tx.keys(e)
	if err != nil {
		return nil, err
	}
	out, err := d.proxy.accordRun(e, "txn", keys, writes)
	if err != nil {
		return nil, err
	}
	var rows [][][]byte
	if err := json.Unmarshal(out, &rows); err != nil {
		return nil, err
	}
	return tx.result(rows), nil
}

// arithTerm is left op right on numeric types (null if either is null,
// unless zeroNull: a null left side then counts as zero, as c += v on a
// missing value sets v).
type arithTerm struct {
	op       byte
	l, r     term
	lt, rt   *types.Type
	typ      *types.Type
	zeroNull bool
}

// termType infers a raw term's type from the references it uses (nil
// when it has none: it takes the type of its context).
func (vs *vars) termType(raw cql.Term) *types.Type {
	switch r := raw.(type) {
	case *cql.Reference:
		if ref := vs.refs[r.Tuple]; ref != nil {
			if c := ref.cols[r.Column]; c != nil {
				return c.Type
			}
		}
	case *cql.BinaryTerm:
		lt, rt := vs.termType(r.Left), vs.termType(r.Right)
		switch {
		case lt == nil:
			return rt
		case rt == nil:
			return lt
		}
		t, err := arithmeticType(lt, rt, r.Op)
		if err == nil {
			return t
		}
	case *cql.Cast:
		if t, err := types.Parse(r.Type); err == nil {
			return t
		}
	}
	return nil
}

// prepareArith prepares an arithmetic term: operand types come from the
// references (a side without any takes the other's, or the context's);
// the result must have the type of its context.
func (vs *vars) prepareArith(r *cql.BinaryTerm, name string, typ *types.Type) (term, error) {
	lt, rt := vs.termType(r.Left), vs.termType(r.Right)
	switch {
	case lt == nil && rt == nil:
		lt, rt = typ, typ
	case lt == nil:
		lt = rt
	case rt == nil:
		rt = lt
	}
	if lt == nil || !isNumeric(lt) || !isNumeric(rt) {
		return nil, invalidf("the '%c' operation needs numeric operands for %s", r.Op, name)
	}
	res, err := arithmeticType(lt, rt, r.Op)
	if err != nil {
		return nil, err
	}
	if typ != nil && res.String() != typ.String() {
		return nil, invalidf("Invalid arithmetic of type %s for %s of type %s", res, name, typ)
	}
	l, err := vs.prepareTerm(r.Left, name, lt)
	if err != nil {
		return nil, err
	}
	rr, err := vs.prepareTerm(r.Right, name, rt)
	if err != nil {
		return nil, err
	}
	return &arithTerm{op: r.Op, l: l, r: rr, lt: lt, rt: rt, typ: res, zeroNull: r.ZeroNull}, nil
}

func (a *arithTerm) bind(e *execCtx) ([]byte, valState, error) {
	l, ls, err := a.l.bind(e)
	if err != nil {
		return nil, vNull, err
	}
	r, rs, err := a.r.bind(e)
	if err != nil {
		return nil, vNull, err
	}
	if rs != vSet {
		return nil, vNull, nil
	}
	if ls != vSet {
		if !a.zeroNull {
			return nil, vNull, nil
		}
		if l, err = types.FromLiteral(a.lt, types.LitInteger, "0"); err != nil {
			return nil, vNull, err
		}
	}
	if (a.op == '/' || a.op == '%') && isZeroNumber(a.rt, r) {
		return nil, vNull, invalidf("Cannot divide by zero")
	}
	v, err := arithmetic(a.op, a.lt, a.rt, a.typ, l, r)
	if err != nil {
		return nil, vNull, err
	}
	return v, vSet, nil
}

// rewriteIncrements turns c += v and c -= v on numeric (non-counter)
// columns into c = row.c + v, reading the updated row in the transaction
// (TransactionStatement's implicit reads for arithmetic updates).
func (d *DB) rewriteIncrements(s *Schema, session string, st *cql.Transaction) error {
	n := 0
	rewrite := func(raws []cql.Statement) error {
		for _, raw := range raws {
			u, ok := raw.(*cql.Update)
			if !ok {
				continue
			}
			ksName := u.Keyspace
			if ksName == "" {
				ksName = session
			}
			ks := s.Keyspaces[ksName]
			if ks == nil {
				continue
			}
			t := ks.Tables[u.Table]
			if t == nil || t.IsCounter() {
				continue
			}
			for i, op := range u.Operations {
				c := t.Column(op.Column)
				if c == nil || op.Key != nil || op.Field != "" || !isNumeric(c.Type) {
					continue
				}
				var sign byte
				switch op.Op {
				case "+", "+=":
					sign = '+'
				case "-", "-=":
					sign = '-'
				default:
					continue
				}
				name := fmt.Sprintf("\x00row%d", n)
				n++
				st.Lets = append(st.Lets, cql.Let{Name: name, Select: &cql.Select{Keyspace: u.Keyspace, Table: u.Table,
					Selectors: []cql.Selector{{Expr: &cql.ColumnSel{Name: op.Column}}}, Where: u.Where}})
				u.Operations[i] = cql.Operation{Column: op.Column, Op: "=",
					Value: &cql.BinaryTerm{Op: sign, Left: &cql.Reference{Tuple: name, Column: op.Column}, Right: op.Value, ZeroNull: true}}
			}
		}
		return nil
	}
	for _, b := range st.Branches {
		if err := rewrite(b.Updates); err != nil {
			return err
		}
	}
	return rewrite(st.Updates)
}

// isZeroNumber reports whether a numeric value is zero.
func isZeroNumber(t *types.Type, v []byte) bool {
	z, err := types.FromLiteral(t, types.LitInteger, "0")
	return err == nil && types.Compare(t, v, z) == 0
}
