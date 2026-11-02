package cql

import (
	"fmt"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/types"
)

// Parse parses one statement, with optional trailing semicolons.
func Parse(query string) (*Parsed, error) {
	toks, err := lex(query)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, src: query}
	st, err := p.statement()
	if err != nil {
		return nil, err
	}
	for p.punct(";") {
	}
	if p.peek().kind != tEOF {
		return nil, p.errorf("extraneous input '%s'", p.peekText())
	}
	return &Parsed{Statement: st, Markers: p.markers}, nil
}

type parser struct {
	toks    []token
	i       int
	src     string
	markers []string
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) peekAt(n int) token {
	if p.i+n < len(p.toks) {
		return p.toks[p.i+n]
	}
	return p.toks[len(p.toks)-1]
}
func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tEOF {
		p.i++
	}
	return t
}

func (p *parser) peekText() string {
	t := p.peek()
	if t.kind == tEOF {
		return "<EOF>"
	}
	return p.src[t.pos:min(len(p.src), t.pos+max(len(t.text), 1))]
}

func (p *parser) errorf(format string, args ...any) error {
	return &SyntaxError{Pos: p.peek().pos, Msg: fmt.Sprintf(format, args...)}
}

// isWord reports whether the next token is the (unquoted) keyword w.
func (p *parser) isWord(w string) bool {
	t := p.peek()
	return t.kind == tIdent && t.text == w
}

func (p *parser) isWordAt(n int, w string) bool {
	t := p.peekAt(n)
	return t.kind == tIdent && t.text == w
}

// word consumes the keyword w if it is next.
func (p *parser) word(w string) bool {
	if p.isWord(w) {
		p.i++
		return true
	}
	return false
}

func (p *parser) expectWord(ws ...string) error {
	for _, w := range ws {
		if !p.word(w) {
			return p.errorf("mismatched input '%s' expecting %s", p.peekText(), strings.ToUpper(w))
		}
	}
	return nil
}

func (p *parser) isPunct(s string) bool {
	t := p.peek()
	return t.kind == tPunct && t.text == s
}

func (p *parser) punct(s string) bool {
	if p.isPunct(s) {
		p.i++
		return true
	}
	return false
}

func (p *parser) expectPunct(s string) error {
	if !p.punct(s) {
		return p.errorf("mismatched input '%s' expecting '%s'", p.peekText(), s)
	}
	return nil
}

// reserved are Cassandra's reserved keywords, which cannot be unquoted
// identifiers.
var reserved = map[string]bool{
	"add": true, "allow": true, "alter": true, "and": true, "apply": true, "asc": true,
	"authorize": true, "batch": true, "begin": true, "by": true, "columnfamily": true,
	"create": true, "default": true, "delete": true, "desc": true, "describe": true, "drop": true,
	"entries": true, "execute": true, "from": true, "full": true, "grant": true, "if": true,
	"in": true, "index": true, "infinity": true, "insert": true, "into": true, "is": true,
	"keyspace": true, "limit": true, "materialized": true, "mbean": true, "mbeans": true,
	"modify": true, "nan": true, "norecursive": true, "not": true, "null": true, "of": true,
	"on": true, "or": true, "order": true, "primary": true, "rename": true, "replace": true,
	"revoke": true, "schema": true, "select": true, "set": true, "table": true, "to": true,
	"token": true, "truncate": true, "unlogged": true, "unset": true, "update": true, "use": true,
	"using": true, "view": true, "where": true, "with": true,
}

// ident reads an identifier: quoted (case-sensitive) or unquoted
// (lowercased, not reserved).
func (p *parser) ident() (string, error) {
	t := p.peek()
	switch {
	case t.kind == tQuoted:
		p.i++
		return t.text, nil
	case t.kind == tIdent && !reserved[t.text]:
		p.i++
		return t.text, nil
	}
	return "", p.errorf("no viable alternative at input '%s'", p.peekText())
}

// tableName reads [keyspace.]table.
func (p *parser) tableName() (string, string, error) {
	a, err := p.ident()
	if err != nil {
		return "", "", err
	}
	if p.punct(".") {
		b, err := p.ident()
		return a, b, err
	}
	return "", a, nil
}

func (p *parser) statement() (Statement, error) {
	t := p.peek()
	if t.kind != tIdent {
		return nil, p.errorf("no viable alternative at input '%s'", p.peekText())
	}
	switch t.text {
	case "select":
		return p.selectStatement()
	case "insert":
		return p.insertStatement()
	case "update":
		return p.updateStatement()
	case "delete":
		return p.deleteStatement()
	case "begin":
		return p.batchStatement()
	case "use":
		p.next()
		ks, err := p.ident()
		return &Use{Keyspace: ks}, err
	case "truncate":
		p.next()
		p.word("table")
		ks, tbl, err := p.tableName()
		return &Truncate{Keyspace: ks, Table: tbl}, err
	case "create":
		switch {
		case p.isWordAt(1, "type"):
			return p.createType()
		case p.isWordAt(1, "keyspace"), p.isWordAt(1, "schema"):
			return p.createKeyspace()
		case p.isWordAt(1, "table"), p.isWordAt(1, "columnfamily"):
			return p.createTable()
		}
	case "alter":
		switch {
		case p.isWordAt(1, "type"):
			return p.alterType()
		case p.isWordAt(1, "keyspace"), p.isWordAt(1, "schema"):
			return p.alterKeyspace()
		case p.isWordAt(1, "table"), p.isWordAt(1, "columnfamily"):
			return p.alterTable()
		}
	case "drop":
		switch {
		case p.isWordAt(1, "type"):
			p.i += 2
			ifExists, err := p.ifExists()
			if err != nil {
				return nil, err
			}
			ks, name, err := p.tableName()
			return &DropType{Keyspace: ks, Name: name, IfExists: ifExists}, err
		case p.isWordAt(1, "keyspace"), p.isWordAt(1, "schema"):
			p.i += 2
			ifExists, err := p.ifExists()
			if err != nil {
				return nil, err
			}
			ks, err := p.ident()
			return &DropKeyspace{Keyspace: ks, IfExists: ifExists}, err
		case p.isWordAt(1, "table"), p.isWordAt(1, "columnfamily"):
			p.i += 2
			ifExists, err := p.ifExists()
			if err != nil {
				return nil, err
			}
			ks, tbl, err := p.tableName()
			return &DropTable{Keyspace: ks, Table: tbl, IfExists: ifExists}, err
		}
	}
	switch t.text {
	case "describe", "desc":
		return p.describe()
	case "create", "alter", "drop", "grant", "revoke", "list", "comment", "security":
		return nil, &UnsupportedError{Statement: strings.ToUpper(t.text)}
	}
	return nil, p.errorf("no viable alternative at input '%s'", p.peekText())
}

// UnsupportedError is a valid statement kind this server does not
// implement (roles, indexes, types, functions, views, ...).
type UnsupportedError struct{ Statement string }

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("%s statements of this kind are not supported", e.Statement)
}

func (p *parser) ifExists() (bool, error) {
	if p.word("if") {
		return true, p.expectWord("exists")
	}
	return false, nil
}

func (p *parser) ifNotExists() (bool, error) {
	if p.word("if") {
		return true, p.expectWord("not", "exists")
	}
	return false, nil
}

// ---- SELECT ----

func (p *parser) selectStatement() (*Select, error) {
	p.next()
	s := &Select{}
	if p.isWord("json") && !p.isFromNext(1) {
		p.next()
		s.JSON = true
	}
	if p.isWord("distinct") && !p.isFromNext(1) && !p.isPunctAt(1, ",") {
		p.next()
		s.Distinct = true
	}
	if !p.punct("*") {
		for {
			sel, err := p.selector()
			if err != nil {
				return nil, err
			}
			s.Selectors = append(s.Selectors, sel)
			if !p.punct(",") {
				break
			}
		}
	}
	if err := p.expectWord("from"); err != nil {
		return nil, err
	}
	var err error
	if s.Keyspace, s.Table, err = p.tableName(); err != nil {
		return nil, err
	}
	if p.word("where") {
		if s.Where, err = p.whereClause(); err != nil {
			return nil, err
		}
	}
	if p.isWord("group") {
		p.next()
		if err := p.expectWord("by"); err != nil {
			return nil, err
		}
		for {
			c, err := p.ident()
			if err != nil {
				return nil, err
			}
			s.GroupBy = append(s.GroupBy, c)
			if !p.punct(",") {
				break
			}
		}
	}
	if p.word("order") {
		if err := p.expectWord("by"); err != nil {
			return nil, err
		}
		for {
			o, err := p.ordering()
			if err != nil {
				return nil, err
			}
			s.OrderBy = append(s.OrderBy, o)
			if !p.punct(",") {
				break
			}
		}
	}
	if p.isWord("per") {
		p.next()
		if err := p.expectWord("partition", "limit"); err != nil {
			return nil, err
		}
		if s.PerPartitionLimit, err = p.intValue(); err != nil {
			return nil, err
		}
	}
	if p.word("limit") {
		if s.Limit, err = p.intValue(); err != nil {
			return nil, err
		}
	}
	if p.word("allow") {
		if err := p.expectWord("filtering"); err != nil {
			return nil, err
		}
		s.AllowFiltering = true
	}
	return s, nil
}

func (p *parser) isFromNext(n int) bool { return p.isWordAt(n, "from") }
func (p *parser) isPunctAt(n int, s string) bool {
	t := p.peekAt(n)
	return t.kind == tPunct && t.text == s
}

func (p *parser) ordering() (Ordering, error) {
	c, err := p.ident()
	if err != nil {
		return Ordering{}, err
	}
	o := Ordering{Column: c}
	if p.word("desc") {
		o.Desc = true
	} else {
		p.word("asc")
	}
	return o, nil
}

func (p *parser) intValue() (Term, error) {
	t := p.peek()
	switch t.kind {
	case tInteger:
		p.next()
		return &Literal{Kind: types.LitInteger, Text: t.text}, nil
	case tQMark:
		return p.marker(""), nil
	}
	if t.kind == tPunct && t.text == ":" {
		p.next()
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		return p.marker(name), nil
	}
	return nil, p.errorf("mismatched input '%s' expecting INTEGER", p.peekText())
}

func (p *parser) marker(name string) *Marker {
	if name == "" {
		p.next()
	}
	p.markers = append(p.markers, name)
	return &Marker{Index: len(p.markers) - 1}
}

func (p *parser) selector() (Selector, error) {
	e, err := p.additive()
	if err != nil {
		return Selector{}, err
	}
	s := Selector{Expr: e}
	if p.word("as") {
		if s.Alias, err = p.ident(); err != nil {
			return Selector{}, err
		}
	}
	return s, nil
}

// additive parses selector arithmetic (selectionAddition in Parser.g).
func (p *parser) additive() (Selectable, error) {
	l, err := p.multiplicative()
	if err != nil {
		return nil, err
	}
	for p.isPunct("+") || p.isPunct("-") {
		op := p.next().text[0]
		r, err := p.multiplicative()
		if err != nil {
			return nil, err
		}
		l = &OpSel{Op: op, Left: l, Right: r}
	}
	return l, nil
}

func (p *parser) multiplicative() (Selectable, error) {
	l, err := p.unary()
	if err != nil {
		return nil, err
	}
	for p.isPunct("*") || p.isPunct("/") || p.isPunct("%") {
		op := p.next().text[0]
		r, err := p.unary()
		if err != nil {
			return nil, err
		}
		l = &OpSel{Op: op, Left: l, Right: r}
	}
	return l, nil
}

func (p *parser) unary() (Selectable, error) {
	if p.punct("-") {
		e, err := p.unary()
		if err != nil {
			return nil, err
		}
		return &OpSel{Op: 'n', Left: e}, nil
	}
	if p.isPunct("(") && !p.isTypeHint() {
		p.next()
		e, err := p.additive()
		if err != nil {
			return nil, err
		}
		return e, p.expectPunct(")")
	}
	return p.selectable()
}

func (p *parser) isTypeHint() bool {
	t := p.peekAt(1)
	return t.kind == tIdent && p.isPunctAt(2, ")")
}

func (p *parser) selectable() (Selectable, error) {
	t := p.peek()
	if (t.kind == tIdent || t.kind == tQuoted) && p.isPunctAt(1, "(") {
		name := t.text
		p.i += 2
		f := &FuncSel{Name: name}
		if t.kind == tIdent {
			switch name {
			case "count":
				if p.punct("*") {
					f.Star = true
					return f, p.expectPunct(")")
				}
			case "cast":
				a, err := p.selectable()
				if err != nil {
					return nil, err
				}
				if err := p.expectWord("as"); err != nil {
					return nil, err
				}
				typ, err := p.ident()
				if err != nil {
					return nil, err
				}
				f.Args, f.Type = []Selectable{a}, typ
				return f, p.expectPunct(")")
			}
		}
		if !p.punct(")") {
			for {
				a, err := p.additive()
				if err != nil {
					return nil, err
				}
				f.Args = append(f.Args, a)
				if !p.punct(",") {
					break
				}
			}
			if err := p.expectPunct(")"); err != nil {
				return nil, err
			}
		}
		return f, nil
	}
	if t.kind == tIdent && !reserved[t.text] || t.kind == tQuoted {
		p.next()
		return &ColumnSel{Name: t.text}, nil
	}
	term, err := p.term()
	if err != nil {
		return nil, err
	}
	return &TermSel{Term: term}, nil
}

func (p *parser) whereClause() ([]Relation, error) {
	var out []Relation
	for {
		r, err := p.relation()
		if err != nil {
			return nil, err
		}
		out = append(out, r)
		if !p.word("and") {
			return out, nil
		}
	}
}

func (p *parser) relation() (Relation, error) {
	var r Relation
	switch {
	case p.isWord("token") && p.isPunctAt(1, "("):
		p.i += 2
		cols, err := p.identList()
		if err != nil {
			return r, err
		}
		r.Columns, r.Token = cols, true
	case p.isPunct("("):
		p.next()
		cols, err := p.identList()
		if err != nil {
			return r, err
		}
		r.Columns, r.Multi = cols, true
	default:
		c, err := p.ident()
		if err != nil {
			return r, err
		}
		r.Columns = []string{c}
	}
	t := p.peek()
	switch {
	case t.kind == tPunct && isComparison(t.text):
		p.next()
		r.Op = t.text
	case p.word("in"):
		r.Op = "IN"
		if p.isPunct("(") {
			p.next()
			l := &ListLiteral{}
			if !p.punct(")") {
				for {
					v, err := p.term()
					if err != nil {
						return r, err
					}
					l.Elems = append(l.Elems, v)
					if !p.punct(",") {
						break
					}
				}
				if err := p.expectPunct(")"); err != nil {
					return r, err
				}
			}
			r.Value = l
			return r, nil
		}
	case p.word("contains"):
		r.Op = "CONTAINS"
		if p.word("key") {
			r.Op = "CONTAINS KEY"
		}
	case p.word("like"):
		r.Op = "LIKE"
	case p.word("is"):
		r.Op = "IS NOT NULL"
		return r, p.expectWord("not", "null")
	default:
		return r, p.errorf("no viable alternative at input '%s'", p.peekText())
	}
	var err error
	r.Value, err = p.term()
	return r, err
}

func isComparison(s string) bool {
	switch s {
	case "=", "<", "<=", ">", ">=", "!=":
		return true
	}
	return false
}

// identList reads "a, b, c)".
func (p *parser) identList() ([]string, error) {
	var out []string
	for {
		c, err := p.ident()
		if err != nil {
			return nil, err
		}
		out = append(out, c)
		if !p.punct(",") {
			break
		}
	}
	return out, p.expectPunct(")")
}

// ---- terms ----

func (p *parser) term() (Term, error) {
	t := p.peek()
	switch t.kind {
	case tString:
		p.next()
		return &Literal{Kind: types.LitString, Text: t.text}, nil
	case tInteger:
		p.next()
		return &Literal{Kind: types.LitInteger, Text: t.text}, nil
	case tFloat:
		p.next()
		return &Literal{Kind: types.LitFloat, Text: t.text}, nil
	case tBoolean:
		p.next()
		return &Literal{Kind: types.LitBoolean, Text: t.text}, nil
	case tUUID:
		p.next()
		return &Literal{Kind: types.LitUUID, Text: t.text}, nil
	case tHex:
		p.next()
		return &Literal{Kind: types.LitHex, Text: t.text}, nil
	case tDuration:
		p.next()
		return &Literal{Kind: types.LitDuration, Text: t.text}, nil
	case tQMark:
		return p.marker(""), nil
	case tIdent, tQuoted:
		if t.kind == tIdent && t.text == "null" {
			p.next()
			return &Null{}, nil
		}
		if p.isPunctAt(1, "(") || p.isPunctAt(1, ".") {
			return p.functionCall()
		}
	case tPunct:
		switch t.text {
		case ":":
			p.next()
			name, err := p.ident()
			if err != nil {
				return nil, err
			}
			return p.marker(name), nil
		case "[":
			p.next()
			l := &ListLiteral{}
			if p.punct("]") {
				return l, nil
			}
			for {
				v, err := p.term()
				if err != nil {
					return nil, err
				}
				l.Elems = append(l.Elems, v)
				if !p.punct(",") {
					break
				}
			}
			return l, p.expectPunct("]")
		case "{":
			return p.mapOrSet()
		case "(":
			// (type) term, or a tuple
			if typ, ok := p.castType(); ok {
				v, err := p.term()
				return &Cast{Type: typ, Term: v}, err
			}
			p.next()
			tl := &TupleLiteral{}
			for {
				v, err := p.term()
				if err != nil {
					return nil, err
				}
				tl.Elems = append(tl.Elems, v)
				if !p.punct(",") {
					break
				}
			}
			return tl, p.expectPunct(")")
		case "-":
			// -NaN and -Infinity lex as floats; nothing else is negated.
		}
	}
	return nil, p.errorf("no viable alternative at input '%s'", p.peekText())
}

// castType recognizes "(nativetype)" at a term position.
func (p *parser) castType() (string, bool) {
	t := p.peekAt(1)
	if t.kind != tIdent || !p.isPunctAt(2, ")") {
		return "", false
	}
	if _, err := types.Parse(t.text); err != nil {
		return "", false
	}
	p.i += 3
	return t.text, true
}

func (p *parser) functionCall() (Term, error) {
	name, err := p.ident()
	if err != nil {
		if p.isWord("token") {
			p.next()
			name = "token"
		} else {
			return nil, err
		}
	}
	if p.punct(".") {
		fn, err := p.ident()
		if err != nil {
			return nil, err
		}
		name += "." + fn
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	f := &FunctionCall{Name: name}
	if p.punct(")") {
		return f, nil
	}
	for {
		v, err := p.term()
		if err != nil {
			return nil, err
		}
		f.Args = append(f.Args, v)
		if !p.punct(",") {
			break
		}
	}
	return f, p.expectPunct(")")
}

func (p *parser) mapOrSet() (Term, error) {
	p.next() // {
	if p.punct("}") {
		return &MapLiteral{}, nil
	}
	if t := p.peek(); (t.kind == tIdent && t.text != "null" || t.kind == tQuoted) && p.isPunctAt(1, ":") {
		u := &UDTLiteral{}
		for {
			f, err := p.ident()
			if err != nil {
				return nil, err
			}
			if err := p.expectPunct(":"); err != nil {
				return nil, err
			}
			v, err := p.term()
			if err != nil {
				return nil, err
			}
			u.Fields, u.Values = append(u.Fields, f), append(u.Values, v)
			if !p.punct(",") {
				break
			}
		}
		return u, p.expectPunct("}")
	}
	first, err := p.term()
	if err != nil {
		return nil, err
	}
	if p.punct(":") {
		m := &MapLiteral{}
		k := first
		for {
			v, err := p.term()
			if err != nil {
				return nil, err
			}
			m.Keys, m.Values = append(m.Keys, k), append(m.Values, v)
			if !p.punct(",") {
				break
			}
			if k, err = p.term(); err != nil {
				return nil, err
			}
			if err := p.expectPunct(":"); err != nil {
				return nil, err
			}
		}
		return m, p.expectPunct("}")
	}
	s := &SetLiteral{Elems: []Term{first}}
	for p.punct(",") {
		v, err := p.term()
		if err != nil {
			return nil, err
		}
		s.Elems = append(s.Elems, v)
	}
	return s, p.expectPunct("}")
}

// ---- INSERT / UPDATE / DELETE / BATCH ----

func (p *parser) using(u *Using, onlyTimestamp bool) error {
	if !p.word("using") {
		return nil
	}
	for {
		switch {
		case p.word("timestamp"):
			v, err := p.intValue()
			if err != nil {
				return err
			}
			u.Timestamp = v
		case !onlyTimestamp && p.word("ttl"):
			v, err := p.intValue()
			if err != nil {
				return err
			}
			u.TTL = v
		default:
			return p.errorf("mismatched input '%s' expecting TIMESTAMP or TTL", p.peekText())
		}
		if !p.word("and") {
			return nil
		}
	}
}

func (p *parser) insertStatement() (*Insert, error) {
	p.next()
	if err := p.expectWord("into"); err != nil {
		return nil, err
	}
	s := &Insert{}
	var err error
	if s.Keyspace, s.Table, err = p.tableName(); err != nil {
		return nil, err
	}
	if p.word("json") {
		if s.JSON, err = p.term(); err != nil {
			return nil, err
		}
		if p.word("default") {
			switch {
			case p.word("unset"):
				s.DefaultUnset = true
			case p.word("null"):
			default:
				return nil, p.errorf("mismatched input '%s' expecting NULL or UNSET", p.peekText())
			}
		}
	} else {
		if err := p.expectPunct("("); err != nil {
			return nil, err
		}
		if s.Columns, err = p.identList(); err != nil {
			return nil, err
		}
		if err := p.expectWord("values"); err != nil {
			return nil, err
		}
		if err := p.expectPunct("("); err != nil {
			return nil, err
		}
		for {
			v, err := p.term()
			if err != nil {
				return nil, err
			}
			s.Values = append(s.Values, v)
			if !p.punct(",") {
				break
			}
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
	}
	if s.IfNotExists, err = p.ifNotExists(); err != nil {
		return nil, err
	}
	return s, p.using(&s.Using, false)
}

func (p *parser) updateStatement() (*Update, error) {
	p.next()
	s := &Update{}
	var err error
	if s.Keyspace, s.Table, err = p.tableName(); err != nil {
		return nil, err
	}
	if err := p.using(&s.Using, false); err != nil {
		return nil, err
	}
	if err := p.expectWord("set"); err != nil {
		return nil, err
	}
	for {
		op, err := p.operation()
		if err != nil {
			return nil, err
		}
		s.Operations = append(s.Operations, op)
		if !p.punct(",") {
			break
		}
	}
	if err := p.expectWord("where"); err != nil {
		return nil, err
	}
	if s.Where, err = p.whereClause(); err != nil {
		return nil, err
	}
	s.IfExists, s.Conditions, err = p.conditions()
	return s, err
}

func (p *parser) operation() (Operation, error) {
	c, err := p.ident()
	if err != nil {
		return Operation{}, err
	}
	op := Operation{Column: c}
	switch {
	case p.punct("["):
		if op.Key, err = p.term(); err != nil {
			return op, err
		}
		if err := p.expectPunct("]"); err != nil {
			return op, err
		}
	case p.punct("."):
		if op.Field, err = p.ident(); err != nil {
			return op, err
		}
	}
	// c += v and c -= v lex as '+' '=' and '-' '='.
	if p.isPunct("+") || p.isPunct("-") {
		op.Op = p.next().text + "="
		if err := p.expectPunct("="); err != nil {
			return op, err
		}
		op.Value, err = p.term()
		return op, err
	}
	if err := p.expectPunct("="); err != nil {
		return op, err
	}
	op.Op = "="
	// c = c + v, c = c - v, c = v + c
	if t := p.peek(); (t.kind == tIdent || t.kind == tQuoted) && t.text == c && op.Key == nil &&
		(p.isPunctAt(1, "+") || p.isPunctAt(1, "-") || p.peekAt(1).kind == tInteger && strings.HasPrefix(p.peekAt(1).text, "-")) {
		p.next()
		if p.punct("+") {
			op.Op = "+"
		} else if p.punct("-") {
			op.Op = "-"
		} else {
			// c = c -1 lexes the minus into the integer.
			op.Op = "+"
		}
		op.Value, err = p.term()
		return op, err
	}
	if op.Value, err = p.term(); err != nil {
		return op, err
	}
	if p.punct("+") {
		other, err := p.ident()
		if err != nil {
			return op, err
		}
		if other != c {
			return op, p.errorf("only expressions of the form X = <value> + X are supported")
		}
		op.Op = "prepend"
	}
	return op, nil
}

// conditions reads IF EXISTS or IF <conditions>.
func (p *parser) conditions() (bool, []Condition, error) {
	if !p.word("if") {
		return false, nil, nil
	}
	if p.word("exists") {
		return true, nil, nil
	}
	var out []Condition
	for {
		c, err := p.ident()
		if err != nil {
			return false, nil, err
		}
		t := p.next()
		cond := Condition{Column: c, Op: t.text}
		switch {
		case t.kind == tPunct && isComparison(t.text):
		case t.kind == tIdent && t.text == "in":
			cond.Op = "IN"
		default:
			return false, nil, p.errorf("no viable alternative at input '%s'", t.text)
		}
		if cond.Value, err = p.term(); err != nil {
			return false, nil, err
		}
		out = append(out, cond)
		if !p.word("and") {
			return false, out, nil
		}
	}
}

func (p *parser) deleteStatement() (*Delete, error) {
	p.next()
	s := &Delete{}
	if !p.isWord("from") {
		for {
			c, err := p.ident()
			if err != nil {
				return nil, err
			}
			d := Deletion{Column: c}
			switch {
			case p.punct("["):
				if d.Key, err = p.term(); err != nil {
					return nil, err
				}
				if err := p.expectPunct("]"); err != nil {
					return nil, err
				}
			case p.punct("."):
				if d.Field, err = p.ident(); err != nil {
					return nil, err
				}
			}
			s.Columns = append(s.Columns, d)
			if !p.punct(",") {
				break
			}
		}
	}
	if err := p.expectWord("from"); err != nil {
		return nil, err
	}
	var err error
	if s.Keyspace, s.Table, err = p.tableName(); err != nil {
		return nil, err
	}
	if err := p.using(&s.Using, true); err != nil {
		return nil, err
	}
	if err := p.expectWord("where"); err != nil {
		return nil, err
	}
	if s.Where, err = p.whereClause(); err != nil {
		return nil, err
	}
	s.IfExists, s.Conditions, err = p.conditions()
	return s, err
}

func (p *parser) batchStatement() (*Batch, error) {
	p.next()
	b := &Batch{Type: "LOGGED"}
	switch {
	case p.word("unlogged"):
		b.Type = "UNLOGGED"
	case p.word("counter"):
		b.Type = "COUNTER"
	}
	if err := p.expectWord("batch"); err != nil {
		return nil, err
	}
	if err := p.using(&b.Using, false); err != nil {
		return nil, err
	}
	for !p.isWord("apply") {
		var st Statement
		var err error
		switch {
		case p.isWord("insert"):
			st, err = p.insertStatement()
		case p.isWord("update"):
			st, err = p.updateStatement()
		case p.isWord("delete"):
			st, err = p.deleteStatement()
		default:
			return nil, p.errorf("no viable alternative at input '%s'", p.peekText())
		}
		if err != nil {
			return nil, err
		}
		b.Statements = append(b.Statements, st)
		for p.punct(";") {
		}
	}
	p.next()
	return b, p.expectWord("batch")
}

// ---- DDL ----

func (p *parser) properties() ([]Property, error) {
	var out []Property
	for {
		prop, err := p.property()
		if err != nil {
			return nil, err
		}
		out = append(out, prop)
		if !p.word("and") {
			return out, nil
		}
	}
}

func (p *parser) property() (Property, error) {
	name, err := p.ident()
	if err != nil {
		return Property{}, err
	}
	if err := p.expectPunct("="); err != nil {
		return Property{}, err
	}
	if t := p.peek(); t.kind == tIdent || t.kind == tQuoted {
		// Unquoted identifiers are accepted as string values.
		p.next()
		return Property{Name: name, Value: &Literal{Kind: types.LitString, Text: t.text}}, nil
	}
	v, err := p.term()
	return Property{Name: name, Value: v}, err
}

func (p *parser) createKeyspace() (*CreateKeyspace, error) {
	p.i += 2
	s := &CreateKeyspace{}
	var err error
	if s.IfNotExists, err = p.ifNotExists(); err != nil {
		return nil, err
	}
	if s.Keyspace, err = p.ident(); err != nil {
		return nil, err
	}
	if err := p.expectWord("with"); err != nil {
		return nil, err
	}
	s.Properties, err = p.properties()
	return s, err
}

func (p *parser) alterKeyspace() (*AlterKeyspace, error) {
	p.i += 2
	s := &AlterKeyspace{}
	var err error
	if s.IfExists, err = p.ifExists(); err != nil {
		return nil, err
	}
	if s.Keyspace, err = p.ident(); err != nil {
		return nil, err
	}
	if err := p.expectWord("with"); err != nil {
		return nil, err
	}
	s.Properties, err = p.properties()
	return s, err
}

// typeText reads a type ("int", "frozen<map<text, int>>") and returns its
// text.
func (p *parser) typeText() (string, error) {
	t := p.peek()
	if t.kind != tIdent {
		return "", p.errorf("no viable alternative at input '%s'", p.peekText())
	}
	start := t.pos
	p.next()
	end := start + len(t.text)
	if p.isPunct("<") {
		depth := 0
		for {
			t := p.next()
			if t.kind == tEOF {
				return "", p.errorf("unterminated type")
			}
			if t.kind == tPunct && t.text == "<" {
				depth++
			}
			if t.kind == tPunct && t.text == ">" {
				depth--
				if depth == 0 {
					end = t.pos + 1
					break
				}
			}
		}
	}
	return p.src[start:end], nil
}

func (p *parser) createTable() (*CreateTable, error) {
	p.i += 2
	s := &CreateTable{}
	var err error
	if s.IfNotExists, err = p.ifNotExists(); err != nil {
		return nil, err
	}
	if s.Keyspace, s.Table, err = p.tableName(); err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	for {
		if p.isWord("primary") {
			p.next()
			if err := p.expectWord("key"); err != nil {
				return nil, err
			}
			if err := p.expectPunct("("); err != nil {
				return nil, err
			}
			if p.punct("(") {
				if s.PartitionKey, err = p.identList(); err != nil {
					return nil, err
				}
			} else {
				c, err := p.ident()
				if err != nil {
					return nil, err
				}
				s.PartitionKey = []string{c}
			}
			for p.punct(",") {
				c, err := p.ident()
				if err != nil {
					return nil, err
				}
				s.Clustering = append(s.Clustering, c)
			}
			if err := p.expectPunct(")"); err != nil {
				return nil, err
			}
		} else {
			name, err := p.ident()
			if err != nil {
				return nil, err
			}
			typ, err := p.typeText()
			if err != nil {
				return nil, err
			}
			col := ColumnDef{Name: name, Type: typ}
			if p.word("static") {
				col.Static = true
			}
			if p.word("primary") {
				if err := p.expectWord("key"); err != nil {
					return nil, err
				}
				if s.PartitionKey != nil {
					return nil, p.errorf("multiple PRIMARY KEYs specified (exactly one required)")
				}
				s.PartitionKey = []string{name}
			}
			s.Columns = append(s.Columns, col)
		}
		if !p.punct(",") {
			break
		}
		if p.isPunct(")") {
			break
		}
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	if p.word("with") {
		for {
			switch {
			case p.isWord("clustering") && p.isWordAt(1, "order"):
				p.i += 2
				if err := p.expectWord("by"); err != nil {
					return nil, err
				}
				if err := p.expectPunct("("); err != nil {
					return nil, err
				}
				for {
					o, err := p.ordering()
					if err != nil {
						return nil, err
					}
					s.ClusteringOrder = append(s.ClusteringOrder, o)
					if !p.punct(",") {
						break
					}
				}
				if err := p.expectPunct(")"); err != nil {
					return nil, err
				}
			case p.isWord("compact") && p.isWordAt(1, "storage"):
				p.i += 2
				s.CompactStorage = true
			default:
				prop, err := p.property()
				if err != nil {
					return nil, err
				}
				s.Properties = append(s.Properties, prop)
			}
			if !p.word("and") {
				break
			}
		}
	}
	return s, nil
}

func (p *parser) alterTable() (*AlterTable, error) {
	p.i += 2
	s := &AlterTable{}
	var err error
	if s.IfExists, err = p.ifExists(); err != nil {
		return nil, err
	}
	if s.Keyspace, s.Table, err = p.tableName(); err != nil {
		return nil, err
	}
	switch {
	case p.word("add"):
		p.ifNotExists()
		paren := p.punct("(")
		for {
			name, err := p.ident()
			if err != nil {
				return nil, err
			}
			typ, err := p.typeText()
			if err != nil {
				return nil, err
			}
			col := ColumnDef{Name: name, Type: typ, Static: p.word("static")}
			s.Add = append(s.Add, col)
			if !paren || !p.punct(",") {
				break
			}
		}
		if paren {
			return s, p.expectPunct(")")
		}
	case p.word("drop"):
		p.ifExists()
		if p.punct("(") {
			s.Drop, err = p.identList()
			return s, err
		}
		c, err := p.ident()
		if err != nil {
			return nil, err
		}
		s.Drop = []string{c}
	case p.word("rename"):
		p.ifExists()
		for {
			from, err := p.ident()
			if err != nil {
				return nil, err
			}
			if err := p.expectWord("to"); err != nil {
				return nil, err
			}
			to, err := p.ident()
			if err != nil {
				return nil, err
			}
			s.Rename = append(s.Rename, [2]string{from, to})
			if !p.word("and") {
				break
			}
		}
	case p.word("with"):
		s.Properties, err = p.properties()
		return s, err
	default:
		return nil, &UnsupportedError{Statement: "ALTER TABLE"}
	}
	return s, nil
}

func (p *parser) describe() (*Describe, error) {
	p.next()
	d := &Describe{}
	switch {
	case p.word("full"):
		d.Full = true
		if err := p.expectWord("schema"); err != nil {
			return nil, err
		}
		d.Kind = "SCHEMA"
	case p.word("schema"):
		d.Kind = "SCHEMA"
	case p.word("cluster"):
		d.Kind = "CLUSTER"
	case p.isWord("keyspaces"), p.isWord("tables"), p.isWord("types"), p.isWord("functions"), p.isWord("aggregates"):
		d.Kind = strings.ToUpper(p.next().text)
	case p.isWord("only") || p.isWord("keyspace"):
		d.Only = p.word("only")
		if err := p.expectWord("keyspace"); err != nil {
			return nil, err
		}
		d.Kind = "KEYSPACE"
		if t := p.peek(); t.kind == tIdent && t.text != "with" || t.kind == tQuoted {
			ks, err := p.ident()
			if err != nil {
				return nil, err
			}
			d.Keyspace = ks
		}
	case p.word("table"), p.word("columnfamily"):
		d.Kind = "TABLE"
		var err error
		if d.Keyspace, d.Name, err = p.tableName(); err != nil {
			return nil, err
		}
	default:
		var err error
		if d.Keyspace, d.Name, err = p.tableName(); err != nil {
			return nil, err
		}
	}
	if p.word("with") {
		if err := p.expectWord("internals"); err != nil {
			return nil, err
		}
		d.Internals = true
	}
	return d, nil
}

func (p *parser) createType() (*CreateType, error) {
	p.i += 2
	s := &CreateType{}
	var err error
	if s.IfNotExists, err = p.ifNotExists(); err != nil {
		return nil, err
	}
	if s.Keyspace, s.Name, err = p.tableName(); err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	for {
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		typ, err := p.typeText()
		if err != nil {
			return nil, err
		}
		s.Fields = append(s.Fields, ColumnDef{Name: name, Type: typ})
		if !p.punct(",") {
			break
		}
	}
	return s, p.expectPunct(")")
}

func (p *parser) alterType() (*AlterType, error) {
	p.i += 2
	s := &AlterType{}
	var err error
	if s.IfExists, err = p.ifExists(); err != nil {
		return nil, err
	}
	if s.Keyspace, s.Name, err = p.tableName(); err != nil {
		return nil, err
	}
	switch {
	case p.word("add"):
		p.ifNotExists()
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		typ, err := p.typeText()
		if err != nil {
			return nil, err
		}
		s.Add = []ColumnDef{{Name: name, Type: typ}}
	case p.word("rename"):
		p.ifExists()
		for {
			from, err := p.ident()
			if err != nil {
				return nil, err
			}
			if err := p.expectWord("to"); err != nil {
				return nil, err
			}
			to, err := p.ident()
			if err != nil {
				return nil, err
			}
			s.Rename = append(s.Rename, [2]string{from, to})
			if !p.word("and") {
				break
			}
		}
	default:
		return nil, &UnsupportedError{Statement: "ALTER TYPE"}
	}
	return s, nil
}
