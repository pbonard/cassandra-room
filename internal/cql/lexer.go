package cql

import (
	"fmt"
	"strings"
)

// tokKind is a lexical token type (Cassandra's Lexer.g).
type tokKind uint8

const (
	tEOF tokKind = iota
	tIdent
	tQuoted // "Quoted Name"
	tString
	tInteger
	tFloat
	tBoolean
	tUUID
	tHex
	tDuration
	tQMark
	tPunct // ( ) , ; . * = < > <= >= != [ ] { } : + - / % ..
)

type token struct {
	kind tokKind
	text string // identifiers lowercased; strings unquoted
	pos  int
}

// SyntaxError is a CQL syntax error.
type SyntaxError struct {
	Pos int
	Msg string
}

func (e *SyntaxError) Error() string { return fmt.Sprintf("line 1:%d %s", e.Pos, e.Msg) }

func lex(s string) ([]token, error) {
	var out []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
			continue
		case c == '-' && i+1 < len(s) && s[i+1] == '-', c == '/' && i+1 < len(s) && s[i+1] == '/':
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			j := strings.Index(s[i+2:], "*/")
			if j < 0 {
				return nil, &SyntaxError{i, "unterminated comment"}
			}
			i += j + 4
			continue
		}
		start := i
		if t, n, ok := lexValue(s[i:]); ok {
			t.pos = start
			out = append(out, t)
			i += n
			continue
		}
		switch {
		case c == '\'':
			var b strings.Builder
			i++
			for {
				if i >= len(s) {
					return nil, &SyntaxError{start, "unterminated string literal"}
				}
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(s[i])
				i++
			}
			out = append(out, token{tString, b.String(), start})
		case c == '$' && strings.HasPrefix(s[i:], "$$"):
			j := strings.Index(s[i+2:], "$$")
			if j < 0 {
				return nil, &SyntaxError{start, "unterminated string literal"}
			}
			out = append(out, token{tString, s[i+2 : i+2+j], start})
			i += j + 4
		case c == '"':
			var b strings.Builder
			i++
			for {
				if i >= len(s) {
					return nil, &SyntaxError{start, "unterminated quoted name"}
				}
				if s[i] == '"' {
					if i+1 < len(s) && s[i+1] == '"' {
						b.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(s[i])
				i++
			}
			out = append(out, token{tQuoted, b.String(), start})
		case isLetter(c):
			j := i + 1
			for j < len(s) && (isLetter(s[j]) || isDigit(s[j]) || s[j] == '_') {
				j++
			}
			word := s[i:j]
			lw := strings.ToLower(word)
			switch {
			case lw == "true" || lw == "false":
				out = append(out, token{tBoolean, lw, start})
			case lw == "nan" || lw == "infinity":
				out = append(out, token{tFloat, map[string]string{"nan": "NaN", "infinity": "Infinity"}[lw], start})
			default:
				out = append(out, token{tIdent, lw, start})
			}
			i = j
		case c == '?':
			out = append(out, token{tQMark, "?", start})
			i++
		default:
			p := string(c)
			if i+1 < len(s) {
				switch two := s[i : i+2]; two {
				case "<=", ">=", "!=", "..":
					p = two
				}
			}
			if !strings.Contains("(),;.*=<>[]{}:+-/%", p[:1]) {
				return nil, &SyntaxError{start, fmt.Sprintf("token recognition error at: '%c'", c)}
			}
			out = append(out, token{tPunct, p, start})
			i += len(p)
		}
	}
	out = append(out, token{kind: tEOF, pos: len(s)})
	return out, nil
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isHex(c byte) bool    { return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

// lexValue matches the constants that need longest-match care: UUIDs,
// hex numbers, durations, floats and integers (with an optional leading
// minus, as Lexer.g's INTEGER).
func lexValue(s string) (token, int, bool) {
	if n := matchUUID(s); n > 0 {
		return token{kind: tUUID, text: strings.ToLower(s[:n])}, n, true
	}
	if len(s) > 1 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		n := 2
		for n < len(s) && isHex(s[n]) {
			n++
		}
		if n == len(s) || !(isLetter(s[n]) || s[n] == '_') {
			return token{kind: tHex, text: s[:n]}, n, true
		}
	}
	if n := matchDuration(s); n > 0 {
		return token{kind: tDuration, text: s[:n]}, n, true
	}
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
		rest := strings.ToLower(s[i:])
		for _, w := range []string{"nan", "infinity"} {
			if strings.HasPrefix(rest, w) && (len(rest) == len(w) || !isIdentChar(rest[len(w)])) {
				return token{kind: tFloat, text: "-" + map[string]string{"nan": "NaN", "infinity": "Infinity"}[w]}, i + len(w), true
			}
		}
	}
	d := i
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == d {
		return token{}, 0, false
	}
	kind := tInteger
	if i < len(s) && s[i] == '.' && !(i+1 < len(s) && s[i+1] == '.') {
		kind = tFloat
		i++
		for i < len(s) && isDigit(s[i]) {
			i++
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && isDigit(s[j]) {
			for j < len(s) && isDigit(s[j]) {
				j++
			}
			kind, i = tFloat, j
		}
	}
	return token{kind: kind, text: s[:i]}, i, true
}

func isIdentChar(c byte) bool { return isLetter(c) || isDigit(c) || c == '_' }

func matchUUID(s string) int {
	if len(s) < 36 {
		return 0
	}
	for i := 0; i < 36; i++ {
		switch i {
		case 8, 13, 18, 23:
			if s[i] != '-' {
				return 0
			}
		default:
			if !isHex(s[i]) {
				return 0
			}
		}
	}
	if len(s) > 36 && isIdentChar(s[36]) {
		return 0
	}
	return 36
}

// matchDuration matches DURATION: digits and units ("1h30m"), or the ISO
// 8601 forms ("P1Y2D", "PT1H", "P2W"), with an optional minus.
func matchDuration(s string) int {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	if i < len(s) && isDigit(s[i]) {
		start := i
		for i < len(s) && isDigit(s[i]) {
			j := i
			for j < len(s) && isDigit(s[j]) {
				j++
			}
			u := unitLen(s[j:])
			if u == 0 {
				break
			}
			i = j + u
		}
		if i == start || i < len(s) && isIdentChar(s[i]) {
			return 0
		}
		return i
	}
	if i < len(s) && s[i] == 'P' {
		j := i + 1
		for j < len(s) && (isIdentChar(s[j])) {
			j++
		}
		if isISODuration(s[i:j]) {
			return j
		}
	}
	return 0
}

func unitLen(s string) int {
	l := strings.ToLower(s)
	for _, u := range []string{"mo", "ms", "us", "ns", "µs", "y", "w", "d", "h", "m", "s"} {
		if strings.HasPrefix(l, u) {
			return len(u)
		}
	}
	return 0
}

func isISODuration(s string) bool {
	if len(s) < 3 || s[0] != 'P' {
		return false
	}
	s = s[1:]
	if strings.HasSuffix(s, "W") {
		return allDigits(s[:len(s)-1])
	}
	date, tm, hasT := strings.Cut(s, "T")
	ok := func(part, units string) bool {
		if part == "" {
			return true
		}
		u := 0
		for len(part) > 0 {
			j := 0
			for j < len(part) && isDigit(part[j]) {
				j++
			}
			if j == 0 || j == len(part) {
				return false
			}
			k := strings.IndexByte(units[u:], part[j])
			if k < 0 {
				return false
			}
			u += k + 1
			part = part[j+1:]
		}
		return true
	}
	if hasT && tm == "" || !hasT && date == "" {
		return false
	}
	return ok(date, "YMD") && ok(tm, "HMS")
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}
