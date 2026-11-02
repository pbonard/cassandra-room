package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"
)

// ToJSON renders a value as JSON (AbstractType.toJSONString); nil is null.
func ToJSON(t *Type, v []byte) string {
	if v == nil {
		return "null"
	}
	if len(v) == 0 && t.Kind != Text && t.Kind != Ascii && t.Kind != Blob {
		return "null"
	}
	str := func(s string) string {
		b, _ := json.Marshal(s)
		return string(b)
	}
	switch t.Kind {
	case Text, Ascii:
		return str(string(v))
	case Boolean:
		return strconv.FormatBool(v[0] != 0)
	case Tinyint:
		return strconv.Itoa(int(int8(v[0])))
	case Smallint:
		return strconv.Itoa(int(int16(binary.BigEndian.Uint16(v))))
	case Int:
		return strconv.Itoa(int(int32(binary.BigEndian.Uint32(v))))
	case Bigint, Counter:
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(v)), 10)
	case Float, Double:
		var f float64
		bits := 64
		if t.Kind == Float {
			f, bits = float64(math.Float32frombits(binary.BigEndian.Uint32(v))), 32
		} else {
			f = math.Float64frombits(binary.BigEndian.Uint64(v))
		}
		switch {
		case math.IsNaN(f):
			return "NaN"
		case math.IsInf(f, 1):
			return "Infinity"
		case math.IsInf(f, -1):
			return "-Infinity"
		}
		return JavaFloatString(f, bits)
	case Varint:
		return VarintValue(v).String()
	case Decimal:
		return decimalString(v)
	case UUID, Timeuuid:
		return str(FormatUUID(v))
	case Timestamp:
		return str(time.UnixMilli(int64(binary.BigEndian.Uint64(v))).UTC().Format("2006-01-02 15:04:05.000Z"))
	case Date:
		days := int64(binary.BigEndian.Uint32(v)) - dateEpoch
		return str(time.Unix(days*86400, 0).UTC().Format("2006-01-02"))
	case Time:
		ns := int64(binary.BigEndian.Uint64(v))
		return str(fmt.Sprintf("%02d:%02d:%02d.%09d", ns/3600e9, ns/60e9%60, ns/1e9%60, ns%1e9))
	case Inet:
		return str(net.IP(v).String())
	case Blob:
		return str("0x" + hex.EncodeToString(v))
	case Duration:
		m, d, n, err := decodeDuration(v)
		if err != nil {
			return "null"
		}
		return str(formatDuration(m, d, n))
	case List, Set:
		elems, err := SplitCollection(v, 1)
		if err != nil {
			return "null"
		}
		parts := make([]string, len(elems))
		for i, e := range elems {
			parts[i] = ToJSON(t.Elems[0], e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case Map:
		elems, err := SplitCollection(v, 2)
		if err != nil {
			return "null"
		}
		var parts []string
		for i := 0; i < len(elems); i += 2 {
			k := ToJSON(t.Elems[0], elems[i])
			if !strings.HasPrefix(k, `"`) {
				k = str(k)
			}
			parts = append(parts, k+": "+ToJSON(t.Elems[1], elems[i+1]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case Vector:
		elems, err := SplitVector(t, v)
		if err != nil {
			return "null"
		}
		parts := make([]string, len(elems))
		for i, e := range elems {
			parts[i] = ToJSON(t.Elems[0], e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case Tuple, UDT:
		fields, err := SplitTuple(v)
		if err != nil {
			return "null"
		}
		var parts []string
		for i, e := range t.Elems {
			var f []byte
			if i < len(fields) {
				f = fields[i]
			}
			if t.Kind == UDT {
				parts = append(parts, str(jsonFieldName(t.Fields[i]))+": "+ToJSON(e, f))
			} else {
				parts = append(parts, ToJSON(e, f))
			}
		}
		if t.Kind == UDT {
			return "{" + strings.Join(parts, ", ") + "}"
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return "null"
}

// jsonFieldName is a column or field name as JSON keys write it: quoted
// with double quotes inside the key if CQL needs it.
func jsonFieldName(n string) string { return QuoteName(n) }

// JSONName is a column's key in SELECT JSON results and INSERT JSON input.
func JSONName(n string) string { return jsonFieldName(n) }

func decimalString(v []byte) string {
	if len(v) < 4 {
		return "null"
	}
	scale := int(int32(binary.BigEndian.Uint32(v)))
	s := VarintValue(v[4:]).String()
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	switch {
	case scale <= 0:
		s += strings.Repeat("0", -scale)
	case scale >= len(s):
		s = "0." + strings.Repeat("0", scale-len(s)) + s
	default:
		s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

func formatDuration(months, days, nanos int64) string {
	if months == 0 && days == 0 && nanos == 0 {
		return "0s"
	}
	var b strings.Builder
	if months < 0 || days < 0 || nanos < 0 {
		b.WriteByte('-')
		months, days, nanos = -months, -days, -nanos
	}
	unit := func(n int64, u string) {
		if n != 0 {
			fmt.Fprintf(&b, "%d%s", n, u)
		}
	}
	unit(months/12, "y")
	unit(months%12, "mo")
	unit(days, "d")
	unit(nanos/3600e9, "h")
	unit(nanos/60e9%60, "m")
	unit(nanos/1e9%60, "s")
	unit(nanos/1e6%1000, "ms")
	unit(nanos/1e3%1000, "us")
	unit(nanos%1000, "ns")
	return b.String()
}

// ParseJSON decodes JSON text with numbers kept exact.
func ParseJSON(s string) (any, error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, fmt.Errorf("Could not decode JSON string as a map: %v. (String was: %s)", err, s)
	}
	return v, nil
}

// FromJSON converts a decoded JSON value to a serialized value of t
// (AbstractType.fromJSONObject); JSON null gives nil.
func FromJSON(t *Type, v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	bad := func() ([]byte, error) {
		return nil, fmt.Errorf("Unable to make %s from '%v'", t, v)
	}
	switch t.Kind {
	case Text, Ascii:
		s, ok := v.(string)
		if !ok {
			return bad()
		}
		return FromString(t, s)
	case Boolean:
		switch x := v.(type) {
		case bool:
			return []byte{boolByte(x)}, nil
		case string:
			return FromString(t, x)
		}
		return bad()
	case Tinyint, Smallint, Int, Bigint, Counter, Varint, Decimal, Float, Double, Timestamp, Date, Time:
		switch x := v.(type) {
		case json.Number:
			k := LitInteger
			if strings.ContainsAny(string(x), ".eE") {
				k = LitFloat
			}
			return FromLiteral(t, k, string(x))
		case string:
			return FromString(t, x)
		}
		return bad()
	case UUID, Timeuuid, Inet, Duration:
		s, ok := v.(string)
		if !ok {
			return bad()
		}
		return FromString(t, s)
	case Blob:
		s, ok := v.(string)
		if !ok || !strings.HasPrefix(s, "0x") {
			return nil, fmt.Errorf("Value '%v' is not a valid blob representation: String representation of blob is missing 0x prefix", v)
		}
		return FromString(t, s)
	case List, Set:
		arr, ok := v.([]any)
		if !ok {
			return bad()
		}
		elems := make([][]byte, len(arr))
		for i, x := range arr {
			e, err := FromJSON(t.Elems[0], x)
			if err != nil {
				return nil, err
			}
			if e == nil {
				return nil, fmt.Errorf("Invalid null element in %s", t)
			}
			elems[i] = e
		}
		if t.Kind == Set {
			sortUnique(t.Elems[0], &elems)
		}
		return BuildCollection(len(elems), elems), nil
	case Map:
		obj, ok := v.(map[string]any)
		if !ok {
			return bad()
		}
		type kv struct{ k, v []byte }
		var entries []kv
		for ks, x := range obj {
			var key []byte
			var err error
			if t.Elems[0].Kind == Text || t.Elems[0].Kind == Ascii {
				key, err = FromString(t.Elems[0], ks)
			} else {
				parsed, perr := ParseJSON(ks)
				if perr != nil {
					parsed = ks
				}
				key, err = FromJSON(t.Elems[0], parsed)
			}
			if err != nil {
				return nil, err
			}
			val, err := FromJSON(t.Elems[1], x)
			if err != nil {
				return nil, err
			}
			if val == nil {
				return nil, fmt.Errorf("Invalid null value in map")
			}
			entries = append(entries, kv{key, val})
		}
		keys := make([][]byte, len(entries))
		for i, e := range entries {
			keys[i] = e.k
		}
		order := make([]int, len(entries))
		for i := range order {
			order[i] = i
		}
		sortIdx(order, func(a, b int) int { return Compare(t.Elems[0], entries[a].k, entries[b].k) })
		var out [][]byte
		for _, i := range order {
			out = append(out, entries[i].k, entries[i].v)
		}
		return BuildCollection(len(out)/2, out), nil
	case Vector:
		arr, ok := v.([]any)
		if !ok || len(arr) != t.Dim {
			return bad()
		}
		elems := make([][]byte, len(arr))
		for i, x := range arr {
			e, err := FromJSON(t.Elems[0], x)
			if err != nil {
				return nil, err
			}
			elems[i] = e
		}
		return BuildVector(t, elems), nil
	case Tuple:
		arr, ok := v.([]any)
		if !ok || len(arr) > len(t.Elems) {
			return bad()
		}
		fields := make([][]byte, len(arr))
		for i, x := range arr {
			f, err := FromJSON(t.Elems[i], x)
			if err != nil {
				return nil, err
			}
			fields[i] = f
		}
		return BuildTuple(fields), nil
	case UDT:
		obj, ok := v.(map[string]any)
		if !ok {
			return bad()
		}
		fields := make([][]byte, len(t.Elems))
		for k, x := range obj {
			name := unquoteJSONName(k)
			i := -1
			for j, f := range t.Fields {
				if f == name {
					i = j
				}
			}
			if i < 0 {
				return nil, fmt.Errorf("Unknown field '%s' in value of user defined type %s", k, t.Name)
			}
			f, err := FromJSON(t.Elems[i], x)
			if err != nil {
				return nil, err
			}
			fields[i] = f
		}
		return BuildTuple(fields), nil
	}
	return bad()
}

// unquoteJSONName reads a JSON key as a CQL name: "\"Name\"" is
// case-sensitive, anything else lowercased.
func unquoteJSONName(k string) string {
	if len(k) >= 2 && k[0] == '"' && k[len(k)-1] == '"' {
		return strings.ReplaceAll(k[1:len(k)-1], `""`, `"`)
	}
	return strings.ToLower(k)
}

// UnquoteJSONName reads a JSON key as a CQL column name.
func UnquoteJSONName(k string) string { return unquoteJSONName(k) }

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func sortUnique(t *Type, elems *[][]byte) {
	e := *elems
	idx := make([]int, len(e))
	for i := range idx {
		idx[i] = i
	}
	sortIdx(idx, func(a, b int) int { return Compare(t, e[a], e[b]) })
	var out [][]byte
	for _, i := range idx {
		if len(out) > 0 && Compare(t, out[len(out)-1], e[i]) == 0 {
			continue
		}
		out = append(out, e[i])
	}
	*elems = out
}

func sortIdx(idx []int, cmp func(a, b int) int) {
	// insertion sort keeps it dependency-free and stable; JSON values are small
	for i := 1; i < len(idx); i++ {
		for j := i; j > 0 && cmp(idx[j-1], idx[j]) > 0; j-- {
			idx[j-1], idx[j] = idx[j], idx[j-1]
		}
	}
}

var _ = bytes.Equal
var _ = big.NewInt

// JavaFloatString formats a float as Java's Float/Double.toString, as
// Cassandra does for JSON: "1.0", "0.001", "1.0E7", "1.234E-5".
func JavaFloatString(f float64, bits int) string {
	a := math.Abs(f)
	if a == 0 || (a >= 1e-3 && a < 1e7) {
		s := strconv.FormatFloat(f, 'f', -1, bits)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	s := strconv.FormatFloat(f, 'e', -1, bits)
	mant, exp, _ := strings.Cut(s, "e")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}
	neg := strings.HasPrefix(exp, "-")
	exp = strings.TrimLeft(exp, "+-0")
	if exp == "" {
		exp = "0"
	}
	if neg {
		exp = "-" + exp
	}
	return mant + "E" + exp
}
