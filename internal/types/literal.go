package types

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// LitKind is the kind of a CQL constant (Constants.Type).
type LitKind uint8

const (
	LitString LitKind = iota + 1
	LitInteger
	LitFloat
	LitBoolean
	LitUUID
	LitHex
	LitDuration
)

func (k LitKind) String() string {
	return [...]string{"", "STRING", "INTEGER", "FLOAT", "BOOLEAN", "UUID", "HEX", "DURATION"}[k]
}

// assignable is Constants.Literal.testAssignment: which native types a
// constant of each kind can be assigned to.
func assignable(k LitKind, t Kind) bool {
	switch k {
	case LitString:
		switch t {
		case Ascii, Text, Inet, Date, Time, Timestamp:
			return true
		}
	case LitInteger:
		switch t {
		case Bigint, Counter, Date, Decimal, Double, Duration, Float, Int, Smallint, Time, Timestamp, Tinyint, Varint:
			return true
		}
	case LitUUID:
		return t == UUID || t == Timeuuid
	case LitFloat:
		return t == Decimal || t == Double || t == Float
	case LitBoolean:
		return t == Boolean
	case LitHex:
		return t == Blob
	case LitDuration:
		return t == Duration
	}
	return false
}

// FromLiteral converts a CQL constant to a serialized value of t, as
// Constants.Literal.prepare: the constant must be assignable to the type,
// and is then parsed with the type's fromString.
func FromLiteral(t *Type, k LitKind, text string) ([]byte, error) {
	if !assignable(k, t.Kind) {
		return nil, fmt.Errorf("invalid %s constant (%s) for type %s", k, text, t)
	}
	if k == LitHex {
		b, err := hex.DecodeString(text[2:])
		if err != nil {
			return nil, fmt.Errorf("cannot parse '%s' as hex bytes", text)
		}
		return b, nil
	}
	return FromString(t, text)
}

// FromString parses the string form of a native value, as each type's
// AbstractType.fromString. An empty string gives the empty value for
// types other than text and ascii, as in Cassandra.
func FromString(t *Type, s string) ([]byte, error) {
	switch t.Kind {
	case Ascii:
		for i := 0; i < len(s); i++ {
			if s[i] >= 0x80 {
				return nil, fmt.Errorf("invalid ASCII character in string literal")
			}
		}
		return []byte(s), nil
	case Text:
		return []byte(s), nil
	}
	if s == "" {
		return []byte{}, nil
	}
	switch t.Kind {
	case Blob:
		b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
		if err != nil {
			return nil, fmt.Errorf("cannot parse '%s' as hex bytes", s)
		}
		return b, nil
	case Boolean:
		switch strings.ToLower(s) {
		case "true":
			return []byte{1}, nil
		case "false":
			return []byte{0}, nil
		}
		return nil, fmt.Errorf("unable to make boolean from '%s'", s)
	case Tinyint, Smallint, Int, Bigint, Counter:
		bits := map[Kind]int{Tinyint: 8, Smallint: 16, Int: 32, Bigint: 64, Counter: 64}[t.Kind]
		n, err := strconv.ParseInt(s, 10, bits)
		if err != nil {
			return nil, fmt.Errorf("unable to make %s from '%s'", t, s)
		}
		return appendInt(nil, n, bits/8), nil
	case Varint:
		n, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return nil, fmt.Errorf("unable to make int from '%s'", s)
		}
		return VarintBytes(n), nil
	case Decimal:
		return parseDecimal(s)
	case Float:
		f, err := parseFloat(s, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to make float from '%s'", s)
		}
		return binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(f))), nil
	case Double:
		f, err := parseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to make double from '%s'", s)
		}
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(f)), nil
	case UUID, Timeuuid:
		u, err := ParseUUID(s)
		if err != nil {
			return nil, err
		}
		if t.Kind == Timeuuid && u[6]>>4 != 1 {
			return nil, fmt.Errorf("unsupported UUID version for timeuuid: %s", s)
		}
		return u[:], nil
	case Inet:
		ip := net.ParseIP(s)
		if ip == nil {
			return nil, fmt.Errorf("unable to make inet address from '%s'", s)
		}
		if ip4 := ip.To4(); ip4 != nil && !strings.Contains(s, ":") {
			return []byte(ip4), nil
		}
		return []byte(ip.To16()), nil
	case Timestamp:
		ms, err := ParseTimestamp(s)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.AppendUint64(nil, uint64(ms)), nil
	case Date:
		d, err := parseDate(s)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.AppendUint32(nil, d), nil
	case Time:
		ns, err := parseTime(s)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.AppendUint64(nil, uint64(ns)), nil
	case Duration:
		return parseDuration(s)
	}
	return nil, fmt.Errorf("cannot parse a %s value from a string", t)
}

func appendInt(dst []byte, n int64, size int) []byte {
	for i := size - 1; i >= 0; i-- {
		dst = append(dst, byte(n>>(8*i)))
	}
	return dst
}

func parseFloat(s string, bits int) (float64, error) {
	switch s {
	case "NaN", "-NaN":
		return math.NaN(), nil
	case "Infinity":
		return math.Inf(1), nil
	case "-Infinity":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, bits)
}

// VarintBytes is the minimal two's complement big-endian encoding of n
// (BigInteger.toByteArray).
func VarintBytes(n *big.Int) []byte {
	switch n.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := n.Bytes()
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	// Negative: two's complement of |n| in len bytes, with a sign bit.
	abs := new(big.Int).Neg(n)
	l := len(abs.Bytes()) + 1
	mod := new(big.Int).Lsh(big.NewInt(1), uint(8*l))
	b := new(big.Int).Add(mod, n).Bytes()
	for len(b) < l {
		b = append([]byte{0xff}, b...)
	}
	// Strip redundant leading 0xff bytes.
	for len(b) > 1 && b[0] == 0xff && b[1]&0x80 != 0 {
		b = b[1:]
	}
	return b
}

// VarintValue decodes a two's complement big-endian integer.
func VarintValue(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return n
}

// parseDecimal parses a decimal as BigDecimal(String): an unscaled
// integer and a scale, serialized as int32 scale then the varint.
func parseDecimal(s string) ([]byte, error) {
	bad := fmt.Errorf("unable to make BigDecimal from '%s'", s)
	mant, exp := s, int64(0)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.ParseInt(s[i+1:], 10, 32)
		if err != nil {
			return nil, bad
		}
		mant, exp = s[:i], e
	}
	scale := int64(0)
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		scale = int64(len(mant) - i - 1)
		mant = mant[:i] + mant[i+1:]
	}
	if mant == "" || mant == "-" || mant == "+" {
		return nil, bad
	}
	n, ok := new(big.Int).SetString(mant, 10)
	if !ok {
		return nil, bad
	}
	scale -= exp
	if scale > math.MaxInt32 || scale < math.MinInt32 {
		return nil, bad
	}
	return append(binary.BigEndian.AppendUint32(nil, uint32(int32(scale))), VarintBytes(n)...), nil
}

// ParseUUID parses the canonical 8-4-4-4-12 form.
func ParseUUID(s string) ([16]byte, error) {
	var u [16]byte
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("unable to make UUID from '%s'", s)
	}
	h := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	if _, err := hex.Decode(u[:], []byte(h)); err != nil {
		return u, fmt.Errorf("unable to make UUID from '%s'", s)
	}
	return u, nil
}

// FormatUUID formats a UUID in the canonical form.
func FormatUUID(u []byte) string {
	h := hex.EncodeToString(u)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

var digits = regexp.MustCompile(`^-?\d+$`)

// timestampLayouts are TimestampSerializer's formats: date and time with
// 'T' or a space, optional seconds and fraction, and an optional zone; or a
// date alone.
var timestampLayouts = func() []string {
	var out []string
	for _, sep := range []string{"T", " "} {
		for _, secs := range []string{":05", ""} {
			for _, zone := range []string{"", "Z07:00", "Z0700", "Z07", " Z07:00", " Z0700", " MST"} {
				out = append(out, "2006-1-2"+sep+"15:4"+secs+zone)
			}
		}
	}
	for _, zone := range []string{"", "Z07:00", "Z0700", "Z07", " Z07:00", " Z0700"} {
		out = append(out, "2006-01-02"+zone)
	}
	return out
}()

// ParseTimestamp parses a timestamp string to milliseconds since the epoch
// (TimestampSerializer.dateStringToTimestamp): "now", an integer, or a date
// with optional time and zone (local time if no zone).
func ParseTimestamp(s string) (int64, error) {
	if strings.EqualFold(s, "now") {
		return time.Now().UnixMilli(), nil
	}
	if digits.MatchString(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unable to make long (for date) from: '%s'", s)
		}
		return n, nil
	}
	// time.Parse wants the fraction right after the seconds; move it there.
	frac, rest := extractFraction(s)
	for _, l := range timestampLayouts {
		t, err := time.ParseInLocation(l, rest, time.Local)
		if err == nil {
			return t.UnixMilli() + frac, nil
		}
	}
	return 0, fmt.Errorf("unable to parse a date/time from '%s'", s)
}

// extractFraction removes a ".fff" fraction of seconds from s, returning
// it in milliseconds.
func extractFraction(s string) (int64, string) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return 0, s
	}
	j := strings.IndexByte(s[i:], '.')
	if j < 0 {
		return 0, s
	}
	j += i
	k := j + 1
	for k < len(s) && s[k] >= '0' && s[k] <= '9' {
		k++
	}
	f := s[j+1 : k]
	if len(f) == 0 {
		return 0, s
	}
	for len(f) < 3 {
		f += "0"
	}
	ms, _ := strconv.ParseInt(f[:3], 10, 64)
	return ms, s[:j] + s[k:]
}

const dateEpoch = 1 << 31

// parseDate is SimpleDateSerializer.dateStringToDays: an unsigned day count
// with the epoch at 2^31, or yyyy-mm-dd.
func parseDate(s string) (uint32, error) {
	if digits.MatchString(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 || n > math.MaxUint32 {
			return 0, fmt.Errorf("unable to coerce '%s' to a formatted date (long)", s)
		}
		return uint32(n), nil
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		t, err = time.Parse("2006-1-2", s)
	}
	if err != nil {
		return 0, fmt.Errorf("unable to coerce '%s' to a formatted date (long)", s)
	}
	days := t.Unix() / 86400
	if t.Unix() < 0 && t.Unix()%86400 != 0 {
		days--
	}
	return uint32(days + dateEpoch), nil
}

// parseTime is TimeSerializer.timeStringToLong: nanoseconds since midnight,
// or hh:mm:ss[.fffffffff].
func parseTime(s string) (int64, error) {
	bad := fmt.Errorf("unable to coerce '%s' to a formatted time (long)", s)
	if digits.MatchString(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 || n >= 86400e9 {
			return 0, bad
		}
		return n, nil
	}
	s = strings.TrimSpace(s)
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, bad
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	sec, frac, _ := strings.Cut(parts[2], ".")
	sc, err3 := strconv.Atoi(sec)
	if err1 != nil || err2 != nil || err3 != nil || h < 0 || h > 23 || m < 0 || m > 59 || sc < 0 || sc > 59 || len(frac) > 9 {
		return 0, bad
	}
	ns := int64(0)
	if frac != "" {
		f := frac + strings.Repeat("0", 9-len(frac))
		v, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			return 0, bad
		}
		ns = v
	}
	return ((int64(h)*60+int64(m))*60+int64(sc))*1e9 + ns, nil
}

// Duration values: months, days and nanoseconds, each a signed vint
// (DurationSerializer).

var durationUnits = regexp.MustCompile(`(?i)(\d+)(y|mo|w|d|h|s|ms|us|µs|ns|m)`)

// parseDuration is Duration.from: "1h30m" style, ISO 8601 "P1DT2H" or
// "P1W".
func parseDuration(s string) ([]byte, error) {
	bad := fmt.Errorf("unable to convert '%s' to a duration", s)
	neg := strings.HasPrefix(s, "-")
	in := strings.TrimPrefix(s, "-")
	var months, days, nanos int64
	if strings.HasPrefix(in, "P") {
		w, isWeek := strings.CutSuffix(in[1:], "W")
		if isWeek {
			n, err := strconv.ParseInt(w, 10, 64)
			if err != nil {
				return nil, bad
			}
			days = 7 * n
		} else {
			datePart, timePart, _ := strings.Cut(in[1:], "T")
			re := regexp.MustCompile(`^(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?$`)
			m := re.FindStringSubmatch(datePart)
			if m == nil {
				return nil, bad
			}
			y, mo, d := atoi(m[1]), atoi(m[2]), atoi(m[3])
			months, days = 12*y+mo, d
			if timePart != "" {
				re := regexp.MustCompile(`^(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$`)
				m := re.FindStringSubmatch(timePart)
				if m == nil {
					return nil, bad
				}
				nanos = (atoi(m[1])*3600 + atoi(m[2])*60 + atoi(m[3])) * 1e9
			}
		}
	} else {
		rest := in
		for rest != "" {
			loc := durationUnits.FindStringSubmatchIndex(rest)
			if loc == nil || loc[0] != 0 {
				return nil, bad
			}
			n, _ := strconv.ParseInt(rest[loc[2]:loc[3]], 10, 64)
			switch strings.ToLower(rest[loc[4]:loc[5]]) {
			case "y":
				months += 12 * n
			case "mo":
				months += n
			case "w":
				days += 7 * n
			case "d":
				days += n
			case "h":
				nanos += n * 3600e9
			case "m":
				nanos += n * 60e9
			case "s":
				nanos += n * 1e9
			case "ms":
				nanos += n * 1e6
			case "us", "µs":
				nanos += n * 1e3
			case "ns":
				nanos += n
			}
			rest = rest[loc[1]:]
		}
		if in == "" {
			return nil, bad
		}
	}
	if neg {
		months, days, nanos = -months, -days, -nanos
	}
	return EncodeDuration(months, days, nanos), nil
}

func atoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// EncodeDuration serializes a duration.
func EncodeDuration(months, days, nanos int64) []byte {
	b := appendVInt(nil, months)
	b = appendVInt(b, days)
	return appendVInt(b, nanos)
}

func decodeDuration(b []byte) (months, days, nanos int64, err error) {
	var vals [3]int64
	for i := range vals {
		v, n, e := readVInt(b)
		if e != nil {
			return 0, 0, 0, errors.New("invalid duration value")
		}
		vals[i], b = v, b[n:]
	}
	if len(b) != 0 {
		return 0, 0, 0, errors.New("invalid duration value")
	}
	return vals[0], vals[1], vals[2], nil
}

// appendVInt writes a signed vint (VIntCoding.writeVInt): zigzag, then
// the unsigned vint whose first byte's leading ones count the extra bytes.
func appendVInt(dst []byte, v int64) []byte {
	u := uint64(v<<1) ^ uint64(v>>63)
	size := 1
	for size < 9 && u >= 1<<(7*size) {
		size++
	}
	if size == 1 {
		return append(dst, byte(u))
	}
	var buf [9]byte
	for i := size - 1; i >= 0; i-- {
		buf[i] = byte(u)
		u >>= 8
	}
	buf[0] |= ^byte(0xff >> (size - 1))
	if size == 9 {
		buf[0] = 0xff
	}
	return append(dst, buf[:size]...)
}

func readVInt(b []byte) (int64, int, error) {
	if len(b) == 0 {
		return 0, 0, errors.New("short vint")
	}
	first := b[0]
	extra := 0
	for extra < 8 && first&(0x80>>extra) != 0 {
		extra++
	}
	if len(b) < 1+extra {
		return 0, 0, errors.New("short vint")
	}
	u := uint64(first & (0xff >> extra))
	if extra == 8 {
		u = 0
	}
	for i := 1; i <= extra; i++ {
		u = u<<8 | uint64(b[i])
	}
	return int64(u>>1) ^ -int64(u&1), 1 + extra, nil
}
