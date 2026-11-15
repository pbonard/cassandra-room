package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
)

// Byte-comparable encoding (utils/bytecomparable/ByteComparable.md): values
// are translated to byte strings whose unsigned lexicographic order is the
// type's order, and which are prefix-free, so that sequences of values
// (partition and clustering keys) can be concatenated with separator bytes.
//
// Sequences: each component is preceded by Next (0x40), or by NullSep
// (0x3E) for a null or EmptySep (0x3F; 0x41 when reversed) for an empty
// value, and the sequence ends with Terminator (0x38). Bounds end with
// LTNext (0x20) or GTNext (0x60) instead, which sort before or after every
// sequence they prefix.
//
// The encodings of fixed-size numbers, floats, UUIDs and strings follow
// Cassandra's OSS50 version (bigint, timestamp and time use the fixed-length
// signed form). varint and decimal use simpler order-preserving forms of our
// own: these bytes are only ever read by this engine.
const (
	LTNext          = 0x20
	Terminator      = 0x38
	NullSep         = 0x3E
	EmptySep        = 0x3F
	Next            = 0x40
	EmptySepReverse = 0x41
	GTNext          = 0x60
)

var errBadComparable = errors.New("types: corrupt byte-comparable value")

// AppendComponent appends one component of a sequence: its separator and,
// unless it is null or empty, its byte-comparable value, inverted if
// reversed. v nil means null.
func AppendComponent(dst []byte, t *Type, v []byte, reversed bool) ([]byte, error) {
	switch {
	case v == nil:
		return append(dst, NullSep), nil
	case len(v) == 0:
		if reversed {
			return append(dst, EmptySepReverse), nil
		}
		return append(dst, EmptySep), nil
	}
	dst = append(dst, Next)
	start := len(dst)
	dst, err := AppendComparable(dst, t, v)
	if err != nil {
		return nil, err
	}
	if reversed {
		for i := start; i < len(dst); i++ {
			dst[i] = ^dst[i]
		}
	}
	return dst, nil
}

// ReadComponent reads a component written by AppendComponent, returning
// its value (nil for null) and the rest of b.
func ReadComponent(b []byte, t *Type, reversed bool) ([]byte, []byte, error) {
	if len(b) == 0 {
		return nil, nil, errBadComparable
	}
	switch b[0] {
	case NullSep:
		return nil, b[1:], nil
	case EmptySep, EmptySepReverse:
		return []byte{}, b[1:], nil
	case Next:
	default:
		return nil, nil, errBadComparable
	}
	r := reader{b: b[1:]}
	if reversed {
		r.flip = 0xff
	}
	v, err := r.value(t)
	if err != nil {
		return nil, nil, err
	}
	return v, r.b[r.i:], nil
}

// AppendComparable appends the byte-comparable form of a non-empty value.
func AppendComparable(dst []byte, t *Type, v []byte) ([]byte, error) {
	switch t.Kind {
	case Boolean, Date:
		return append(dst, v...), nil
	case Tinyint, Smallint, Int, Bigint, Counter, Timestamp, Time:
		dst = append(dst, v...)
		dst[len(dst)-len(v)] ^= 0x80
		return dst, nil
	case Float:
		u := binary.BigEndian.Uint32(v)
		if u&(1<<31) != 0 {
			u = ^u
		} else {
			u |= 1 << 31
		}
		return binary.BigEndian.AppendUint32(dst, u), nil
	case Double:
		u := binary.BigEndian.Uint64(v)
		if u&(1<<63) != 0 {
			u = ^u
		} else {
			u |= 1 << 63
		}
		return binary.BigEndian.AppendUint64(dst, u), nil
	case UUID:
		msb := binary.BigEndian.Uint64(v)
		version := (msb >> 12) & 0xf
		if version == 1 {
			msb = reorderTimestamp(msb)
		} else {
			msb = version<<60 | (msb>>4)&0x0FFFFFFFFFFFF000 | msb&0xFFF
		}
		dst = binary.BigEndian.AppendUint64(dst, msb)
		return append(dst, v[8:]...), nil
	case Timeuuid:
		dst = binary.BigEndian.AppendUint64(dst, reorderTimestamp(binary.BigEndian.Uint64(v)))
		return binary.BigEndian.AppendUint64(dst, binary.BigEndian.Uint64(v[8:])^0x8080808080808080), nil
	case Ascii, Text, Blob, Inet:
		return appendEscaped(dst, v), nil
	case Varint:
		return appendVarintComparable(dst, v), nil
	case Decimal:
		return appendDecimalComparable(dst, v)
	case List, Set:
		elems, err := SplitCollection(v, 1)
		if err != nil {
			return nil, err
		}
		for _, e := range elems {
			if dst, err = AppendComponent(dst, t.Elems[0], e, false); err != nil {
				return nil, err
			}
		}
		return append(dst, Terminator), nil
	case Map:
		elems, err := SplitCollection(v, 2)
		if err != nil {
			return nil, err
		}
		for i, e := range elems {
			if dst, err = AppendComponent(dst, t.Elems[i%2], e, false); err != nil {
				return nil, err
			}
		}
		return append(dst, Terminator), nil
	case Tuple, UDT:
		fields, err := SplitTuple(v)
		if err != nil {
			return nil, err
		}
		for i, f := range fields {
			if dst, err = AppendComponent(dst, t.Elems[i], f, false); err != nil {
				return nil, err
			}
		}
		return append(dst, Terminator), nil
	}
	return nil, fmt.Errorf("%s values cannot be used in keys", t)
}

func reorderTimestamp(msb uint64) uint64 {
	return msb<<48 | (msb<<16)&0xFFFF00000000 | msb>>32
}

func reorderBackTimestamp(msb uint64) uint64 {
	return msb<<32 | (msb>>16)&0xFFFF0000 | msb>>48
}

// appendEscaped is ByteSource's variable-length escaping: a run of n zero
// bytes becomes 00 FE^(n-1) FF (FE instead of the last FF at the end of
// the value), and a value not ending in zero is terminated by 00.
func appendEscaped(dst, v []byte) []byte {
	escaped := false
	for _, c := range v {
		if c == 0 {
			if escaped {
				dst = append(dst, 0xFE)
			} else {
				dst = append(dst, 0x00)
				escaped = true
			}
			continue
		}
		if escaped {
			dst = append(dst, 0xFF)
			escaped = false
		}
		dst = append(dst, c)
	}
	if escaped {
		return append(dst, 0xFE)
	}
	return append(dst, 0x00)
}

// Varint: a sign byte (0x01 negative, 0x02 otherwise), the length of the
// two's complement bytes as 4 bytes (inverted for negatives, so that longer
// negatives sort lower), then the bytes.
func appendVarintComparable(dst, v []byte) []byte {
	n := VarintValue(v)
	b := VarintBytes(n) // minimal form, so equal values encode equally
	if n.Sign() < 0 {
		dst = append(dst, 0x01)
		dst = binary.BigEndian.AppendUint32(dst, ^uint32(len(b)))
	} else {
		dst = append(dst, 0x02)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(b)))
	}
	return append(dst, b...)
}

// Decimal: 0x02 for zero; otherwise, with |v| = 0.d1d2...dn × 10^e and
// d1 != 0, 0x03 then e as 8 sign-flipped bytes, the digits as bytes 1-10,
// and 0x00; negatives are 0x01 then the inverted bytes of their absolute
// value's encoding (after the sign byte).
func appendDecimalComparable(dst, v []byte) ([]byte, error) {
	if len(v) < 4 {
		return nil, errBadComparable
	}
	scale := int64(int32(binary.BigEndian.Uint32(v)))
	unscaled := VarintValue(v[4:])
	if unscaled.Sign() == 0 {
		return append(dst, 0x02), nil
	}
	neg := unscaled.Sign() < 0
	digits := []byte(new(big.Int).Abs(unscaled).String())
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
		scale--
	}
	e := int64(len(digits)) - scale
	if neg {
		dst = append(dst, 0x01)
	} else {
		dst = append(dst, 0x03)
	}
	start := len(dst)
	dst = binary.BigEndian.AppendUint64(dst, uint64(e)^(1<<63))
	for _, d := range digits {
		dst = append(dst, d-'0'+1)
	}
	dst = append(dst, 0x00)
	if neg {
		for i := start; i < len(dst); i++ {
			dst[i] = ^dst[i]
		}
	}
	return dst, nil
}

// reader decodes byte-comparable values, inverting bytes if flip is 0xff.
type reader struct {
	b    []byte
	i    int
	flip byte
}

func (r *reader) byte() (byte, error) {
	if r.i >= len(r.b) {
		return 0, errBadComparable
	}
	c := r.b[r.i] ^ r.flip
	r.i++
	return c, nil
}

func (r *reader) fixed(n int) ([]byte, error) {
	if r.i+n > len(r.b) {
		return nil, errBadComparable
	}
	out := make([]byte, n)
	for j := range out {
		out[j] = r.b[r.i+j] ^ r.flip
	}
	r.i += n
	return out, nil
}

func (r *reader) peek() int {
	if r.i >= len(r.b) {
		return -1
	}
	return int(r.b[r.i] ^ r.flip)
}

// component reads a nested component (inside a collection or tuple).
func (r *reader) component(t *Type) ([]byte, error) {
	sep, err := r.byte()
	if err != nil {
		return nil, err
	}
	switch sep {
	case NullSep:
		return nil, nil
	case EmptySep:
		return []byte{}, nil
	case Next:
		return r.value(t)
	}
	return nil, errBadComparable
}

func (r *reader) value(t *Type) ([]byte, error) {
	switch t.Kind {
	case Boolean, Tinyint:
		return r.signed(t, 1)
	case Smallint:
		return r.signed(t, 2)
	case Int, Date:
		return r.signed(t, 4)
	case Bigint, Counter, Timestamp, Time:
		return r.signed(t, 8)
	case Float:
		b, err := r.fixed(4)
		if err != nil {
			return nil, err
		}
		u := binary.BigEndian.Uint32(b)
		if u&(1<<31) != 0 {
			u &^= 1 << 31
		} else {
			u = ^u
		}
		return binary.BigEndian.AppendUint32(b[:0], u), nil
	case Double:
		b, err := r.fixed(8)
		if err != nil {
			return nil, err
		}
		u := binary.BigEndian.Uint64(b)
		if u&(1<<63) != 0 {
			u &^= 1 << 63
		} else {
			u = ^u
		}
		return binary.BigEndian.AppendUint64(b[:0], u), nil
	case UUID:
		b, err := r.fixed(16)
		if err != nil {
			return nil, err
		}
		hi := binary.BigEndian.Uint64(b)
		if hi>>60 == 1 {
			hi = reorderBackTimestamp(hi)
		} else {
			hi = (hi<<4)&0xFFFFFFFFFFFF0000 | (hi>>60)<<12 | hi&0xFFF
		}
		binary.BigEndian.PutUint64(b, hi)
		return b, nil
	case Timeuuid:
		b, err := r.fixed(16)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint64(b, reorderBackTimestamp(binary.BigEndian.Uint64(b)))
		binary.BigEndian.PutUint64(b[8:], binary.BigEndian.Uint64(b[8:])^0x8080808080808080)
		return b, nil
	case Ascii, Text, Blob, Inet:
		return r.escaped()
	case Varint:
		sign, err := r.byte()
		if err != nil {
			return nil, err
		}
		lb, err := r.fixed(4)
		if err != nil {
			return nil, err
		}
		l := binary.BigEndian.Uint32(lb)
		if sign == 0x01 {
			l = ^l
		}
		if l > math.MaxInt32 {
			return nil, errBadComparable
		}
		return r.fixed(int(l))
	case Decimal:
		return r.decimal()
	case List, Set, Map, Tuple, UDT:
		var elems [][]byte
		for r.peek() != Terminator {
			et := t.Elems[0]
			switch t.Kind {
			case Map:
				et = t.Elems[len(elems)%2]
			case Tuple, UDT:
				if len(elems) >= len(t.Elems) {
					return nil, errBadComparable
				}
				et = t.Elems[len(elems)]
			}
			e, err := r.component(et)
			if err != nil {
				return nil, err
			}
			elems = append(elems, e)
		}
		r.i++
		switch t.Kind {
		case Tuple, UDT:
			return BuildTuple(elems), nil
		case Map:
			return BuildCollection(len(elems)/2, elems), nil
		}
		return BuildCollection(len(elems), elems), nil
	}
	return nil, fmt.Errorf("%s values cannot be used in keys", t)
}

func (r *reader) signed(t *Type, n int) ([]byte, error) {
	b, err := r.fixed(n)
	if err != nil {
		return nil, err
	}
	switch t.Kind {
	case Boolean, Date:
	default:
		b[0] ^= 0x80
	}
	return b, nil
}

func (r *reader) escaped() ([]byte, error) {
	var out []byte
	for {
		c, err := r.byte()
		if err != nil {
			return nil, err
		}
		if c != 0 {
			out = append(out, c)
			continue
		}
		// An escape: FE or FF continue the value with a zero; anything else
		// (a separator, or the end) ends it.
		for {
			switch r.peek() {
			case 0xFE:
				r.i++
				out = append(out, 0)
				continue
			case 0xFF:
				r.i++
				out = append(out, 0)
			default:
				return out, nil
			}
			break
		}
	}
}

func (r *reader) decimal() ([]byte, error) {
	sign, err := r.byte()
	if err != nil {
		return nil, err
	}
	if sign == 0x02 {
		return []byte{0, 0, 0, 0, 0}, nil
	}
	if sign != 0x01 && sign != 0x03 {
		return nil, errBadComparable
	}
	neg := sign == 0x01
	if neg {
		r.flip ^= 0xff
		defer func() { r.flip ^= 0xff }()
	}
	eb, err := r.fixed(8)
	if err != nil {
		return nil, err
	}
	e := int64(binary.BigEndian.Uint64(eb) ^ (1 << 63))
	var digits []byte
	for {
		c, err := r.byte()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			break
		}
		if c > 10 {
			return nil, errBadComparable
		}
		digits = append(digits, c-1+'0')
	}
	n, ok := new(big.Int).SetString(string(digits), 10)
	if !ok {
		return nil, errBadComparable
	}
	if neg {
		n.Neg(n)
	}
	scale := int64(len(digits)) - e
	if scale > math.MaxInt32 || scale < math.MinInt32 {
		return nil, errBadComparable
	}
	return append(binary.BigEndian.AppendUint32(nil, uint32(int32(scale))), VarintBytes(n)...), nil
}

// Compare compares two serialized values of t in the type's order, via
// their byte-comparable forms. Empty values sort first.
func Compare(t *Type, a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		return min(len(a), 1) - min(len(b), 1)
	}
	ea, err1 := AppendComparable(nil, t, a)
	eb, err2 := AppendComparable(nil, t, b)
	if err1 != nil || err2 != nil {
		return compareBytes(a, b)
	}
	return compareBytes(ea, eb)
}

func compareBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}
