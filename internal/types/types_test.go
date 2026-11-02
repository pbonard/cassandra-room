package types

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseType(t *testing.T) {
	for in, want := range map[string]string{
		"int":                            "int",
		"VARCHAR":                        "text",
		"frozen<map<text, int>>":         "frozen<map<text, int>>",
		"map<text,frozen<list<int>>>":    "map<text, frozen<list<int>>>",
		"map<text, list<int>>":           "map<text, frozen<list<int>>>",
		"tuple<int, text>":               "frozen<tuple<int, text>>",
		"frozen<tuple<int, list<text>>>": "frozen<tuple<int, frozen<list<text>>>>",
		"frozen<map<text, list<int>>>":   "frozen<map<text, frozen<list<int>>>>",
		"set<varchar>":                   "set<text>",
	} {
		typ, err := Parse(in)
		require.NoError(t, err, in)
		require.Equal(t, want, typ.String(), in)
		again, err := Parse(typ.String())
		require.NoError(t, err)
		require.True(t, typ.Equal(again))
	}
	for _, bad := range []string{"", "foo", "map<int>", "list<int", "list<counter>"} {
		_, err := Parse(bad)
		require.Error(t, err, bad)
	}
}

func TestFromLiteral(t *testing.T) {
	v, err := FromLiteral(IntType, LitInteger, "-2")
	require.NoError(t, err)
	require.Equal(t, []byte{0xff, 0xff, 0xff, 0xfe}, v)
	_, err = FromLiteral(IntType, LitString, "2")
	require.Error(t, err, "a string is not assignable to int")
	_, err = FromLiteral(IntType, LitInteger, "3000000000")
	require.Error(t, err)
	v, err = FromLiteral(TimestampType, LitString, "2011-02-03 04:05:00+0000")
	require.NoError(t, err)
	require.EqualValues(t, 1296705900000, int64(binary.BigEndian.Uint64(v)))
	v, err = FromLiteral(TimestampType, LitString, "2011-02-03T04:05:06.789Z")
	require.NoError(t, err)
	require.EqualValues(t, 1296705906789, int64(binary.BigEndian.Uint64(v)))
	v, err = FromLiteral(DateType, LitString, "1970-01-02")
	require.NoError(t, err)
	require.EqualValues(t, 1<<31+1, binary.BigEndian.Uint32(v))
	v, err = FromLiteral(TimeType, LitString, "01:02:03.5")
	require.NoError(t, err)
	require.EqualValues(t, 3723500000000, binary.BigEndian.Uint64(v))
	v, err = FromLiteral(BlobType, LitHex, "0xCAFE")
	require.NoError(t, err)
	require.Equal(t, []byte{0xca, 0xfe}, v)
	v, err = FromLiteral(DecimalType, LitFloat, "-1.50")
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 2, 0xff, 0x6a}, v) // scale 2, -150
	v, err = FromLiteral(VarintType, LitInteger, "-129")
	require.NoError(t, err)
	require.Equal(t, []byte{0xff, 0x7f}, v)
	v, err = FromLiteral(DurationType, LitDuration, "1h30m")
	require.NoError(t, err)
	m, d, n, err := decodeDuration(v)
	require.NoError(t, err)
	require.Equal(t, [3]int64{0, 0, 90 * 60e9}, [3]int64{m, d, n})
	v, err = FromLiteral(DurationType, LitDuration, "P1Y2DT3H")
	require.NoError(t, err)
	m, d, n, _ = decodeDuration(v)
	require.Equal(t, [3]int64{12, 2, 3 * 3600e9}, [3]int64{m, d, n})
	v, err = FromLiteral(InetType, LitString, "127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, []byte{127, 0, 0, 1}, v)
	_, err = FromLiteral(TimeuuidType, LitUUID, "cc520882-9507-44fb-8fc9-b349ecdee658")
	require.Error(t, err, "a v4 UUID is not a timeuuid")
}

func TestVInt(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 63, -64, 64, 1 << 20, -(1 << 40), math.MaxInt64, math.MinInt64} {
		b := appendVInt(nil, v)
		got, n, err := readVInt(b)
		require.NoError(t, err)
		require.Equal(t, len(b), n)
		require.Equal(t, v, got)
	}
}

// natural compares two values of t independently of the byte-comparable
// encoding, as Cassandra's comparators do.
func natural(t *Type, a, b []byte) int {
	switch t.Kind {
	case Boolean, Date:
		return bytes.Compare(a, b)
	case Tinyint:
		return cmp(int64(int8(a[0])), int64(int8(b[0])))
	case Smallint:
		return cmp(int64(int16(binary.BigEndian.Uint16(a))), int64(int16(binary.BigEndian.Uint16(b))))
	case Int:
		return cmp(int64(int32(binary.BigEndian.Uint32(a))), int64(int32(binary.BigEndian.Uint32(b))))
	case Bigint, Timestamp, Time:
		return cmp(int64(binary.BigEndian.Uint64(a)), int64(binary.BigEndian.Uint64(b)))
	case Double:
		return compareDouble(math.Float64frombits(binary.BigEndian.Uint64(a)), math.Float64frombits(binary.BigEndian.Uint64(b)))
	case Text, Blob:
		return bytes.Compare(a, b)
	case Varint:
		return VarintValue(a).Cmp(VarintValue(b))
	case Decimal:
		return decimalRat(a).Cmp(decimalRat(b))
	case Timeuuid:
		ta, tb := reorderTimestamp(binary.BigEndian.Uint64(a)), reorderTimestamp(binary.BigEndian.Uint64(b))
		if ta != tb {
			return cmp(int64(ta^1<<63), int64(tb^1<<63))
		}
		// lsb compared as signed bytes
		for i := 8; i < 16; i++ {
			if a[i] != b[i] {
				return cmp(int64(int8(a[i])), int64(int8(b[i])))
			}
		}
		return 0
	case Tuple:
		fa, _ := SplitTuple(a)
		fb, _ := SplitTuple(b)
		for i := 0; i < len(fa) && i < len(fb); i++ {
			switch {
			case fa[i] == nil && fb[i] == nil:
				continue
			case fa[i] == nil:
				return -1 // nulls sort first
			case fb[i] == nil:
				return 1
			}
			if c := natural(t.Elems[i], fa[i], fb[i]); c != 0 {
				return c
			}
		}
		return len(fa) - len(fb)
	case List:
		ea, _ := SplitCollection(a, 1)
		eb, _ := SplitCollection(b, 1)
		for i := 0; i < len(ea) && i < len(eb); i++ {
			if c := natural(t.Elems[0], ea[i], eb[i]); c != 0 {
				return c
			}
		}
		return cmp(int64(len(ea)), int64(len(eb)))
	}
	panic(t.String())
}

func cmp(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func compareDouble(a, b float64) int {
	// Double.compare: -0.0 < 0.0, NaN greatest.
	ab, bb := int64(math.Float64bits(a)), int64(math.Float64bits(b))
	if ab < 0 {
		ab ^= math.MaxInt64
	}
	if bb < 0 {
		bb ^= math.MaxInt64
	}
	return cmp(ab, bb)
}

func decimalRat(v []byte) *big.Rat {
	scale := int32(binary.BigEndian.Uint32(v))
	r := new(big.Rat).SetInt(VarintValue(v[4:]))
	p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(scale))), nil)
	if scale > 0 {
		return r.Quo(r, new(big.Rat).SetInt(p))
	}
	return r.Mul(r, new(big.Rat).SetInt(p))
}

func abs(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

func randValue(rng *rand.Rand, t *Type) []byte {
	switch t.Kind {
	case Boolean:
		return []byte{byte(rng.Intn(2))}
	case Tinyint, Smallint, Int, Bigint, Timestamp, Date:
		b := make([]byte, t.fixedSize())
		rng.Read(b)
		if rng.Intn(3) == 0 {
			for i := 1; i < len(b); i++ {
				b[i] = b[0] // small and extreme values
			}
		}
		return b
	case Time:
		return binary.BigEndian.AppendUint64(nil, uint64(rng.Int63n(86400e9)))
	case Double:
		specials := []float64{0, math.Copysign(0, -1), 1, -1, math.Inf(1), math.Inf(-1), math.NaN(), math.SmallestNonzeroFloat64}
		f := rng.NormFloat64() * math.Pow(10, float64(rng.Intn(20)-10))
		if rng.Intn(4) == 0 {
			f = specials[rng.Intn(len(specials))]
		}
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(f))
	case Text, Blob:
		b := make([]byte, rng.Intn(6)+1)
		for i := range b {
			b[i] = []byte{0, 0, 1, 'a', 0xff, 0xfe}[rng.Intn(6)]
		}
		return b
	case Varint:
		n := new(big.Int).Rand(rng, new(big.Int).Lsh(big.NewInt(1), uint(rng.Intn(80)+1)))
		if rng.Intn(2) == 0 {
			n.Neg(n)
		}
		return VarintBytes(n)
	case Decimal:
		n := new(big.Int).Rand(rng, big.NewInt(int64(rng.Intn(1e6)+1)))
		if rng.Intn(2) == 0 {
			n.Neg(n)
		}
		return append(binary.BigEndian.AppendUint32(nil, uint32(int32(rng.Intn(10)-5))), VarintBytes(n)...)
	case Timeuuid:
		b := make([]byte, 16)
		rng.Read(b)
		b[6] = b[6]&0x0f | 0x10
		return b
	case Tuple:
		var f [][]byte
		for _, e := range t.Elems {
			if rng.Intn(5) == 0 {
				f = append(f, nil)
			} else {
				f = append(f, randValue(rng, e))
			}
		}
		return BuildTuple(f)
	case List:
		var e [][]byte
		for i := rng.Intn(3); i > 0; i-- {
			e = append(e, randValue(rng, t.Elems[0]))
		}
		return BuildCollection(len(e), e)
	}
	panic(t.String())
}

// canonical returns the value decoding should give back: varints and
// decimals in minimal form.
func canonical(t *Type, v []byte) []byte {
	switch t.Kind {
	case Varint:
		return VarintBytes(VarintValue(v))
	case Decimal:
		b, _ := appendDecimalComparable(nil, v)
		r := reader{b: b}
		out, _ := r.decimal()
		return out
	}
	return v
}

// TestComparable checks that the byte-comparable encoding orders values as
// the types' comparators do, round-trips, and stays correct reversed and
// inside sequences.
func TestComparable(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	typs := []*Type{BooleanType, TinyintType, SmallintType, IntType, BigintType, TimestampType, DateType,
		TimeType, DoubleType, TextType, BlobType, VarintType, DecimalType, TimeuuidType,
		{Kind: Tuple, Elems: []*Type{IntType, TextType}, Frozen: true}, ListOf(BlobType, true)}
	for _, typ := range typs {
		for i := 0; i < 3000; i++ {
			a, b := randValue(rng, typ), randValue(rng, typ)
			want := natural(typ, a, b)
			for _, rev := range []bool{false, true} {
				ea, err := AppendComponent([]byte{9}, typ, a, rev)
				require.NoError(t, err)
				eb, err := AppendComponent([]byte{9}, typ, b, rev)
				require.NoError(t, err)
				// A trailing separator byte must not change the order.
				ea, eb = append(ea, Terminator), append(eb, Next)
				got := bytes.Compare(ea, eb)
				if want == 0 {
					require.True(t, got < 0, "%s %x %x", typ, a, b)
				} else {
					w := want
					if rev {
						w = -w
					}
					require.Equal(t, sign(w), sign(got), "%s rev=%v %x %x: %x %x", typ, rev, a, b, ea, eb)
				}
				dec, rest, err := ReadComponent(ea[1:], typ, rev)
				require.NoError(t, err, "%s %x", typ, ea)
				require.Equal(t, canonical(typ, a), dec, "%s rev=%v", typ, rev)
				require.Equal(t, []byte{Terminator}, rest)
			}
		}
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	}
	return 0
}

func TestEscapedExamples(t *testing.T) {
	// From ByteComparable.md.
	require.Equal(t, []byte{0x22, 0x00, 0xFE}, appendEscaped(nil, []byte{0x22, 0x00}))
	require.Equal(t, []byte{0x22, 0x00, 0xFE, 0xFF, 0x33, 0x00}, appendEscaped(nil, []byte{0x22, 0, 0, 0x33}))
	require.Equal(t, []byte{0x22, 0x00, 0xFF, 0x11, 0x00}, appendEscaped(nil, []byte{0x22, 0, 0x11}))
	u, _ := ParseUUID("2a92d750-d8dc-11e6-a2de-cf8ecd4cf053")
	b, _ := AppendComparable(nil, UUIDType, u[:])
	require.Equal(t, "11e6d8dc2a92d750a2decf8ecd4cf053", hexs(b))
	u, _ = ParseUUID("cc520882-9507-44fb-8fc9-b349ecdee658")
	b, _ = AppendComparable(nil, UUIDType, u[:])
	require.Equal(t, "4cc52088295074fb8fc9b349ecdee658", hexs(b))
}

func hexs(b []byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, h[c>>4], h[c&15])
	}
	return string(out)
}

func TestVectorType(t *testing.T) {
	for _, src := range []string{"vector<float, 4>", "vector<text, 2>", "frozen<vector<float, 3>>"} {
		x, err := Parse(src)
		require.NoError(t, err)
		y, err := Parse(x.String())
		require.NoError(t, err, x.String())
		require.True(t, x.Equal(y))
	}
	x, _ := Parse("vector<text, 2>")
	v := BuildVector(x, [][]byte{[]byte("ab"), []byte("")})
	elems, err := SplitVector(x, v)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("ab"), []byte("")}, elems)
	require.NoError(t, x.Validate(v))
	require.Error(t, x.Validate(v[:2]))
	f, _ := Parse("vector<float, 3>")
	require.Equal(t, "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)", f.MarshalClass())
	_, err = Parse("vector<float, 0>")
	require.Error(t, err)
}
