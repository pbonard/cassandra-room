package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Vectors (CEP-30, VectorType): a fixed number of elements of one type,
// vector<float, 3>. Elements of fixed-size types are concatenated;
// variable-size ones are each prefixed with an unsigned vint length. On the
// wire the type is a custom type naming its marshal class.

// VectorOf returns vector<elem, dim>.
func VectorOf(elem *Type, dim int) *Type {
	return &Type{Kind: Vector, Elems: []*Type{elem}, Dim: dim, Frozen: true}
}

// SplitVector splits a serialized vector into its elements.
func SplitVector(t *Type, v []byte) ([][]byte, error) {
	out := make([][]byte, 0, t.Dim)
	size := t.Elems[0].fixedSize()
	for i := 0; i < t.Dim; i++ {
		n := size
		if n < 0 {
			l, k := binary.Uvarint(v)
			if k <= 0 {
				return nil, errors.New("invalid vector value")
			}
			v, n = v[k:], int(l)
		}
		if n > len(v) {
			return nil, fmt.Errorf("vector value too short for %d elements", t.Dim)
		}
		out = append(out, v[:n])
		v = v[n:]
	}
	if len(v) != 0 {
		return nil, fmt.Errorf("vector value has extra bytes after %d elements", t.Dim)
	}
	return out, nil
}

// BuildVector serializes elements as a vector of t.
func BuildVector(t *Type, elems [][]byte) []byte {
	var out []byte
	fixed := t.Elems[0].fixedSize() >= 0
	for _, e := range elems {
		if !fixed {
			out = binary.AppendUvarint(out, uint64(len(e)))
		}
		out = append(out, e...)
	}
	return out
}

// FloatVector decodes a vector of floats (or doubles) for similarity.
func FloatVector(t *Type, v []byte) ([]float32, error) {
	if t.Kind != Vector || (t.Elems[0].Kind != Float && t.Elems[0].Kind != Double) {
		return nil, fmt.Errorf("%s is not a vector of floats", t)
	}
	elems, err := SplitVector(t, v)
	if err != nil {
		return nil, err
	}
	out := make([]float32, len(elems))
	for i, e := range elems {
		if t.Elems[0].Kind == Float {
			out[i] = math.Float32frombits(binary.BigEndian.Uint32(e))
		} else {
			out[i] = float32(math.Float64frombits(binary.BigEndian.Uint64(e)))
		}
	}
	return out, nil
}

var marshalNames = map[Kind]string{
	Ascii: "AsciiType", Bigint: "LongType", Blob: "BytesType", Boolean: "BooleanType", Counter: "CounterColumnType",
	Date: "SimpleDateType", Decimal: "DecimalType", Double: "DoubleType", Duration: "DurationType", Float: "FloatType",
	Inet: "InetAddressType", Int: "Int32Type", Smallint: "ShortType", Text: "UTF8Type", Time: "TimeType",
	Timestamp: "TimestampType", Timeuuid: "TimeUUIDType", Tinyint: "ByteType", UUID: "UUIDType", Varint: "IntegerType",
}

// MarshalClass is the type's Cassandra marshal class name
// (AbstractType.toString), as custom types appear on the wire.
func (t *Type) MarshalClass() string {
	const pkg = "org.apache.cassandra.db.marshal."
	elems := func() string {
		parts := make([]string, len(t.Elems))
		for i, e := range t.Elems {
			parts[i] = e.MarshalClass()
		}
		return strings.Join(parts, ",")
	}
	var s string
	switch t.Kind {
	case Vector:
		// TypeParser.stringifyVectorParameters: "(type , dimension)".
		return fmt.Sprintf("%sVectorType(%s , %d)", pkg, t.Elems[0].MarshalClass(), t.Dim)
	case List:
		s = pkg + "ListType(" + elems() + ")"
	case Set:
		s = pkg + "SetType(" + elems() + ")"
	case Map:
		s = pkg + "MapType(" + elems() + ")"
	case Tuple:
		return pkg + "TupleType(" + elems() + ")"
	case UDT:
		s = pkg + "UserType(" + t.Keyspace + "," + fmt.Sprintf("%x", t.Name) + ")"
	default:
		return pkg + marshalNames[t.Kind]
	}
	if t.Frozen {
		return pkg + "FrozenType(" + s + ")"
	}
	return s
}
