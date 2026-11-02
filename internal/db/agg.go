package db

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/types"
)

// aggState accumulates aggregate selectors (AggregateFcts); non-aggregate
// selectors take the first row's values, as Cassandra does.
type aggState struct {
	sels  []selector
	first [][]byte
	seen  bool
	count []int64
	vals  [][]byte   // min, max
	sums  []*big.Rat // sum, avg
}

func newAggState(sels []selector) *aggState {
	return &aggState{sels: sels, count: make([]int64, len(sels)), vals: make([][]byte, len(sels)), sums: make([]*big.Rat, len(sels))}
}

func (a *aggState) add(q *query, r *rowData) error {
	if !a.seen {
		a.seen = true
		a.first = make([][]byte, len(a.sels))
		for i := range a.sels {
			if a.sels[i].kind < selCount || a.sels[i].kind > selAvg {
				v, err := q.evaluate(&a.sels[i], r)
				if err != nil {
					return err
				}
				a.first[i] = v
			}
		}
	}
	for i, s := range a.sels {
		var v []byte
		if s.col != nil {
			v = r.value(s.col)
			if v == nil {
				continue
			}
		}
		switch s.kind {
		case selCount:
			a.count[i]++
		case selMin, selMax:
			if a.vals[i] == nil {
				a.vals[i] = v
				continue
			}
			c := types.Compare(s.typ, v, a.vals[i])
			if (s.kind == selMin && c < 0) || (s.kind == selMax && c > 0) {
				a.vals[i] = v
			}
		case selSum, selAvg:
			if len(v) == 0 {
				continue
			}
			n := numericValue(s.typ, v)
			if n == nil {
				// NaN and infinities: keep them as floats.
				n = new(big.Rat)
			}
			if a.sums[i] == nil {
				a.sums[i] = new(big.Rat)
			}
			a.sums[i].Add(a.sums[i], n)
			a.count[i]++
		}
	}
	return nil
}

func (a *aggState) result(*query) ([][]byte, error) {
	out := make([][]byte, len(a.sels))
	for i, s := range a.sels {
		switch s.kind {
		case selCount:
			out[i] = binary.BigEndian.AppendUint64(nil, uint64(a.count[i]))
		case selMin, selMax:
			out[i] = a.vals[i]
		case selSum:
			sum := a.sums[i]
			if sum == nil {
				sum = new(big.Rat)
			}
			out[i] = fromRat(s.typ, sum)
		case selAvg:
			avg := new(big.Rat)
			if a.count[i] > 0 {
				avg.Quo(a.sums[i], new(big.Rat).SetInt64(a.count[i]))
			}
			out[i] = fromRat(s.typ, avg)
		default:
			if a.seen {
				out[i] = a.first[i]
			}
		}
	}
	return out, nil
}

// formatValue renders a value as CQL text (AbstractType.toCQLString).
func formatValue(t *types.Type, v []byte) string {
	if len(v) == 0 && t.Kind != types.Text && t.Kind != types.Ascii {
		return ""
	}
	switch t.Kind {
	case types.Text, types.Ascii:
		return string(v)
	case types.Boolean:
		return strconv.FormatBool(v[0] != 0)
	case types.Tinyint:
		return strconv.Itoa(int(int8(v[0])))
	case types.Smallint:
		return strconv.Itoa(int(int16(binary.BigEndian.Uint16(v))))
	case types.Int:
		return strconv.Itoa(int(int32(binary.BigEndian.Uint32(v))))
	case types.Bigint, types.Counter:
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(v)), 10)
	case types.Float:
		return formatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(v))), 32)
	case types.Double:
		return formatFloat(math.Float64frombits(binary.BigEndian.Uint64(v)), 64)
	case types.Varint:
		return types.VarintValue(v).String()
	case types.Decimal:
		scale := int(int32(binary.BigEndian.Uint32(v)))
		s := types.VarintValue(v[4:]).String()
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
	case types.UUID, types.Timeuuid:
		return types.FormatUUID(v)
	case types.Timestamp:
		return time.UnixMilli(int64(binary.BigEndian.Uint64(v))).UTC().Format("2006-01-02T15:04:05.000Z")
	case types.Date:
		days := int64(binary.BigEndian.Uint32(v)) - 1<<31
		return time.Unix(days*86400, 0).UTC().Format("2006-01-02")
	case types.Time:
		ns := int64(binary.BigEndian.Uint64(v))
		return fmt.Sprintf("%02d:%02d:%02d.%09d", ns/3600e9, ns/60e9%60, ns/1e9%60, ns%1e9)
	case types.Inet:
		return net.IP(v).String()
	}
	return "0x" + hex.EncodeToString(v)
}

func formatFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	s := strconv.FormatFloat(f, 'f', -1, bits)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}
