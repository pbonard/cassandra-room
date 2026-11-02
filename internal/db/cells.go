package db

import (
	"encoding/binary"
	"slices"
	"sort"

	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
)

// Multi-cell columns (ComplexColumnData): a non-frozen collection or user
// type is stored as one cell per element, whose path identifies it:
//
//	set: the element (byte-comparable), empty value
//	map: the key (byte-comparable), the value
//	list: a time-ordered id (Lists use time UUIDs), the element
//	UDT: the field index (2 bytes), the field's value
//
// Paths sort as the elements do, so a column's cells read back in order.
// Overwriting the whole column first deletes it with a complex deletion at
// the write's timestamp minus one, as Cassandra's setters do.

// elementPath is a set element's or map key's path.
func elementPath(t *types.Type, v []byte) ([]byte, error) {
	return types.AppendComponent(nil, t, v, false)
}

func fieldPath(i int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(i)) }

// listPath returns the path of a list element appended (or prepended) at
// ts: the timestamp, sign-flipped (negated for prepends, so later prepends
// come first), then a sequence number and a node-unique suffix.
func (d *DB) listPath(ts int64, prepend bool) []byte {
	t := ts
	if prepend {
		t = -ts
	}
	out := binary.BigEndian.AppendUint64(make([]byte, 0, 16), uint64(t)^(1<<63))
	out = binary.BigEndian.AppendUint32(out, d.listSeq.Add(1))
	return append(out, d.nodeSuffix[:]...)
}

// columnCells returns the cells of column id in sorted cells.
func columnCells(cells []row.Cell, id uint32) []row.Cell {
	i := sort.Search(len(cells), func(i int) bool { return cells[i].Column >= id })
	j := i
	for j < len(cells) && cells[j].Column == id {
		j++
	}
	return cells[i:j]
}

// assemble rebuilds a multi-cell column's value from its live cells, nil
// if there are none (CollectionType.serializeForNativeProtocol).
func assemble(t *types.Type, cells []row.Cell) []byte {
	var elems [][]byte
	n := 0
	for _, c := range cells {
		if c.Path == nil || c.ComplexDeletion {
			continue
		}
		switch t.Kind {
		case types.Set:
			k, _, err := types.ReadComponent(c.Path, t.Elems[0], false)
			if err != nil {
				continue
			}
			elems = append(elems, k)
		case types.Map:
			k, _, err := types.ReadComponent(c.Path, t.Elems[0], false)
			if err != nil {
				continue
			}
			elems = append(elems, k, c.Value)
		case types.List:
			elems = append(elems, c.Value)
		case types.UDT:
			if len(c.Path) != 2 {
				continue
			}
			i := int(binary.BigEndian.Uint16(c.Path))
			if i >= len(t.Elems) {
				continue
			}
			for len(elems) <= i {
				elems = append(elems, nil)
			}
			elems[i] = c.Value
		}
		n++
	}
	if n == 0 {
		return nil
	}
	switch t.Kind {
	case types.UDT:
		for len(elems) < len(t.Elems) {
			elems = append(elems, nil)
		}
		return types.BuildTuple(elems)
	case types.Map:
		return types.BuildCollection(len(elems)/2, elems)
	}
	return types.BuildCollection(len(elems), elems)
}

// elementCells returns the cells of a whole multi-cell value.
func (d *DB) elementCells(c *Column, v []byte, ts int64, prepend bool, cell func(path, value []byte) row.Cell) ([]row.Cell, error) {
	t := c.Type
	var out []row.Cell
	switch t.Kind {
	case types.Set, types.List:
		elems, err := types.SplitCollection(v, 1)
		if err != nil {
			return nil, invalidf("Invalid collection value for %s", c.Name)
		}
		for _, e := range elems {
			if t.Kind == types.List {
				out = append(out, cell(d.listPath(ts, prepend), e))
				continue
			}
			p, err := elementPath(t.Elems[0], e)
			if err != nil {
				return nil, err
			}
			out = append(out, cell(p, []byte{}))
		}
		if t.Kind == types.List && prepend {
			// The paths of one prepend are increasing: keep the order.
			slices.SortStableFunc(out, func(a, b row.Cell) int { return row.Compare(&a, &b) })
		}
	case types.Map:
		elems, err := types.SplitCollection(v, 2)
		if err != nil {
			return nil, invalidf("Invalid map value for %s", c.Name)
		}
		for i := 0; i < len(elems); i += 2 {
			p, err := elementPath(t.Elems[0], elems[i])
			if err != nil {
				return nil, err
			}
			out = append(out, cell(p, elems[i+1]))
		}
	case types.UDT:
		fields, err := types.SplitTuple(v)
		if err != nil {
			return nil, invalidf("Invalid user type value for %s", c.Name)
		}
		for i, f := range fields {
			if f != nil {
				out = append(out, cell(fieldPath(i), f))
			}
		}
	}
	return out, nil
}

func sortCells(cells []row.Cell) {
	slices.SortFunc(cells, func(a, b row.Cell) int { return row.Compare(&a, &b) })
}
