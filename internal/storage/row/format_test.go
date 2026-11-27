package row

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// appendV2 encodes r in version 2, the format before the cellLive flag.
func appendV2(r *Row) []byte {
	flags := byte(0)
	if !r.Deletion.IsLive() {
		flags |= flagHasDeletion
	}
	dst := []byte{2, flags}
	if flags&flagHasDeletion != 0 {
		dst = binary.AppendVarint(dst, r.Deletion.MarkedForDeleteAt)
		dst = binary.AppendVarint(dst, r.Deletion.LocalDeletionTime)
	}
	dst = binary.AppendUvarint(dst, uint64(len(r.Cells)))
	for i := range r.Cells {
		c := &r.Cells[i]
		f := uint64(0)
		if c.Path != nil {
			f |= cellHasPath
		}
		if c.Counter {
			f |= cellCounter
		}
		if c.ComplexDeletion {
			f |= cellComplexDeletion
		}
		dst = binary.AppendUvarint(dst, uint64(c.Column)<<3|f)
		if c.Path != nil {
			dst = binary.AppendUvarint(dst, uint64(len(c.Path)))
			dst = append(dst, c.Path...)
		}
		dst = binary.AppendVarint(dst, c.Timestamp)
		dst = binary.AppendVarint(dst, c.LocalDeletionTime)
		dst = binary.AppendUvarint(dst, uint64(c.TTL))
		if !c.IsTombstone() {
			dst = binary.AppendUvarint(dst, uint64(len(c.Value)))
			dst = append(dst, c.Value...)
		}
	}
	return dst
}

func formatRow() *Row {
	return &Row{
		Deletion: Deletion{MarkedForDeleteAt: 5, LocalDeletionTime: 100},
		Cells: []Cell{
			live(1, 10, "a"),
			tombstone(2, 11, 200),
			expiring(3, 12, 300, 60, "c"),
			{Column: 4, ComplexDeletion: true, Timestamp: 13, LocalDeletionTime: 400},
			{Column: 4, Path: []byte{1}, Timestamp: 14, LocalDeletionTime: NoDeletionTime, Value: []byte("e1")},
			{Column: 4, Path: []byte{2}, Timestamp: 15, LocalDeletionTime: 500, TTL: 9, Value: []byte("e2")},
			live(1<<20, 17, "wide column id"),
		},
	}
}

// TestFormatV3 checks that version 3 round-trips every kind of cell, that
// version 2 values still decode and merge, and that live cells lose their
// deletion time and TTL.
func TestFormatV3(t *testing.T) {
	r := formatRow()
	v3, v2 := Append(nil, r), appendV2(r)
	require.Equal(t, byte(3), v3[0])
	for _, v := range [][]byte{v3, v2} {
		got, err := Decode(v)
		require.NoError(t, err)
		require.Equal(t, r, got)
	}
	// Three live cells without TTL (columns 1, 4 element 1, 1<<20): 10
	// bytes of local deletion time and 1 of TTL each.
	require.Equal(t, len(v2)-3*11, len(v3))

	// A counter (a row deletion deletes counters, so in a row without).
	counter := &Row{Deletion: Deletion{MarkedForDeleteAt: NoTimestamp}, Cells: []Cell{
		{Column: 5, Counter: true, Timestamp: 16, LocalDeletionTime: NoDeletionTime, Value: bytes.Repeat([]byte{7}, counterShardSize)},
	}}
	for _, v := range [][]byte{Append(nil, counter), appendV2(counter)} {
		got, err := Decode(v)
		require.NoError(t, err)
		require.Equal(t, counter, got)
	}

	// Merging a version 2 value gives the version 3 encoding.
	other := Append(nil, &Row{Deletion: Deletion{MarkedForDeleteAt: NoTimestamp}, Cells: []Cell{live(2, 20, "b")}})
	m2, err := Merge(nil, v2, other)
	require.NoError(t, err)
	m3, err := Merge(nil, v3, other)
	require.NoError(t, err)
	require.Equal(t, m3, m2)
	m2, err = Merge(nil, v2, v2)
	require.NoError(t, err)
	require.Equal(t, v3, m2)

	// Version 2 statistics and purging read the same values.
	s2, err := Summarize(v2, nil)
	require.NoError(t, err)
	s3, err := Summarize(v3, nil)
	require.NoError(t, err)
	require.Equal(t, s3, s2)

	_, err = Decode([]byte{4, 0, 0})
	require.ErrorIs(t, err, ErrCorrupt)
}
