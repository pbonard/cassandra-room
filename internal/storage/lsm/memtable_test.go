package lsm

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestMemTableOrderAndGet(t *testing.T) {
	m := newMemTable(new(atomic.Int64))
	rng := rand.New(rand.NewPCG(1, 2))
	type entry struct {
		key     string
		trailer uint64
	}
	var model []entry
	latest := map[string]uint64{}
	for seq := uint64(1); seq <= 5000; seq++ {
		key := fmt.Sprintf("k%04d", rng.IntN(1000))
		tr := makeTrailer(seq, pebble.InternalKeyKindSet)
		m.add([]byte(key), tr, []byte(fmt.Sprint(seq)))
		model = append(model, entry{key, tr})
		latest[key] = seq
	}
	slices.SortFunc(model, func(a, b entry) int {
		if c := bytes.Compare([]byte(a.key), []byte(b.key)); c != 0 {
			return c
		}
		if a.trailer > b.trailer {
			return -1
		}
		return 1
	})
	i := 0
	for n := m.first(); n != nil; n = n.next() {
		require.Equal(t, model[i].key, string(n.key))
		require.Equal(t, model[i].trailer, n.trailer)
		i++
	}
	require.Equal(t, len(model), i)

	for key, seq := range latest {
		n := m.get([]byte(key), 5000)
		require.NotNil(t, n)
		require.Equal(t, fmt.Sprint(seq), string(n.value))
		// Entries above the read sequence number are invisible.
		if older := m.get([]byte(key), seq-1); older != nil {
			require.Less(t, older.trailer>>8, seq)
		}
	}
	require.Nil(t, m.get([]byte("missing"), 5000))
}

// TestMemTableConcurrentReaders runs readers against the single writer under
// the race detector.
func TestMemTableConcurrentReaders(t *testing.T) {
	m := newMemTable(new(atomic.Int64))
	var wg sync.WaitGroup
	done := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				var prev []byte
				for n := m.first(); n != nil; n = n.next() {
					if prev != nil && bytes.Compare(prev, n.key) > 0 {
						t.Errorf("out of order: %q after %q", n.key, prev)
						return
					}
					prev = n.key
				}
			}
		}()
	}
	for seq := uint64(1); seq <= 20000; seq++ {
		m.add([]byte(fmt.Sprintf("k%06d", (seq*7919)%20000)), makeTrailer(seq, pebble.InternalKeyKindSet), nil)
	}
	close(done)
	wg.Wait()
}

func TestVersionEditRoundTrip(t *testing.T) {
	e := &versionEdit{
		nextFileNum: 42,
		lastSeq:     1 << 40,
		minWAL:      7,
		tables: []*tableEdit{
			{id: 3, created: true, flushedSeq: 5},
			{
				id:         9,
				deleted:    []uint64{3, 5},
				flushedSeq: 20,
				added: []*fileMeta{{
					num:      9,
					size:     1234,
					minSeq:   10,
					maxSeq:   20,
					smallest: pebble.MakeInternalKey([]byte("a"), 10, pebble.InternalKeyKindSet),
					largest:  pebble.MakeInternalKey([]byte("zz"), 20, pebble.InternalKeyKindDelete),
				}},
			},
			{id: 4, dropped: true},
		},
	}
	got, err := decodeVersionEdit(e.encode())
	require.NoError(t, err)
	require.Equal(t, e.nextFileNum, got.nextFileNum)
	require.Equal(t, e.lastSeq, got.lastSeq)
	require.Equal(t, e.minWAL, got.minWAL)
	require.Len(t, got.tables, 3)
	require.Equal(t, *e.tables[0], *got.tables[0])
	require.Equal(t, e.tables[1].deleted, got.tables[1].deleted)
	require.Equal(t, e.tables[1].flushedSeq, got.tables[1].flushedSeq)
	want, have := e.tables[1].added[0], got.tables[1].added[0]
	require.Equal(t, []any{want.num, want.size, want.minSeq, want.maxSeq, want.minTS, want.maxTS, want.smallest, want.largest},
		[]any{have.num, have.size, have.minSeq, have.maxSeq, have.minTS, have.maxTS, have.smallest, have.largest})
	require.Equal(t, *e.tables[2], *got.tables[2])

	_, err = decodeVersionEdit([]byte{tagAddFile, 1})
	require.ErrorIs(t, err, errCorruptManifest)

	// Edits for a table that was never created are rejected on replay.
	s := newManifestState()
	require.ErrorIs(t, s.apply(&versionEdit{tables: []*tableEdit{{id: 1, flushedSeq: 2}}}), errCorruptManifest)
}

// TestMemTableConcurrentGet checks that a lookup never misses an entry that
// was fully inserted before it started, while the writer keeps inserting and
// memtables are replaced. A lookup that reloaded the next pointer after the
// final comparison returned a concurrently inserted smaller node here.
func TestMemTableConcurrentGet(t *testing.T) {
	const keys = 200
	var cur atomic.Pointer[memTable]
	var lastIn [keys]atomic.Pointer[memTable]
	var lastSeq [keys]atomic.Uint64
	var visible atomic.Uint64
	cur.Store(newMemTable(new(atomic.Int64)))
	var wg sync.WaitGroup
	done := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for k := r; ; k = (k + 13) % keys {
				select {
				case <-done:
					return
				default:
				}
				m := lastIn[k].Load()
				want := lastSeq[k].Load()
				seq := visible.Load()
				if m == nil || want > seq {
					continue
				}
				if n := m.get([]byte(fmt.Sprintf("k%03d", k)), seq); n == nil {
					t.Errorf("missed key %d (seq %d) at visible seq %d", k, want, seq)
					return
				}
			}
		}(r)
	}
	size := 0
	for seq := uint64(1); seq <= 300000; seq++ {
		if size >= 16<<10 {
			cur.Store(newMemTable(new(atomic.Int64)))
			size = 0
		}
		m := cur.Load()
		k := int(seq % keys)
		key := []byte(fmt.Sprintf("k%03d", k))
		m.add(key, makeTrailer(seq, pebble.InternalKeyKindSet), []byte(fmt.Sprint(seq)))
		size += len(key) + 70
		visible.Store(seq)
		lastSeq[k].Store(seq)
		lastIn[k].Store(m)
	}
	close(done)
	wg.Wait()
}

// TestMemTableConcurrentInsert links nodes from many goroutines at once, as
// concurrent batches do, and checks that the list ends up complete and
// ordered.
func TestMemTableConcurrentInsert(t *testing.T) {
	m := newMemTable(new(atomic.Int64))
	const writers, perWriter = 8, 5000
	var allocMu sync.Mutex
	var wg sync.WaitGroup
	var seq atomic.Uint64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Allocation is serialized, as Apply does under commitMu.
				allocMu.Lock()
				n := m.newNode([]byte(fmt.Sprintf("k%05d", (w*perWriter+i)*7919%(writers*perWriter))),
					makeTrailer(seq.Add(1), pebble.InternalKeyKindMerge), nil)
				allocMu.Unlock()
				m.insert(n)
			}
		}(w)
	}
	wg.Wait()
	count := 0
	var prev *memNode
	for n := m.first(); n != nil; n = n.next() {
		if prev != nil {
			require.Less(t, compareNode(prev, n.key, n.trailer), 0, "out of order")
		}
		prev = n
		count++
	}
	require.Equal(t, writers*perWriter, count)
	for level := 1; level < maxHeight; level++ {
		var p *memNode
		for n := m.head.tower[level].Load(); n != nil; n = n.tower[level].Load() {
			if p != nil {
				require.Less(t, compareNode(p, n.key, n.trailer), 0, "level %d out of order", level)
			}
			p = n
		}
	}
}
