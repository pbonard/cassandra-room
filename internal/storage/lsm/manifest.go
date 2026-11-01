package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	currentFileName = "CURRENT"
	tablesDirName   = "tables"
)

func walFileName(num uint64) string      { return fmt.Sprintf("%06d.wal", num) }
func tableFileName(num uint64) string    { return fmt.Sprintf("%06d.sst", num) }
func manifestFileName(num uint64) string { return fmt.Sprintf("MANIFEST-%06d", num) }

// parseFileName returns the kind ("wal", "sst" or "manifest") and number of
// a file the engine created, or ok=false for any other file.
func parseFileName(name string) (kind string, num uint64, ok bool) {
	if s, found := strings.CutPrefix(name, "MANIFEST-"); found {
		_, err := fmt.Sscanf(s, "%d", &num)
		return "manifest", num, err == nil
	}
	for _, k := range []string{"wal", "sst"} {
		if s, found := strings.CutSuffix(name, "."+k); found {
			_, err := fmt.Sscanf(s, "%d", &num)
			return k, num, err == nil
		}
	}
	return "", 0, false
}

// fileMeta describes one sstable of a table.
type fileMeta struct {
	num               uint64
	size              uint64
	smallest, largest pebble.InternalKey
	minSeq, maxSeq    uint64
	// minTS and maxTS are the smallest and largest write timestamps in the
	// table (Cassandra's Statistics.db min/max timestamp). Reads skip tables
	// whose data is all older than a row deletion already found.
	minTS, maxTS int64
	stats        fileStats

	// Runtime state, not part of the manifest.
	reader *tableReader
	path   string
	fs     vfs.FS
	// refs counts the read states and compactions using the file.
	refs atomic.Int32
	// obsolete is set when a compaction replaced the file; it is deleted
	// once unreferenced.
	obsolete atomic.Bool
	// compacting is set while a compaction uses the file. Guarded by
	// Engine.mu.
	compacting bool
}

// unref drops a reference. The last one closes the file, and deletes it if
// a compaction replaced it.
func (f *fileMeta) unref() {
	if f.refs.Add(-1) != 0 {
		return
	}
	if f.reader != nil {
		_ = f.reader.close()
	}
	if f.obsolete.Load() {
		_ = f.fs.Remove(f.path)
	}
}

// tableEdit is the part of a version edit that concerns one table. Fields
// apply in this order: created, added, deleted, flushedSeq, dropped.
type tableEdit struct {
	id         TableID
	created    bool
	added      []*fileMeta
	deleted    []uint64
	flushedSeq uint64 // 0 = unchanged
	dropped    bool
}

// versionEdit is one manifest record. Zero numbers mean "unchanged".
type versionEdit struct {
	tables      []*tableEdit
	nextFileNum uint64
	lastSeq     uint64
	minWAL      uint64
}

const (
	tagNextFileNum = 1
	tagLastSeq     = 2
	tagMinWAL      = 3
	tagAddFile     = 4  // table, num, size, minSeq, maxSeq, minTS, maxTS, smallest, largest
	tagDeleteFile  = 5  // table, num
	tagCreateTable = 6  // table
	tagDropTable   = 7  // table
	tagFlushedSeq  = 8  // table, seq
	tagFileStats   = 9  // table, num, fileStats without cells (M5); follows tagAddFile
	tagFileStats2  = 10 // table, num, fileStats; follows the file's tagAddFile
)

var errCorruptManifest = errors.New("lsm: corrupt manifest")

func (e *versionEdit) encode() []byte {
	var b []byte
	put := func(vs ...uint64) {
		for _, v := range vs {
			b = binary.AppendUvarint(b, v)
		}
	}
	for _, kv := range [][2]uint64{
		{tagNextFileNum, e.nextFileNum}, {tagLastSeq, e.lastSeq}, {tagMinWAL, e.minWAL},
	} {
		if kv[1] != 0 {
			put(kv[0], kv[1])
		}
	}
	for _, t := range e.tables {
		id := uint64(t.id)
		if t.created {
			put(tagCreateTable, id)
		}
		for _, f := range t.added {
			put(tagAddFile, id, f.num, f.size, f.minSeq, f.maxSeq)
			b = binary.AppendVarint(b, f.minTS)
			b = binary.AppendVarint(b, f.maxTS)
			b = appendInternalKey(b, f.smallest)
			b = appendInternalKey(b, f.largest)
			if f.stats.valid {
				put(tagFileStats2, id, f.num)
				b = appendFileStats(b, &f.stats)
			}
		}
		for _, num := range t.deleted {
			put(tagDeleteFile, id, num)
		}
		if t.flushedSeq != 0 {
			put(tagFlushedSeq, id, t.flushedSeq)
		}
		if t.dropped {
			put(tagDropTable, id)
		}
	}
	return b
}

func appendInternalKey(b []byte, k pebble.InternalKey) []byte {
	b = binary.AppendUvarint(b, uint64(len(k.UserKey)))
	b = append(b, k.UserKey...)
	return binary.LittleEndian.AppendUint64(b, uint64(k.Trailer))
}

func decodeVersionEdit(b []byte) (*versionEdit, error) {
	e := &versionEdit{}
	var err error
	readNum := func() uint64 {
		if err != nil {
			return 0
		}
		v, n := binary.Uvarint(b)
		if n <= 0 {
			err = errCorruptManifest
			return 0
		}
		b = b[n:]
		return v
	}
	readVarint := func() int64 {
		if err != nil {
			return 0
		}
		v, n := binary.Varint(b)
		if n <= 0 {
			err = errCorruptManifest
			return 0
		}
		b = b[n:]
		return v
	}
	readKey := func() pebble.InternalKey {
		n := readNum()
		if err != nil || uint64(len(b)) < n+8 {
			err = errCorruptManifest
			return pebble.InternalKey{}
		}
		k := pebble.InternalKey{
			UserKey: append([]byte(nil), b[:n]...),
			Trailer: pebble.InternalKeyTrailer(binary.LittleEndian.Uint64(b[n : n+8])),
		}
		b = b[n+8:]
		return k
	}
	// Consecutive records for the same table share one tableEdit, which keeps
	// the order created, added, deleted, flushedSeq, dropped.
	tableEditFor := func(id TableID) *tableEdit {
		if n := len(e.tables); n > 0 && e.tables[n-1].id == id {
			return e.tables[n-1]
		}
		t := &tableEdit{id: id}
		e.tables = append(e.tables, t)
		return t
	}
	for len(b) > 0 && err == nil {
		switch tag := readNum(); tag {
		case tagNextFileNum:
			e.nextFileNum = readNum()
		case tagLastSeq:
			e.lastSeq = readNum()
		case tagMinWAL:
			e.minWAL = readNum()
		case tagCreateTable:
			tableEditFor(TableID(readNum())).created = true
		case tagDropTable:
			tableEditFor(TableID(readNum())).dropped = true
		case tagFlushedSeq:
			t := tableEditFor(TableID(readNum()))
			t.flushedSeq = readNum()
		case tagDeleteFile:
			t := tableEditFor(TableID(readNum()))
			t.deleted = append(t.deleted, readNum())
		case tagAddFile:
			t := tableEditFor(TableID(readNum()))
			f := &fileMeta{num: readNum(), size: readNum(), minSeq: readNum(), maxSeq: readNum()}
			f.minTS = readVarint()
			f.maxTS = readVarint()
			f.smallest = readKey()
			f.largest = readKey()
			t.added = append(t.added, f)
		case tagFileStats, tagFileStats2:
			t := tableEditFor(TableID(readNum()))
			num := readNum()
			s := fileStats{valid: true, created: readVarint(), entries: readNum()}
			if tag == tagFileStats2 {
				s.cells = readNum()
			} else {
				s.cells = s.entries // M5 files: one cell per row assumed
			}
			s.tombstones, s.maxLDT, s.minLDT = readNum(), readVarint(), readVarint()
			n := readNum()
			if err == nil && n > uint64(len(b)) {
				err = errCorruptManifest
			}
			for i := uint64(0); i < n && err == nil; i++ {
				s.hist.points = append(s.hist.points, readVarint())
				s.hist.counts = append(s.hist.counts, readNum())
			}
			if l := len(t.added); err == nil && (l == 0 || t.added[l-1].num != num) {
				err = fmt.Errorf("%w: stats for file %d not after it", errCorruptManifest, num)
			}
			if err == nil {
				t.added[len(t.added)-1].stats = s
			}
		default:
			if err == nil {
				err = fmt.Errorf("%w: unknown tag %d", errCorruptManifest, tag)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}

// tableManifest is one table's state in a replayed manifest.
type tableManifest struct {
	files      map[uint64]*fileMeta
	flushedSeq uint64
}

// manifestState is the result of replaying a manifest.
type manifestState struct {
	tables      map[TableID]*tableManifest
	nextFileNum uint64
	lastSeq     uint64
	minWAL      uint64
}

func newManifestState() *manifestState {
	return &manifestState{tables: map[TableID]*tableManifest{}}
}

func (s *manifestState) apply(e *versionEdit) error {
	for _, te := range e.tables {
		if te.created {
			if _, ok := s.tables[te.id]; ok {
				return fmt.Errorf("%w: table %d created twice", errCorruptManifest, te.id)
			}
			s.tables[te.id] = &tableManifest{files: map[uint64]*fileMeta{}}
		}
		t, ok := s.tables[te.id]
		if !ok {
			return fmt.Errorf("%w: edit for unknown table %d", errCorruptManifest, te.id)
		}
		for _, f := range te.added {
			t.files[f.num] = f
		}
		for _, num := range te.deleted {
			delete(t.files, num)
		}
		t.flushedSeq = max(t.flushedSeq, te.flushedSeq)
		if te.dropped {
			delete(s.tables, te.id)
		}
	}
	s.nextFileNum = max(s.nextFileNum, e.nextFileNum)
	s.lastSeq = max(s.lastSeq, e.lastSeq)
	s.minWAL = max(s.minWAL, e.minWAL)
	return nil
}

// loadManifest replays the manifest named by CURRENT. It returns ok=false if
// there is no CURRENT file (a new engine). A torn tail ends the replay: the
// last edit was never acknowledged, since edits are synced before they take
// effect.
func loadManifest(fs vfs.FS, dir string) (state *manifestState, ok bool, err error) {
	f, err := fs.Open(fs.PathJoin(dir, currentFileName))
	if err != nil {
		if _, serr := fs.Stat(fs.PathJoin(dir, currentFileName)); serr != nil {
			return nil, false, nil
		}
		return nil, false, err
	}
	cur, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, false, err
	}
	kind, num, valid := parseFileName(strings.TrimSpace(string(cur)))
	if !valid || kind != "manifest" {
		return nil, false, fmt.Errorf("%w: CURRENT is %q", errCorruptManifest, cur)
	}
	mf, err := fs.Open(fs.PathJoin(dir, manifestFileName(num)))
	if err != nil {
		return nil, false, err
	}
	defer mf.Close()
	state = newManifestState()
	r := record.NewReader(mf, 0)
	for {
		rr, err := r.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		data, err := io.ReadAll(rr)
		if record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		e, err := decodeVersionEdit(data)
		if err != nil {
			return nil, false, err
		}
		if err := state.apply(e); err != nil {
			return nil, false, err
		}
	}
	return state, true, nil
}

// manifestWriter appends version edits to one manifest file.
type manifestWriter struct {
	num uint64
	f   vfs.File
	w   *record.Writer
}

// createManifest writes a new manifest whose first edit is snapshot, then
// points CURRENT at it.
func createManifest(fs vfs.FS, dir string, num uint64, snapshot *versionEdit) (*manifestWriter, error) {
	f, err := fs.Create(fs.PathJoin(dir, manifestFileName(num)), vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	m := &manifestWriter{num: num, f: f, w: record.NewWriter(f)}
	if err := m.write(snapshot); err != nil {
		m.close()
		return nil, err
	}
	if err := setCurrent(fs, dir, num); err != nil {
		m.close()
		return nil, err
	}
	return m, nil
}

// write appends e and syncs it.
func (m *manifestWriter) write(e *versionEdit) error {
	if _, err := m.w.WriteRecord(e.encode()); err != nil {
		return err
	}
	if err := m.w.Flush(); err != nil {
		return err
	}
	return m.f.Sync()
}

func (m *manifestWriter) close() error {
	err := m.w.Close()
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// setCurrent atomically points CURRENT at manifest num.
func setCurrent(fs vfs.FS, dir string, num uint64) error {
	tmp := fs.PathJoin(dir, currentFileName+".tmp")
	f, err := fs.Create(tmp, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(f, manifestFileName(num)+"\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, fs.PathJoin(dir, currentFileName)); err != nil {
		return err
	}
	return syncDir(fs, dir)
}

func syncDir(fs vfs.FS, dir string) error {
	d, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
