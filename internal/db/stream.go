package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cockroachdb/pebble/vfs"
)

// Streaming moves key ranges of tables between nodes as sstable files, as
// Cassandra's entire-sstable streaming does (CASSANDRA-14556): the source
// exports the range (lsm.Table.ExportRange hard-links sstables wholly
// inside it and writes the sections of the others), the receiver pulls the
// files in chunks over the large-message connection, imports them
// (hard-linked into the table, not rewritten) and indexes the rows. The
// receiver always pulls; a node that must push a range asks the receiver
// to pull it.
//
// Bootstrap, decommission and removenode record each range they finish,
// so an interrupted operation resumes where it stopped instead of
// streaming everything again (Cassandra's resumable bootstrap and
// system.available_ranges).

const streamChunk = 4 << 20

// streamRequest is a VerbStreamFile request (to the source) or a
// VerbStreamPush request (to a receiver: pull this range from From).
type streamRequest struct {
	Op      string // prepare, chunk, done (to the source)
	Session string
	Table   uint64
	Lo, Hi  []byte
	Name    string `json:",omitempty"`
	Offset  int64  `json:",omitempty"`
	From    string `json:",omitempty"`
}

type streamFile struct {
	Name string
	Size int64
}

func (d *DB) streamDir(kind, session string) string {
	return d.fs.PathJoin(d.dir, "streaming", kind+"-"+session)
}

// cleanStreaming removes what interrupted streams left.
func (d *DB) cleanStreaming() {
	_ = d.fs.RemoveAll(d.fs.PathJoin(d.dir, "streaming"))
}

// handleStreamFile serves a range's files to a receiver.
func (p *proxy) handleStreamFile(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var r streamRequest
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	if r.Session == "" || !isHex(r.Session) {
		return nil, errors.New("db: bad stream session")
	}
	d := p.d
	dir := d.streamDir("out", r.Session)
	switch r.Op {
	case "prepare":
		lt, err := d.engine.Table(lsm.TableID(r.Table))
		if err != nil {
			return nil, err
		}
		if err := d.fs.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		hi := r.Hi
		if len(hi) == 0 {
			hi = nil
		}
		paths, err := lt.ExportRange(r.Lo, hi, d.fs, dir)
		if err != nil {
			_ = d.fs.RemoveAll(dir)
			return nil, err
		}
		var files []streamFile
		for _, path := range paths {
			st, err := d.fs.Stat(path)
			if err != nil {
				return nil, err
			}
			files = append(files, streamFile{Name: d.fs.PathBase(path), Size: st.Size()})
		}
		return json.Marshal(files)
	case "chunk":
		if r.Name != d.fs.PathBase(r.Name) {
			return nil, errors.New("db: bad stream file name")
		}
		f, err := d.fs.Open(d.fs.PathJoin(dir, r.Name))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		buf := make([]byte, streamChunk)
		n, err := f.ReadAt(buf, r.Offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		return buf[:n], nil
	case "done":
		return nil, d.fs.RemoveAll(dir)
	}
	return nil, fmt.Errorf("db: unknown stream operation %q", r.Op)
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// handleStreamPush is a request to pull a range from another node.
func (p *proxy) handleStreamPush(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var r streamRequest
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	n, err := p.fetchFiles(ctx, r.From, lsm.TableID(r.Table), r.Lo, r.Hi)
	if err != nil {
		return nil, err
	}
	return []byte(strconv.FormatInt(n, 10)), nil
}

// fetchFiles pulls a key range of a table from a node as files and
// imports them; it returns the bytes received.
func (p *proxy) fetchFiles(ctx context.Context, from string, id lsm.TableID, lo, hi []byte) (int64, error) {
	d := p.d
	t := d.schema.Load().byStorage[id]
	if t == nil {
		return 0, invalidf("a table was dropped while it was streamed")
	}
	lt, err := d.engine.Table(id)
	if err != nil {
		return 0, err
	}
	var sid [8]byte
	_, _ = rand.Read(sid[:])
	session := hex.EncodeToString(sid[:])
	request := func(r streamRequest, timeout time.Duration) ([]byte, error) {
		r.Session, r.Table = session, uint64(id)
		b, _ := json.Marshal(r)
		rctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return p.m.Request(rctx, from, cluster.VerbStreamFile, b)
	}
	b, err := request(streamRequest{Op: "prepare", Lo: lo, Hi: hi}, 5*time.Minute)
	if err != nil {
		return 0, fmt.Errorf("db: preparing a stream from %s: %w", from, err)
	}
	defer func() { _, _ = request(streamRequest{Op: "done"}, 10*time.Second) }()
	var files []streamFile
	if err := json.Unmarshal(b, &files); err != nil {
		return 0, err
	}
	in := d.streamDir("in", session)
	if err := d.fs.MkdirAll(in, 0o755); err != nil {
		return 0, err
	}
	defer func() { _ = d.fs.RemoveAll(in) }()
	var paths []string
	var total int64
	for _, sf := range files {
		path := d.fs.PathJoin(in, d.fs.PathBase(sf.Name))
		if err := receiveFile(d.fs, path, sf, func(off int64) ([]byte, error) {
			return request(streamRequest{Op: "chunk", Name: sf.Name, Offset: off}, time.Minute)
		}); err != nil {
			return total, fmt.Errorf("db: streaming %s from %s: %w", sf.Name, from, err)
		}
		paths = append(paths, path)
		total += sf.Size
	}
	if len(paths) > 0 {
		if err := lt.Import(d.fs, paths); err != nil {
			return total, err
		}
	}
	return total, d.indexRange(t, lo, hi)
}

func receiveFile(fs vfs.FS, path string, sf streamFile, chunk func(off int64) ([]byte, error)) error {
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	defer f.Close()
	for off := int64(0); off < sf.Size; {
		b, err := chunk(off)
		if err != nil {
			return err
		}
		if len(b) == 0 {
			return io.ErrUnexpectedEOF
		}
		if _, err := f.Write(b); err != nil {
			return err
		}
		off += int64(len(b))
	}
	return f.Sync()
}

// indexRange writes the index entries of a table's rows in [lo, hi) (after
// an import, which bypasses the write path).
func (d *DB) indexRange(t *Table, lo, hi []byte) error {
	if len(t.Indexes) == 0 {
		return nil
	}
	lt, err := d.engine.Table(t.Storage)
	if err != nil {
		return err
	}
	if len(hi) == 0 {
		hi = nil
	}
	it, err := lt.NewIter(lo, hi)
	if err != nil {
		return err
	}
	var keys [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		keys = append(keys, slices.Clone(it.Key()))
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return err
	}
	for _, ix := range t.Indexes {
		if isFloatVector(ix.col.Type) {
			for _, k := range keys {
				if err := d.indexVector(t, ix, k); err != nil {
					return err
				}
			}
			continue
		}
		if err := d.buildIndexKeys(t, ix, keys, d.opts.Now().Unix()); err != nil {
			return err
		}
	}
	return nil
}

// streamBetween copies a key range of a table from one node to another:
// the receiver pulls it.
func (p *proxy) streamBetween(from, to string, id lsm.TableID, lo, hi []byte) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if to == p.self {
		return p.fetchFiles(ctx, from, id, lo, hi)
	}
	b, _ := json.Marshal(streamRequest{Table: uint64(id), Lo: lo, Hi: hi, From: from})
	out, err := p.m.Request(ctx, to, cluster.VerbStreamPush, b)
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(string(out), 10, 64)
	return n, nil
}

// ---- progress of resumable operations ----

const streamProgressFile = "stream-progress.json"

// streamProgress records the ranges an operation (bootstrap, decommission,
// removenode of a host) has finished.
type streamProgress struct {
	d  *DB
	mu sync.Mutex
	m  map[string]map[string]bool
}

func (d *DB) progress() *streamProgress {
	d.progressOnce.Do(func() {
		sp := &streamProgress{d: d, m: map[string]map[string]bool{}}
		if b, err := d.readFile(streamProgressFile); err == nil && len(b) > 0 {
			_ = json.Unmarshal(b, &sp.m)
		}
		d.prog = sp
	})
	return d.prog
}

func progressUnit(id lsm.TableID, lo, hi []byte) string {
	return fmt.Sprintf("%d/%x/%x", id, lo, hi)
}

func (sp *streamProgress) done(op, unit string) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.m[op][unit]
}

func (sp *streamProgress) mark(op, unit string) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.m[op] == nil {
		sp.m[op] = map[string]bool{}
	}
	sp.m[op][unit] = true
	return sp.saveLocked()
}

func (sp *streamProgress) clear(op string) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if _, ok := sp.m[op]; !ok {
		return nil
	}
	delete(sp.m, op)
	return sp.saveLocked()
}

func (sp *streamProgress) saveLocked() error {
	b, _ := json.Marshal(sp.m)
	return sp.d.writeFileAtomic(streamProgressFile, b)
}
