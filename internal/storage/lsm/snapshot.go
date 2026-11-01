package lsm

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/vfs"
)

// Snapshot is a consistent, read-only view of a table at one point in time
// (section 7): the table's read state, whose sstables stay open and on disk
// while the snapshot lives, and the sequence number that hides later writes
// in its memtables. A Snapshot is safe for concurrent use. It must be
// closed.
type Snapshot struct {
	t      *Table
	s      *readState
	seq    uint64
	closed bool
}

// NewSnapshot returns a snapshot of the table's current contents.
func (t *Table) NewSnapshot() (*Snapshot, error) {
	if t.dropped.Load() {
		return nil, ErrUnknownTable
	}
	// Load the state before the sequence number, as Get does.
	s := t.acquireState()
	return &Snapshot{t: t, s: s, seq: t.e.visibleSeq.Load()}, nil
}

// Get returns the row as of the snapshot (see Table.Get).
func (sn *Snapshot) Get(key []byte) ([]byte, error) {
	return sn.t.getFromState(sn.s, sn.seq, key)
}

// NewIter returns an iterator over the snapshot's rows in [lower, upper).
// The iterator holds its own reference: it may outlive the snapshot.
func (sn *Snapshot) NewIter(lower, upper []byte) (*Iterator, error) {
	sn.s.refs.Add(1)
	return sn.t.newIterFromState(sn.s, sn.seq, lower, upper)
}

// Close releases the snapshot.
func (sn *Snapshot) Close() error {
	if sn.closed {
		return errors.New("lsm: snapshot already closed")
	}
	sn.closed = true
	sn.s.release()
	return nil
}

// snapshotsDirName holds on-disk snapshots: snapshots/<name>/<table dir>/.
const snapshotsDirName = "snapshots"

// snapshotListName lists the sstables of an on-disk snapshot, one file name
// per line; it is written last, so a snapshot without it is incomplete.
const snapshotListName = "SSTABLES"

// SnapshotToDisk flushes the table and hard-links its sstables into
// <engine dir>/snapshots/<name>/<table>/, as Cassandra's nodetool snapshot
// does. sstables are immutable, so the links share their space until the
// live table compacts them away. It returns the snapshot's directory. The
// files can be restored into any table with Import.
func (t *Table) SnapshotToDisk(name string) (string, error) {
	if name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return "", fmt.Errorf("lsm: invalid snapshot name %q", name)
	}
	if err := t.Flush(); err != nil {
		return "", err
	}
	e := t.e
	s := t.acquireState()
	defer s.release()
	root := e.fs.PathJoin(e.dir, snapshotsDirName, name)
	dir := e.fs.PathJoin(root, t.id.dirName())
	if _, err := e.fs.Stat(dir); err == nil {
		return "", fmt.Errorf("lsm: snapshot %q of table %d exists", name, t.id)
	}
	if err := e.fs.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	var list bytes.Buffer
	files := slices.Clone(s.files)
	slices.SortFunc(files, func(a, b *fileMeta) int { return bytes.Compare(a.smallest.UserKey, b.smallest.UserKey) })
	for _, f := range files {
		base := tableFileName(f.num)
		if err := e.fs.Link(f.path, e.fs.PathJoin(dir, base)); err != nil {
			return "", err
		}
		fmt.Fprintln(&list, base)
	}
	if err := syncDir(e.fs, dir); err != nil {
		return "", err
	}
	if err := writeFileSync(e.fs, e.fs.PathJoin(dir, snapshotListName), list.Bytes()); err != nil {
		return "", err
	}
	for _, d := range []string{dir, root, e.fs.PathJoin(e.dir, snapshotsDirName), e.dir} {
		if err := syncDir(e.fs, d); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// SnapshotFiles returns the sstable paths of a complete on-disk snapshot of
// the table, as written by SnapshotToDisk.
func (t *Table) SnapshotFiles(name string) ([]string, error) {
	e := t.e
	dir := e.fs.PathJoin(e.dir, snapshotsDirName, name, t.id.dirName())
	f, err := e.fs.Open(e.fs.PathJoin(dir, snapshotListName))
	if err != nil {
		return nil, fmt.Errorf("lsm: snapshot %q of table %d: %w", name, t.id, err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			out = append(out, e.fs.PathJoin(dir, line))
		}
	}
	return out, sc.Err()
}

// ClearSnapshot deletes an on-disk snapshot of every table.
func (e *Engine) ClearSnapshot(name string) error {
	if name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return fmt.Errorf("lsm: invalid snapshot name %q", name)
	}
	return e.fs.RemoveAll(e.fs.PathJoin(e.dir, snapshotsDirName, name))
}

func writeFileSync(fs vfs.FS, path string, data []byte) error {
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
