package accord

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Journal makes a node's command state durable (Cassandra's AccordJournal):
// every change is appended before the node answers the request that caused
// it, and replayed when the node restarts.
type Journal interface {
	// Append records a command's state, replacing earlier records of it.
	Append(c *Command) error
	// Load returns the latest record of every command.
	Load() ([]*Command, error)
	// Compact rewrites the journal with the given commands only.
	Compact(cs []*Command) error
	// Delete forgets a command.
	Delete(id TxnID) error
}

// MemJournal is an in-memory journal (for tests: it survives a node being
// recreated from it).
type MemJournal struct {
	mu   sync.Mutex
	cmds map[TxnID][]byte
	n    int
}

func (j *MemJournal) Append(c *Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cmds == nil {
		j.cmds = map[TxnID][]byte{}
	}
	j.cmds[c.ID] = b
	j.n++
	return nil
}

func (j *MemJournal) Delete(id TxnID) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.cmds, id)
	return nil
}

func (j *MemJournal) Load() ([]*Command, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*Command
	for _, b := range j.cmds {
		var c Command
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, nil
}

func (j *MemJournal) Compact(cs []*Command) error {
	j.mu.Lock()
	j.cmds = nil
	j.mu.Unlock()
	for _, c := range cs {
		if err := j.Append(c); err != nil {
			return err
		}
	}
	return nil
}

// FileJournal is an append-only file of JSON records, synced after every
// append, rewritten by Compact.
type FileJournal struct {
	mu   sync.Mutex
	path string
	f    *os.File
	// Sync syncs every append (default true via NewFileJournal).
	Sync bool
}

// NewFileJournal opens (or creates) a journal file.
func NewFileJournal(path string) (*FileJournal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileJournal{path: path, f: f, Sync: true}, nil
}

func (j *FileJournal) Append(c *Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Write(b); err != nil {
		return err
	}
	if j.Sync {
		return j.f.Sync()
	}
	return nil
}

// Delete appends a record forgetting the command (dropped at the next
// compaction).
func (j *FileJournal) Delete(id TxnID) error {
	return j.Append(&Command{ID: id, Status: forgotten})
}

func (j *FileJournal) Load() ([]*Command, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	latest := map[TxnID]*Command{}
	var order []TxnID
	r := bufio.NewReaderSize(j.f, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 && err == nil {
			var c Command
			if json.Unmarshal(line, &c) == nil {
				if latest[c.ID] == nil {
					order = append(order, c.ID)
				}
				latest[c.ID] = &c
			}
		}
		if err != nil {
			break // EOF, or a torn last record
		}
	}
	out := make([]*Command, 0, len(order))
	for _, id := range order {
		if latest[id].Status != forgotten {
			out = append(out, latest[id])
		}
	}
	return out, nil
}

func (j *FileJournal) Compact(cs []*Command) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	tmp := j.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, c := range cs {
		b, err := json.Marshal(c)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, j.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(j.path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	nf, err := os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	j.f.Close()
	j.f = nf
	return nil
}

// Close closes the journal file.
func (j *FileJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}
