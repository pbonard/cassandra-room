package lsm

import (
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

// walWriter appends batches to one WAL segment. It uses Pebble's
// record.LogWriter, which buffers records and writes them from a background
// goroutine, so a write costs a memory copy, and concurrent synced writes
// share one fsync (group commit).
type walWriter struct {
	num uint64
	w   *record.LogWriter
	// syncSem bounds the number of queued syncs, as LogWriter requires.
	syncSem chan struct{}
}

// createWAL creates WAL segment num. If recycle is non-zero, the obsolete
// segment with that number is renamed and overwritten instead of creating a
// new file, as Pebble does: overwriting allocated blocks avoids growing the
// file, and on a real disk makes each fsync cheaper (no block allocation or
// file size update to journal). Records carry their segment number, so on
// replay the old segment's records left past the new end are recognised as
// stale and end the replay; recycle must be lower than num.
func createWAL(fs vfs.FS, dir string, num, recycle uint64) (*walWriter, error) {
	path := fs.PathJoin(dir, walFileName(num))
	var f vfs.File
	var err error
	if recycle != 0 {
		if recycle >= num {
			return nil, fmt.Errorf("lsm: recycling WAL %d as %d", recycle, num)
		}
		f, err = fs.ReuseForWrite(fs.PathJoin(dir, walFileName(recycle)), path, vfs.WriteCategoryUnspecified)
	} else {
		f, err = fs.Create(path, vfs.WriteCategoryUnspecified)
	}
	if err != nil {
		return nil, err
	}
	sem := make(chan struct{}, record.SyncConcurrency-1)
	w := newLogWriter(f, num, record.LogWriterConfig{
		QueueSemChan:        sem,
		WriteWALSyncOffsets: func() bool { return false },
	})
	return &walWriter{num: num, w: w, syncSem: sem}, nil
}

// pendingSync is a queued sync; wait blocks until the record is durable.
type pendingSync struct {
	wg  sync.WaitGroup
	err error
}

func (p *pendingSync) wait() error {
	p.wg.Wait()
	return p.err
}

// add queues one batch. With ps == nil it returns once the record is in the
// writer's buffer; otherwise the record is also queued for sync and ps.wait
// blocks until it is durable, so the caller can release its locks first.
func (w *walWriter) add(data []byte, ps *pendingSync) error {
	if ps == nil {
		_, err := w.w.WriteRecord(data)
		return err
	}
	w.syncSem <- struct{}{} // released by the LogWriter when the sync completes
	ps.err = nil
	ps.wg.Add(1)
	_, err := w.w.SyncRecord(data, &ps.wg, &ps.err)
	return err
}

// size returns the bytes written to the segment so far.
func (w *walWriter) size() int64 { return w.w.Size() }

// close flushes, syncs and closes the segment.
func (w *walWriter) close() error { return w.w.Close() }

// replayWAL calls fn for every complete record in WAL segment num. A torn or
// zeroed tail (from a crash during a write that was never acknowledged as
// synced) ends the replay without error.
func replayWAL(fs vfs.FS, path string, num uint64, fn func(data []byte) error) error {
	f, err := fs.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := newLogReader(f, num)
	for {
		rr, err := r.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lsm: reading WAL %s: %w", path, err)
		}
		data, err := io.ReadAll(rr)
		if record.IsInvalidRecord(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lsm: reading WAL %s: %w", path, err)
		}
		if err := fn(data); err != nil {
			return err
		}
	}
}
