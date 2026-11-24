package accord

import (
	"errors"
	"sync"
)

// Record is one journal record: a command's encoded state, or (no Data) the
// command forgotten.
type Record struct {
	ID   TxnID
	Data []byte
}

// BatchJournal is a Journal that can make several records durable at once
// (with one sync): the node then commits its journal writes in groups
// (AccordJournal's group commit), outside its store lock.
type BatchJournal interface {
	Journal
	AppendBatch(recs []Record) error
}

// groupCommit queues records and writes them in batches from one goroutine,
// in order; a writer waits for its sequence number to be durable.
type groupCommit struct {
	j       BatchJournal
	onError func()
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []Record
	next    uint64 // last sequence number handed out
	durable uint64 // last sequence number written
	failed  uint64 // last sequence number of a failed batch
	err     error
	closed  bool
	done    chan struct{}
}

func newGroupCommit(j BatchJournal, onError func()) *groupCommit {
	g := &groupCommit{j: j, onError: onError, done: make(chan struct{})}
	g.cond = sync.NewCond(&g.mu)
	go g.run()
	return g
}

func (g *groupCommit) enqueue(rec Record) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.queue = append(g.queue, rec)
	g.next++
	g.cond.Broadcast()
	return g.next
}

func (g *groupCommit) run() {
	defer close(g.done)
	for {
		g.mu.Lock()
		for len(g.queue) == 0 && !g.closed {
			g.cond.Wait()
		}
		if len(g.queue) == 0 {
			g.mu.Unlock()
			return
		}
		batch, upto := g.queue, g.next
		g.queue = nil
		g.mu.Unlock()
		err := g.j.AppendBatch(batch)
		g.mu.Lock()
		g.durable = upto
		if err != nil {
			g.failed, g.err = upto, err
			if g.onError != nil {
				g.onError()
			}
		}
		g.cond.Broadcast()
		g.mu.Unlock()
	}
}

// wait waits until the records up to seq are durable; it fails if their
// batch (or a later one) failed.
func (g *groupCommit) wait(seq uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.durable < seq && !g.closed {
		g.cond.Wait()
	}
	if g.durable < seq {
		return errStopped
	}
	if g.err != nil && seq <= g.failed {
		return g.err
	}
	return nil
}

func (g *groupCommit) close() {
	g.mu.Lock()
	g.closed = true
	g.cond.Broadcast()
	g.mu.Unlock()
	<-g.done
}

var errJournal = errors.New("accord: journal write failed")
