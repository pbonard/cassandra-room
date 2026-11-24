package accord

import (
	"encoding/json"
	"sync"
	"time"
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

// batchAdapter makes any Journal a BatchJournal (one write per record), so
// every node commits through groupCommit.
type batchAdapter struct{ Journal }

func (a batchAdapter) AppendBatch(recs []Record) error {
	for _, r := range recs {
		if r.Data == nil {
			if err := a.Delete(r.ID); err != nil {
				return err
			}
			continue
		}
		var c Command
		if err := json.Unmarshal(r.Data, &c); err != nil {
			return err
		}
		if err := a.Append(&c); err != nil {
			return err
		}
	}
	return nil
}

// groupCommit queues records and writes them in batches from one goroutine,
// in order; a writer waits for its sequence number to be durable.
//
// A failed batch is retried until it is written: the durable position never
// moves past it, so nothing queued after it is reported durable either, and
// the node stalls instead of answering with state the journal lacks (the
// store already holds that state in memory, and later requests read it).
type groupCommit struct {
	j       BatchJournal
	onError func()
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []Record
	next    uint64 // last sequence number handed out
	durable uint64 // last sequence number written
	closed  bool
	stop    chan struct{}
	done    chan struct{}
}

func newGroupCommit(j BatchJournal, onError func()) *groupCommit {
	g := &groupCommit{j: j, onError: onError, stop: make(chan struct{}), done: make(chan struct{})}
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
		backoff := 10 * time.Millisecond
		for {
			err := g.j.AppendBatch(batch)
			if err == nil {
				break
			}
			if g.onError != nil {
				g.onError()
			}
			select {
			case <-g.stop:
				return // never written: its waiters get errStopped
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, time.Second)
		}
		g.mu.Lock()
		g.durable = upto
		g.cond.Broadcast()
		g.mu.Unlock()
	}
}

// wait waits until the records up to seq are durable.
func (g *groupCommit) wait(seq uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.durable < seq && !g.closed {
		g.cond.Wait()
	}
	if g.durable < seq {
		return errStopped
	}
	return nil
}

// isDurable reports whether the records up to seq are written.
func (g *groupCommit) isDurable(seq uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.durable >= seq
}

// close writes what is queued (unless the journal keeps failing) and stops.
func (g *groupCommit) close() {
	g.mu.Lock()
	g.closed = true
	g.cond.Broadcast()
	g.mu.Unlock()
	select {
	case <-g.done:
	case <-time.After(time.Second):
		close(g.stop)
		<-g.done
	}
}
