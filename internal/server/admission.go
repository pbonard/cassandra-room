package server

import (
	"container/list"
	"context"
	"sync"
)

type waiter struct {
	n     int64
	ready chan struct{}
}

// admissionController enforces resizable global concurrency and in-flight byte budgets
// using FIFO waiter queues (modeled after golang.org/x/sync/semaphore.Weighted) so
// releasing resources wakes only the next eligible waiter(s) without a thundering herd.
type admissionController struct {
	mu             sync.Mutex
	maxRequests    int
	activeRequests int
	reqWaiters     list.List

	maxBytes    int64
	activeBytes int64
	byteWaiters list.List
}

func newAdmissionController(maxRequests int, maxBytes int64) *admissionController {
	return &admissionController{
		maxRequests: maxRequests,
		maxBytes:    maxBytes,
	}
}

func (a *admissionController) setMaxRequests(limit int) {
	if limit <= 0 {
		limit = 1
	}
	a.mu.Lock()
	a.maxRequests = limit
	a.wakeReqWaitersLocked()
	a.mu.Unlock()
}

func (a *admissionController) setMaxBytes(limit int64) {
	if limit <= 0 {
		limit = 1
	}
	a.mu.Lock()
	a.maxBytes = limit
	a.wakeByteWaitersLocked()
	a.mu.Unlock()
}

func (a *admissionController) acquireRequest(ctx context.Context, throwOnOverload bool) (overloaded bool, err error) {
	a.mu.Lock()
	if a.reqWaiters.Len() == 0 && a.activeRequests < a.maxRequests {
		a.activeRequests++
		a.mu.Unlock()
		return false, nil
	}
	if throwOnOverload {
		a.mu.Unlock()
		return true, nil
	}

	w := &waiter{n: 1, ready: make(chan struct{})}
	elem := a.reqWaiters.PushBack(w)
	a.mu.Unlock()

	select {
	case <-w.ready:
		return false, nil
	case <-ctx.Done():
		a.mu.Lock()
		select {
		case <-w.ready:
			// We were already granted the slot right as ctx expired; release it.
			a.activeRequests--
			a.wakeReqWaitersLocked()
		default:
			a.reqWaiters.Remove(elem)
		}
		a.mu.Unlock()
		return false, ctx.Err()
	}
}

func (a *admissionController) releaseRequest() {
	a.mu.Lock()
	a.activeRequests--
	if a.activeRequests < 0 {
		a.activeRequests = 0
	}
	a.wakeReqWaitersLocked()
	a.mu.Unlock()
}

func (a *admissionController) wakeReqWaitersLocked() {
	for a.reqWaiters.Len() > 0 && a.activeRequests < a.maxRequests {
		front := a.reqWaiters.Front()
		w := a.reqWaiters.Remove(front).(*waiter)
		a.activeRequests++
		close(w.ready)
	}
}

func (a *admissionController) acquireBytes(ctx context.Context, n int64, throwOnOverload bool) (overloaded bool, err error) {
	if n <= 0 {
		return false, nil
	}
	a.mu.Lock()
	canFit := a.activeBytes+n <= a.maxBytes || (!throwOnOverload && a.activeBytes == 0)
	if a.byteWaiters.Len() == 0 && canFit {
		a.activeBytes += n
		a.mu.Unlock()
		return false, nil
	}
	if throwOnOverload {
		a.mu.Unlock()
		return true, nil
	}

	w := &waiter{n: n, ready: make(chan struct{})}
	elem := a.byteWaiters.PushBack(w)
	a.mu.Unlock()

	select {
	case <-w.ready:
		return false, nil
	case <-ctx.Done():
		a.mu.Lock()
		select {
		case <-w.ready:
			a.activeBytes -= n
			if a.activeBytes < 0 {
				a.activeBytes = 0
			}
			a.wakeByteWaitersLocked()
		default:
			a.byteWaiters.Remove(elem)
			// If we were at the front, removing ourselves may unblock smaller waiters behind us.
			a.wakeByteWaitersLocked()
		}
		a.mu.Unlock()
		return false, ctx.Err()
	}
}

func (a *admissionController) releaseBytes(n int64) {
	if n <= 0 {
		return
	}
	a.mu.Lock()
	a.activeBytes -= n
	if a.activeBytes < 0 {
		a.activeBytes = 0
	}
	a.wakeByteWaitersLocked()
	a.mu.Unlock()
}

func (a *admissionController) wakeByteWaitersLocked() {
	for a.byteWaiters.Len() > 0 {
		front := a.byteWaiters.Front()
		w := front.Value.(*waiter)
		if a.activeBytes+w.n > a.maxBytes && a.activeBytes > 0 {
			break
		}
		a.byteWaiters.Remove(front)
		a.activeBytes += w.n
		close(w.ready)
	}
}
