package server

import (
	"container/list"
	"sync"

	"github.com/cassandra-room/cassandra-room/internal/db"
)

const numLRUShards = 64

type lruEntry[V any] struct {
	key   string
	value V
}

type lruShard[V any] struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List
	items    map[string]*list.Element
}

// lruCache is a sharded, thread-safe bounded LRU cache used for prepared statements
// so memory usage stays strictly bounded and hot-path lookups
// do not contend on a single global mutex across connections.
type lruCache[V any] struct {
	shards [numLRUShards]lruShard[V]
}

func newLRUCache[V any](capacity int) *lruCache[V] {
	if capacity <= 0 {
		capacity = 1024
	}
	perShard := (capacity + numLRUShards - 1) / numLRUShards
	if perShard < 1 {
		perShard = 1
	}
	c := &lruCache[V]{}
	for i := range c.shards {
		c.shards[i] = lruShard[V]{
			capacity: perShard,
			ll:       list.New(),
			items:    make(map[string]*list.Element, perShard),
		}
	}
	return c
}

// statement returns the statement for a query in a keyspace, preparing it
// on a cache miss.
func (s *Server) statement(keyspace, query string) (*db.Prepared, error) {
	id := db.StatementID(keyspace, query)
	if p, ok := s.statements.Get(string(id)); ok && p.Query == query && p.Keyspace == keyspace {
		return p, nil
	}
	p, err := s.db.Prepare(keyspace, query)
	if err != nil {
		return nil, err
	}
	s.statements.Put(string(id), p)
	return p, nil
}

func (s *Server) lookupPrepared(id []byte) (*db.Prepared, bool) {
	return s.statements.Get(string(id))
}

func (c *lruCache[V]) Get(key string) (V, bool) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.items[key]; ok {
		s.ll.MoveToFront(elem)
		return elem.Value.(*lruEntry[V]).value, true
	}
	var zero V
	return zero, false
}

func (c *lruCache[V]) Put(key string, value V) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.items[key]; ok {
		s.ll.MoveToFront(elem)
		elem.Value.(*lruEntry[V]).value = value
		return
	}

	if s.ll.Len() >= s.capacity {
		oldest := s.ll.Back()
		if oldest != nil {
			s.ll.Remove(oldest)
			delete(s.items, oldest.Value.(*lruEntry[V]).key)
		}
	}

	elem := s.ll.PushFront(&lruEntry[V]{key: key, value: value})
	s.items[key] = elem
}

func (c *lruCache[V]) shardFor(key string) *lruShard[V] {
	return &c.shards[fnv64a(key)&(numLRUShards-1)]
}
