// Package store is a sharded in-memory key-value store.
package store

import (
	"hash/fnv"
	"sync"
)

const shardCount = 16

type shard struct {
	mu sync.RWMutex
	m  map[string]string
}

// Store is a key-value map split into shards to reduce lock contention.
type Store struct {
	shards [shardCount]*shard
}

// New returns an empty store.
func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i] = &shard{m: make(map[string]string)}
	}
	return s
}

// shardFor picks the shard that owns key.
func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[h.Sum32()%shardCount]
}

// Get returns the value for key and whether it was present.
func (s *Store) Get(key string) (string, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	v, ok := sh.m[key]
	return v, ok
}

// Set stores value under key.
func (s *Store) Set(key, value string) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.m[key] = value
}

// Del removes key and reports whether it existed.
func (s *Store) Del(key string) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, ok := sh.m[key]
	delete(sh.m, key)
	return ok
}
