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

type Store struct {
	shards [shardCount]*shard
}

func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i] = &shard{m: make(map[string]string)}
	}
	return s
}

func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[h.Sum32()%shardCount]
}

func (s *Store) Get(key string) (string, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	v, ok := sh.m[key]
	return v, ok
}

func (s *Store) Set(key, value string) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.m[key] = value
}

func (s *Store) Del(key string) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, ok := sh.m[key]
	delete(sh.m, key)
	return ok
}
