// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package raft

import "sync"

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	Set(key string, value []byte)

	Get(key string) ([]byte, bool)

	// HasData returns true iff any Sets were made on this Storage.
	HasData() bool
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMapStorage() *MapStorage {
	m := make(map[string][]byte)
	return &MapStorage{
		m: m,
	}
}

func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]
	return v, found
}

func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0
}
