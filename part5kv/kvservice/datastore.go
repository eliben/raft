// Basic in-memory datastore backing the KV service.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package kvservice

import "sync"

// DataStore is a simple, concurrency-safe key-value store used as a backend
// for kvservice.
type DataStore struct {
	sync.Mutex
	data map[string]string
}

func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[string]string),
	}
}

// Get fetches the value of key from the datastore and returns (v, true) if
// it was found or ("", false) otherwise.
func (ds *DataStore) Get(key string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	value, ok := ds.data[key]
	return value, ok
}

// Put assigns datastore[key]=value, and returns (v, true) if the key was
// previously in the store and its value was v, or ("", false) otherwise.
func (ds *DataStore) Put(key, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	v, ok := ds.data[key]
	ds.data[key] = value
	return v, ok
}

// CAS performs an atomic compare-and-swap:
// if key exists and its prev value == compare, write value, else nop
// The prev value and whether the key existed in the store is returned.
func (ds *DataStore) CAS(key, compare, value string) (string, bool) {
	ds.Lock()
	defer ds.Unlock()

	prevValue, ok := ds.data[key]
	if ok && prevValue == compare {
		ds.data[key] = value
	}
	return prevValue, ok
}
