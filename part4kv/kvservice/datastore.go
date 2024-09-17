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

func (ds *DataStore) Get(key string) string {
	ds.Lock()
	defer ds.Unlock()

	return ds.data[key]
}

func (ds *DataStore) Put(key, value string) {
	ds.Lock()
	defer ds.Unlock()

	ds.data[key] = value
}
