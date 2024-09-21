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
