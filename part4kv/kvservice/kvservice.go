package kvservice

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/eliben/raft/part3/raft"
)

type KVService struct {
	sync.Mutex

	id         int
	rs         *raft.Server
	commitChan chan raft.CommitEntry

	ds *DataStore

	// commitSubscribers is a list of channels that want to be notified of
	// any committed entries in the Raft log. Edit this list only via the
	// createCommitSubsciption and removeCommitSubscription methods.
	commitSubscribers []chan raft.CommitEntry

	srv *http.Server
}

// New creates a new KVService
//
//   - id: this service's ID within its Raft cluster
//   - peerIds: the IDs of the other Raft peers in the cluster
//   - readyChan: notification channel that has to be closed when the Raft
//     cluster is ready (all peers are up and connected to each other).
func New(id int, peerIds []int, readyChan <-chan any) *KVService {
	commitChan := make(chan raft.CommitEntry)

	// raft.Server handles the Raft RPCs in the cluster; after Serve is called,
	// it's ready to accept RPC connections from peers.
	rs := raft.NewServer(id, peerIds, raft.NewMapStorage(), readyChan, commitChan)
	rs.Serve()
	kvs := &KVService{
		id:         id,
		rs:         rs,
		commitChan: commitChan,
		ds:         NewDataStore(),
	}

	kvs.runUpdater()
	return kvs
}

func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}

// ServeHTTP starts serving the KV REST API on the given TCP port. This
// function does not block; it fires up the HTTP server and returns. To properly
// shut down the server, call the Shutdown method.
func (kvs *KVService) ServeHTTP(port string) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)

	kvs.srv = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
}

// Shutdown performs a proper shutdown of the service; it disconnects from
// all Raft peers, shuts down the Raft RPC server, and shuts down the main
// HTTP service. It only returns once shutdown is complete.
func (kvs *KVService) Shutdown() error {
	kvs.rs.DisconnectAll()
	kvs.rs.Shutdown()
	close(kvs.commitChan)
	return kvs.srv.Shutdown(context.Background())
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {

}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	type putRequest struct {
		Key   string
		Value string
	}
	pr := &putRequest{}
	err := readRequestJSON(req, pr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create a command and submit it, but first create a subscription for
	// new commits - to avoid potential race conditions.
	sub := kvs.createCommitSubsciption()
	defer kvs.removeCommitSubscription(sub)

	cmd := Command{
		kind:  CommandPut,
		key:   pr.Key,
		value: pr.Value,
		id:    kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)

	// If we're not the Raft leader, send an appropriate status
	// TODO
}

// runUpdater runs the "updater" goroutine that reads the commit channel
// from Raft and updates the data store; this is the Replicated State Machine
// part of distributed consensus!
func (kvs *KVService) runUpdater() {
	go func() {
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			switch cmd.kind {
			case CommandGet:
			case CommandPut:
				kvs.ds.Put(cmd.key, cmd.value)
			default:
				panic(fmt.Errorf("unexpected command %v", cmd))
			}

			kvs.Lock()
			for _, sub := range kvs.commitSubscribers {
				sub <- entry
			}
			kvs.Unlock()
		}
	}()
}

func (kvs *KVService) createCommitSubsciption() chan raft.CommitEntry {
	kvs.Lock()
	defer kvs.Unlock()

	ch := make(chan raft.CommitEntry, 1)
	kvs.commitSubscribers = append(kvs.commitSubscribers, ch)
	return ch
}

func (kvs *KVService) removeCommitSubscription(ch chan raft.CommitEntry) {
	kvs.Lock()
	defer kvs.Unlock()

	kvs.commitSubscribers = slices.DeleteFunc(kvs.commitSubscribers, func(c chan raft.CommitEntry) bool {
		return c == ch
	})
}
