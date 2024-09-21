package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/eliben/raft/part3/raft"
	"github.com/eliben/raft/part4kv/api"
)

const DebugKV = 1

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
	gob.Register(Command{})
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

func (kvs *KVService) DisconnectFromRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}

// IsLeader checks if kvs thinks it's the leader in the Raft cluster. Only
// use this for testin and debugging.
func (kvs *KVService) IsLeader() bool {
	return kvs.rs.IsLeader()
}

// ServeHTTP starts serving the KV REST API on the given TCP port. This
// function does not block; it fires up the HTTP server and returns. To properly
// shut down the server, call the Shutdown method.
func (kvs *KVService) ServeHTTP(port int) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)

	kvs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		kvs.kvlog("serving HTTP on %s", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

// Shutdown performs a proper shutdown of the service: shuts down the Raft RPC
// server, and shuts down the main HTTP service. It only returns once shutdown
// is complete.
// Note: DisconnectFromRaftPeers on all peers in the cluster should be done
// before Shutdown is called.
func (kvs *KVService) Shutdown() error {
	kvs.kvlog("shutting down Raft server")
	kvs.rs.Shutdown()
	close(kvs.commitChan)

	if kvs.srv != nil {
		kvs.kvlog("shutting down HTTP server")
		return kvs.srv.Shutdown(context.Background())
	}

	return nil
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {

}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	pr := &api.PutRequest{}
	err := readRequestJSON(req, pr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("received PUT %v", pr)

	// Create a command and submit it, but first create a subscription for
	// new commits - to avoid potential race conditions.
	sub := kvs.createCommitSubsciption()
	defer kvs.removeCommitSubscription(sub)

	cmd := Command{
		Kind:  CommandPut,
		Key:   pr.Key,
		Value: pr.Value,
		Id:    kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)

	// If we're not the Raft leader, send an appropriate status
	if logIndex < 0 {
		renderJSON(w, api.PutResponse{Status: api.StatusNotLeader})
		return
	}

	// We're the Raft leader, so we should respond. The command has already been
	// submitted, and now we wait for it to be committed. sub will be sent
	// all commit entries by the updater.
	for entry := range sub {
		// Wait until we see a committed command with the same logIndex we expect.
		if entry.Index != logIndex {
			continue
		}

		// If this is our command, all is good! If it's some other server's command,
		// this means we lost leadership at some point and should return an error
		// to the client.
		entryCmd := entry.Command.(Command)
		if entryCmd.Id == kvs.id {
			renderJSON(w, api.PutResponse{
				Status:    api.StatusOK,
				KeyFound:  entryCmd.ResultFound,
				PrevValue: entryCmd.ResultValue,
			})
		} else {
			renderJSON(w, api.PutResponse{Status: api.StatusFailedCommit})
		}
		return
	}
}

// runUpdater runs the "updater" goroutine that reads the commit channel
// from Raft and updates the data store; this is the Replicated State Machine
// part of distributed consensus!
// It also notifies subscribers (registered with createCommitSubsciption) of
// each commit entry.
func (kvs *KVService) runUpdater() {
	go func() {
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			switch cmd.Kind {
			case CommandGet:
			case CommandPut:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
			default:
				panic(fmt.Errorf("unexpected command %v", cmd))
			}

			// We're modifying the command to include results from the datastore,
			// so clone an entry with the update command for the subscibers.
			newEntry := raft.CommitEntry{
				Command: cmd,
				Index:   entry.Index,
				Term:    entry.Term,
			}

			// Forward this entry to all current subscribers.
			kvs.Lock()
			for _, sub := range kvs.commitSubscribers {
				sub <- newEntry
			}
			kvs.Unlock()
		}
	}()
}

// createCommitSubsciption creates a "commit subscription", a new channel that
// will get sent all CommitEntry values by the updater. This is a buffered
// channel, and it should be read as fast as possible. To remove the channel
// from the subscription list, call removeCommitSubscription (that function
// also closes the channel).
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

	// Note: the list's size is O(concurrent REST requests waiting for responses).
	// This number is expected to be very low almost all the time, so we don't
	// worry about the performance of deleting from a slice; it can easily be
	// replaced by some sort of set data structure, if needed.
	kvs.commitSubscribers = slices.DeleteFunc(kvs.commitSubscribers, func(c chan raft.CommitEntry) bool {
		return c == ch
	})
	close(ch)
}

// kvlog logs a debugging message if DebugKV > 0
func (kvs *KVService) kvlog(format string, args ...any) {
	if DebugKV > 0 {
		format = fmt.Sprintf("[kv %d] ", kvs.id) + format
		log.Printf(format, args...)
	}
}
