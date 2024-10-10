// KV service based on Raft - main implementation file.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/eliben/raft/part3/raft"
	"github.com/eliben/raft/part4kv/api"
)

const DebugKV = 1

type KVService struct {
	sync.Mutex

	// id is the service ID in a Raft cluster.
	id int

	// rs is the Raft server that contains a CM
	rs *raft.Server

	// commitChan is the commit channel passed to the Raft server; when commands
	// are committed, they're sent on this channel.
	commitChan chan raft.CommitEntry

	// commitSubs are the commit subscriptions currently active in this service.
	// See the createCommitSubsciption method for more details.
	commitSubs map[int]chan raft.CommitEntry

	// ds is the underlying data store implementing the KV DB.
	ds *DataStore

	// srv is the HTTP server exposed by the service to the external world.
	srv *http.Server
}

// New creates a new KVService
//
//   - id: this service's ID within its Raft cluster
//   - peerIds: the IDs of the other Raft peers in the cluster
//   - storage: a raft.Storage implementation the service can use for
//     durable storage to persist its state.
//   - readyChan: notification channel that has to be closed when the Raft
//     cluster is ready (all peers are up and connected to each other).
func New(id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	// raft.Server handles the Raft RPCs in the cluster; after Serve is called,
	// it's ready to accept RPC connections from peers.
	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
	rs.Serve()
	kvs := &KVService{
		id:         id,
		rs:         rs,
		commitChan: commitChan,
		ds:         NewDataStore(),
		commitSubs: make(map[int]chan raft.CommitEntry),
	}

	kvs.runUpdater()
	return kvs
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
	mux.HandleFunc("POST /cas/", kvs.handleCAS)

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
	kvs.kvlog("closing commitChan")
	close(kvs.commitChan)

	if kvs.srv != nil {
		kvs.kvlog("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kvs.srv.Shutdown(ctx)
		kvs.kvlog("HTTP shutdown complete")
		return nil
	}

	return nil
}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	pr := &api.PutRequest{}
	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP PUT %v", pr)

	// Submit a command into the Raft server; this is the state change in the
	// replicated state machine built on top of the Raft log.
	cmd := Command{
		Kind:  CommandPut,
		Key:   pr.Key,
		Value: pr.Value,
		Id:    kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	// If we're not the Raft leader, send an appropriate status
	if logIndex < 0 {
		renderJSON(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subscribe for a commit update for our log index. Then wait for it to
	// be delivered.
	sub := kvs.createCommitSubsciption(logIndex)

	// Wait on the sub channel: the updater will deliver a value when the Raft
	// log has a commit at logIndex. To ensure clean shutdown of the service,
	// also select on the request context - if the request is canceled, this
	// handler aborts without sending data back to the client.
	select {
	case entry := <-sub:
		// If this is our command, all is good! If it's some other server's command,
		// this means we lost leadership at some point and should return an error
		// to the client.
		entryCmd := entry.Command.(Command)
		if entryCmd.Id == kvs.id {
			renderJSON(w, api.PutResponse{
				RespStatus: api.StatusOK,
				KeyFound:   entryCmd.ResultFound,
				PrevValue:  entryCmd.ResultValue,
			})
		} else {
			renderJSON(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {
	// The details of this handler are very similar to handleGet: refer to that
	// function for detailed comments.
	gr := &api.GetRequest{}
	if err := readRequestJSON(req, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP GET %v", gr)

	cmd := Command{
		Kind: CommandGet,
		Key:  gr.Key,
		Id:   kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	// If we're not the Raft leader, send an appropriate status
	if logIndex < 0 {
		renderJSON(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// Subsribe for a commit update for our log index. Then wait for it to
	// be delivered.
	sub := kvs.createCommitSubsciption(logIndex)

	// Wait on the sub channel: the updater will deliver a value when the Raft
	// log has a commit at logIndex. To ensure clean shutdown of the service,
	// also select on the request context - if the request is canceled, this
	// handler aborts without sending data back to the client.
	select {
	case entry := <-sub:
		// If this is our command, all is good! If it's some other server's command,
		// this means we lost leadership at some point and should return an error
		// to the client.
		entryCmd := entry.Command.(Command)
		if entryCmd.Id == kvs.id {
			renderJSON(w, api.GetResponse{
				RespStatus: api.StatusOK,
				KeyFound:   entryCmd.ResultFound,
				Value:      entryCmd.ResultValue,
			})
		} else {
			renderJSON(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, req *http.Request) {
	cr := &api.CASRequest{}
	if err := readRequestJSON(req, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP CAS %v", cr)

	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		Value:        cr.Value,
		CompareValue: cr.CompareValue,
		Id:           kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		renderJSON(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubsciption(logIndex)

	select {
	case entry := <-sub:
		entryCmd := entry.Command.(Command)
		if entryCmd.Id == kvs.id {
			renderJSON(w, api.CASResponse{
				RespStatus: api.StatusOK,
				KeyFound:   entryCmd.ResultFound,
				PrevValue:  entryCmd.ResultValue,
			})
		} else {
			renderJSON(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

// runUpdater runs the "updater" goroutine that reads the commit channel
// from Raft and updates the data store; this is the Replicated State Machine
// part of distributed consensus!
// It also notifies subscribers (registered with createCommitSubsciption).
func (kvs *KVService) runUpdater() {
	go func() {
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			switch cmd.Kind {
			case CommandGet:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
			case CommandPut:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
			case CommandCAS:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
			default:
				panic(fmt.Errorf("unexpected command %v", cmd))
			}

			// We're modifying the command to include results from the datastore,
			// so clone an entry with the update command for the subscribers.
			newEntry := raft.CommitEntry{
				Command: cmd,
				Index:   entry.Index,
				Term:    entry.Term,
			}

			// Forward this entry to the subscriber interested in its index, and
			// close the subscription - it's single-use.
			if sub := kvs.popCommitSubscription(entry.Index); sub != nil {
				sub <- newEntry
				close(sub)
			}
		}
	}()
}

// createCommitSubsciption creates a "commit subscription" for a certain log
// index. It's used by client request handlers that submit a command to the
// Raft CM. createCommitSubsciption(index) means "I want to be notified when
// an entry is committed at this index in the Raft log". The entry is delivered
// on the returend (buffered) channel by the updater goroutine, after which
// the channel is closed and the subscription is automatically canceled.
func (kvs *KVService) createCommitSubsciption(logIndex int) chan raft.CommitEntry {
	kvs.Lock()
	defer kvs.Unlock()

	if _, exists := kvs.commitSubs[logIndex]; exists {
		panic(fmt.Sprintf("duplicate commit subscription for logIndex=%d", logIndex))
	}

	ch := make(chan raft.CommitEntry, 1)
	kvs.commitSubs[logIndex] = ch
	return ch
}

func (kvs *KVService) popCommitSubscription(logIndex int) chan raft.CommitEntry {
	kvs.Lock()
	defer kvs.Unlock()

	ch := kvs.commitSubs[logIndex]
	delete(kvs.commitSubs, logIndex)
	return ch
}

// kvlog logs a debugging message if DebugKV > 0
func (kvs *KVService) kvlog(format string, args ...any) {
	if DebugKV > 0 {
		format = fmt.Sprintf("[kv %d] ", kvs.id) + format
		log.Printf(format, args...)
	}
}

// The following functions exist for testing purposes, to simulate faults.

func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) DisconnectFromAllRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) DisconnectFromRaftPeer(peerId int) error {
	return kvs.rs.DisconnectPeer(peerId)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}
