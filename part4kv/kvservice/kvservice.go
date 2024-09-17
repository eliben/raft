package kvservice

import (
	"context"
	"log"
	"net"
	"net/http"

	"github.com/eliben/raft/part3/raft"
)

type KVService struct {
	id         int
	rs         *raft.Server
	commitChan chan raft.CommitEntry

	ds *DataStore

	srv *http.Server
}

func New(id int, peerIds []int, readyChan <-chan any) *KVService {
	commitChan := make(chan raft.CommitEntry)

	// raft.Server handles the Raft RPCs in the cluster; after Serve is called,
	// it's ready to accept RPC connections from peers.
	rs := raft.NewServer(id, peerIds, raft.NewMapStorage(), readyChan, commitChan)
	rs.Serve()
	return &KVService{
		id:         id,
		rs:         rs,
		commitChan: commitChan,
		ds:         NewDataStore(),
	}
}

func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}

// TODO: Serve method that serves REST on HTTP (with a port it's given)
// Requires clean shutdown with http.Server.Shutdown
// Serve doesn't block, but it gets all the processes started.
// ... a Shutdown method cleans everything up, calls DisconnectAll and Shutdown
// on the underlying rs, shuts down http server, etc.

func (kvs *KVService) ServeHTTP(port string) {
	if kvs.srv != nil {
		panic("ServeHTTP called with existing server")
	}
	mux := http.NewServeMux()

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

func (kvs *KVService) Shutdown() error {
	return kvs.srv.Shutdown(context.Background())
}
