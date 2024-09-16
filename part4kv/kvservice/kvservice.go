package kvservice

import (
	"net"

	"github.com/eliben/raft/part3/raft"
)

type KVService struct {
	id         int
	rs         *raft.Server
	commitChan chan raft.CommitEntry

	ds *DataStore
}

func New(id int, peerIds []int, readyChan <-chan any) *KVService {
	storage := raft.NewMapStorage()
	commitChan := make(chan raft.CommitEntry)

	// raft.Server handles the Raft RPCs in the cluster; after Serve is called,
	// it's ready to accept RPC connections from peers.
	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
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
