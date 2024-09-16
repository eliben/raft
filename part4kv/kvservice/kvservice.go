package kvservice

import (
	"net"

	"github.com/eliben/raft/part3/raft"
)

type KVService struct {
	id         int
	rs         *raft.Server
	commitChan chan raft.CommitEntry
}

func New(id int, peerIds []int, readyChan <-chan any) *KVService {
	storage := raft.NewMapStorage()
	commitChan := make(chan raft.CommitEntry)

	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
	return &KVService{
		id:         id,
		rs:         rs,
		commitChan: commitChan,
	}
}

func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}
