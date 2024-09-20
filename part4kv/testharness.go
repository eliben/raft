package main

import (
	"log"
	"testing"

	"github.com/eliben/raft/part4kv/kvservice"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

// Test harness for kvservice and client system tests.
type Harness struct {
	n int

	// kvCluster is a list of all KVService instances participating in a cluster.
	// A service's index into this list is its ID in the cluster.
	kvCluster []*kvservice.KVService

	t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	kvss := make([]*kvservice.KVService, n)
	ready := make(chan any)

	// Create all KVService instances in this cluster.
	for i := range n {
		peerIds := make([]int, 0)
		for p := range n {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		kvss[i] = kvservice.New(i, peerIds, ready)
	}

	// Connect the Raft peers of the services to each other and close the ready
	// channel to signal to them it's all ready.
	for i := range n {
		for j := range n {
			if i != j {
				kvss[i].ConnectToRaftPeer(j, kvss[j].GetRaftListenAddr())
			}
		}
	}
	close(ready)

	// Each KVService instance serves a REST API on a different port
	for i := range n {
		port := 14200 + i
		kvss[i].ServeHTTP(port)
	}

	h := &Harness{
		n:         n,
		kvCluster: kvss,
		t:         t,
	}
	return h
}

func (h *Harness) Shutdown() {
	for i := range h.n {
		h.kvCluster[i].DisconnectFromRaftPeers()
	}

	for i := range h.n {
		if err := h.kvCluster[i].Shutdown(); err != nil {
			h.t.Errorf("error while shutting down service %d: %v", i, err)
		}
	}
}
