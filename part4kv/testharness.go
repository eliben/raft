package main

import (
	"fmt"
	"log"
	"testing"
	"time"

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

	// kvServiceAddrs is a list of HTTP addresses (localhost:<PORT>) the KV
	// services are accepting client commands on.
	kvServiceAddrs []string

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
	kvServiceAddrs := make([]string, n)
	for i := range n {
		port := 14200 + i
		kvss[i].ServeHTTP(port)

		kvServiceAddrs[i] = fmt.Sprintf("localhost:%d", port)
	}

	h := &Harness{
		n:              n,
		kvCluster:      kvss,
		kvServiceAddrs: kvServiceAddrs,
		t:              t,
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

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id in the Raft cluster. It retries serveral times if
// no leader is identified yet.
func (h *Harness) CheckSingleLeader() int {
	for r := 0; r < 8; r++ {
		leaderId := -1
		for i := range h.n {
			if h.kvCluster[i].IsLeader() {
				if leaderId < 0 {
					leaderId = i
				} else {
					h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
				}
			}
		}
		if leaderId >= 0 {
			return leaderId
		}
		time.Sleep(50 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1
}
