package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/eliben/raft/part4kv/kvclient"
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

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	// ctx is context used for the HTTP client commands used by tests.
	// ctxCancel is its cancellation function.
	ctx       context.Context
	ctxCancel func()
}

func NewHarness(t *testing.T, n int) *Harness {
	kvss := make([]*kvservice.KVService, n)
	ready := make(chan any)
	connected := make([]bool, n)

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
		connected[i] = true
	}
	close(ready)

	// Each KVService instance serves a REST API on a different port
	kvServiceAddrs := make([]string, n)
	for i := range n {
		port := 14200 + i
		kvss[i].ServeHTTP(port)

		kvServiceAddrs[i] = fmt.Sprintf("localhost:%d", port)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	h := &Harness{
		n:              n,
		kvCluster:      kvss,
		kvServiceAddrs: kvServiceAddrs,
		t:              t,
		connected:      connected,
		ctx:            ctx,
		ctxCancel:      ctxCancel,
	}
	return h
}

func (h *Harness) DisconnectServiceFromPeers(id int) {
	tlog("Disconnect %d", id)
	h.kvCluster[id].DisconnectFromAllRaftPeers()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.kvCluster[j].DisconnectFromRaftPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) ReconnectServiceToPeers(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.kvCluster[id].ConnectToRaftPeer(j, h.kvCluster[j].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.kvCluster[j].ConnectToRaftPeer(id, h.kvCluster[id].GetRaftListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true

}

func (h *Harness) Shutdown() {
	for i := range h.n {
		h.kvCluster[i].DisconnectFromAllRaftPeers()
		h.connected[i] = false
	}

	// These help the HTTP server in KVService shut down properly.
	http.DefaultClient.CloseIdleConnections()
	h.ctxCancel()

	for i := range h.n {
		if err := h.kvCluster[i].Shutdown(); err != nil {
			h.t.Errorf("error while shutting down service %d: %v", i, err)
		}
	}
}

func (h *Harness) NewClient() *kvclient.KVClient {
	return kvclient.New(h.kvServiceAddrs)
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id in the Raft cluster. It retries serveral times if
// no leader is identified yet, so this method is also useful to check that
// the Raft cluster settled on a leader and is ready to execute commands.
func (h *Harness) CheckSingleLeader() int {
	for r := 0; r < 8; r++ {
		leaderId := -1
		for i := range h.n {
			if h.connected[i] && h.kvCluster[i].IsLeader() {
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
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1
}

// CheckPut sends a Put request through client c, and checks there are no
// errors. Returns (prevValue, keyFound).
func (h *Harness) CheckPut(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, _ := context.WithTimeout(h.ctx, 500*time.Millisecond)
	//defer cancel()
	pv, f, err := c.Put(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

// CheckGet sends a Get request through client c, and checks there are
// no errors; it also checks that the key was found, and has the expected
// value.
func (h *Harness) CheckGet(c *kvclient.KVClient, key string, wantValue string) {
	ctx, _ := context.WithTimeout(h.ctx, 500*time.Millisecond)
	//defer cancel()
	gv, f, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}
	if !f {
		h.t.Errorf("got found=false, want true for key=%s", key)
	}
	if gv != wantValue {
		h.t.Errorf("got value=%v, want %v", gv, wantValue)
	}
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
