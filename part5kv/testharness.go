// Test harness for testing the KV service and clients.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eliben/raft/part3/raft"
	"github.com/eliben/raft/part5kv/kvclient"
	"github.com/eliben/raft/part5kv/kvservice"
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

	storage []*raft.MapStorage

	t *testing.T

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	// alive has a bool per server in the cluster, specifying whether this server
	// is currently alive (false means it has crashed and wasn't restarted yet).
	// connected implies alive.
	alive []bool

	// ctx is context used for the HTTP client commands used by tests.
	// ctxCancel is its cancellation function.
	ctx       context.Context
	ctxCancel func()
}

func NewHarness(t *testing.T, n int) *Harness {
	kvss := make([]*kvservice.KVService, n)
	ready := make(chan any)
	connected := make([]bool, n)
	alive := make([]bool, n)
	storage := make([]*raft.MapStorage, n)

	// Create all KVService instances in this cluster.
	for i := range n {
		peerIds := make([]int, 0)
		for p := range n {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = raft.NewMapStorage()
		kvss[i] = kvservice.New(i, peerIds, storage[i], ready)
		alive[i] = true
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
		alive:          alive,
		storage:        storage,
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
		if j != id && h.alive[j] {
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

// CrashService "crashes" a service by disconnecting it from all peers and
// then asking it to shut down. We're not going to be using the same service
// instance again.
func (h *Harness) CrashService(id int) {
	tlog("Crash %d", id)
	h.DisconnectServiceFromPeers(id)
	h.alive[id] = false
	if err := h.kvCluster[id].Shutdown(); err != nil {
		h.t.Errorf("error while shutting down service %d: %v", id, err)
	}
}

// RestartService "restarts" a service by creating a new instance and
// connecting it to peers.
func (h *Harness) RestartService(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartService", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := range h.n {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}
	ready := make(chan any)
	h.kvCluster[id] = kvservice.New(id, peerIds, h.storage[id], ready)
	h.kvCluster[id].ServeHTTP(14200 + id)

	h.ReconnectServiceToPeers(id)
	close(ready)
	h.alive[id] = true
	time.Sleep(20 * time.Millisecond)
}

// DelayNextHTTPResponseFromService delays the next HTTP response from this
// service to a client.
func (h *Harness) DelayNextHTTPResponseFromService(id int) {
	tlog("Delaying next HTTP response from %d", id)
	h.kvCluster[id].DelayNextHTTPResponse()
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
		if h.alive[i] {
			h.alive[i] = false
			if err := h.kvCluster[i].Shutdown(); err != nil {
				h.t.Errorf("error while shutting down service %d: %v", i, err)
			}
		}
	}
}

// NewClient creates a new client that will contact all the existing live
// services.
func (h *Harness) NewClient() *kvclient.KVClient {
	var addrs []string
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	return kvclient.New(addrs)
}

// NewClientWithRandomAddrsOrder creates a new client that will contact all
// the existing live services, but in a randomized order.
func (h *Harness) NewClientWithRandomAddrsOrder() *kvclient.KVClient {
	var addrs []string
	for i := range h.n {
		if h.alive[i] {
			addrs = append(addrs, h.kvServiceAddrs[i])
		}
	}
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return kvclient.New(addrs)
}

// NewClientSingleService creates a new client that will contact only a single
// service (specified by id). Note that if this isn't the leader, the client
// may get stuck in retries.
func (h *Harness) NewClientSingleService(id int) *kvclient.KVClient {
	addrs := h.kvServiceAddrs[id : id+1]
	return kvclient.New(addrs)
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
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, err := c.Put(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

// CheckAppend sends a Append request through client c, and checks there are no
// errors. Returns (prevValue, keyFound).
func (h *Harness) CheckAppend(c *kvclient.KVClient, key, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, err := c.Append(ctx, key, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

// CheckGet sends a Get request through client c, and checks there are
// no errors; it also checks that the key was found, and has the expected
// value.
func (h *Harness) CheckGet(c *kvclient.KVClient, key string, wantValue string) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
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

// CheckCAS sends a CAS request through client c, and checks there are no
// errors. Returns (prevValue, keyFound).
func (h *Harness) CheckCAS(c *kvclient.KVClient, key, compare, value string) (string, bool) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	pv, f, err := c.CAS(ctx, key, compare, value)
	if err != nil {
		h.t.Error(err)
	}
	return pv, f
}

// CheckGetNotFound sends a Get request through client c, and checks there are
// no errors, but the key isn't found in the service.
func (h *Harness) CheckGetNotFound(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	_, f, err := c.Get(ctx, key)
	if err != nil {
		h.t.Error(err)
	}
	if f {
		h.t.Errorf("got found=true, want false for key=%s", key)
	}
}

// CheckGetTimesOut checks that a Get request with the given client will
// time out if we set up a context with a deadline, because the client is
// unable to get the service to commit its command.
func (h *Harness) CheckGetTimesOut(c *kvclient.KVClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := c.Get(ctx, key)
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		h.t.Errorf("got err %v; want 'deadline exceeded'", err)
	}
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}
