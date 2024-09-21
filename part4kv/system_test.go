package main

import (
	"testing"
	"time"

	"github.com/eliben/raft/part4kv/kvclient"
)

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func TestSetupHarness(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	sleepMs(80)
}

func TestBasicPutGetSingleClient(t *testing.T) {
	// Basic smoke test: send one Put, followed by one Get from a single client.
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := kvclient.New(h.kvServiceAddrs)
	h.CheckPut(c1, "llave", "cosa")

	if v := h.CheckGetFound(c1, "llave"); v != "cosa" {
		t.Errorf("got %v, want cosa", v)
	}
	sleepMs(80)
}
