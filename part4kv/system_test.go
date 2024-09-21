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

func TestPutPrevValue(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := kvclient.New(h.kvServiceAddrs)
	// Make sure we get the expected found/prev values before and after Put
	prev, found := h.CheckPut(c1, "llave", "cosa")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}

	prev, found = h.CheckPut(c1, "llave", "frodo")
	if !found || prev != "cosa" {
		t.Errorf(`got found=%v, prev=%v, want true/"cosa"`, found, prev)
	}

	// A different key...
	prev, found = h.CheckPut(c1, "mafteah", "davar")
	if found || prev != "" {
		t.Errorf(`got found=%v, prev=%v, want false/""`, found, prev)
	}
}

func TestBasicPutGetDifferentClients(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := kvclient.New(h.kvServiceAddrs)
	h.CheckPut(c1, "k", "v")

	c2 := kvclient.New(h.kvServiceAddrs)
	if v := h.CheckGetFound(c2, "k"); v != "v" {
		t.Errorf(`got %v, want "v"`, v)
	}
	sleepMs(80)
}
