package main

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/eliben/raft/part4kv/kvclient"
)

func TestSetupHarness(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	time.Sleep(20 * time.Millisecond)
}

func TestConnectWithClient(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := kvclient.New(h.kvServiceAddrs)
	if err := c1.Put(context.Background(), "llave", "formigadon"); err != nil {
		log.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := c1.Put(context.Background(), "mafteah", "davar"); err != nil {
		log.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
}
