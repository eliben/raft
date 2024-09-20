package main

import (
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
	time.Sleep(20 * time.Millisecond)

	c1 := kvclient.New(h.kvServiceAddrs)
	if err := c1.Put("llave", "formigadon"); err != nil {
		log.Fatal(err)
	}
}
