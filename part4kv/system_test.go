package main

import (
	"context"
	"fmt"
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
	pv, f, err := c1.Put(context.Background(), "llave", "formigadon")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pv, f)
	time.Sleep(100 * time.Millisecond)

	pv, f, err = c1.Put(context.Background(), "mafteah", "davar")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pv, f)
	time.Sleep(100 * time.Millisecond)

	pv, f, err = c1.Put(context.Background(), "llave", "nexus")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pv, f)
	time.Sleep(100 * time.Millisecond)

	gv, f, err := c1.Get(context.Background(), "llave")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(gv, f)
	time.Sleep(100 * time.Millisecond)

	gv, f, err = c1.Get(context.Background(), "zb")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(gv, f)
}
