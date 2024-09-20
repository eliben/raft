package main

import (
	"testing"
	"time"
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

}
