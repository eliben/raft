package main

import (
	"testing"
	"time"
)

func TestSetupHarness(t *testing.T) {
	h := NewHarness(t, 3)
	time.Sleep(20 * time.Millisecond)
	defer h.Shutdown()
}
