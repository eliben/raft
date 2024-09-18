package main

import (
	"fmt"
	"testing"
	"time"
)

func TestSetupHarness(t *testing.T) {
	h := NewHarness(t, 3)
	fmt.Println("XXX done create harness")
	time.Sleep(20 * time.Millisecond)
	defer h.Shutdown()
}
