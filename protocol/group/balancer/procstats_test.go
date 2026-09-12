package balancer

import (
	"testing"
	"time"
)

func TestReadProcStats(t *testing.T) {
	before := readProcStats()
	if !before.ok {
		t.Skip("readProcStats not implemented on this platform")
	}
	if before.rssBytes == 0 {
		t.Fatal("rssBytes = 0, want > 0")
	}

	// Burn CPU for a bit so the next sample's cpu time has a chance to move.
	deadline := time.Now().Add(50 * time.Millisecond)
	sum := 0
	for time.Now().Before(deadline) {
		sum += sum + 1
	}
	_ = sum

	after := readProcStats()
	if !after.ok {
		t.Fatal("readProcStats became not ok on the second call")
	}
	if after.cpu <= before.cpu && after.cpu <= 0 {
		t.Fatalf("cpu did not advance and is not positive: before=%v after=%v", before.cpu, after.cpu)
	}
}
