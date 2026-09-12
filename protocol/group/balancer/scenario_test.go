package balancer

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

// The tests below are the reproducible PC scenarios of the measurement plan. Each prints one
// machine-readable line "SCENARIO name=<n> probes=<count> switches=<count> recovery_ms=<virtual ms>"
// that the Task 11 script parses.

func TestScenarioDialErrorRecovery(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b", "c")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(150, c.Now()), "c": measured(120, c.Now())})
	start := c.Now()
	f.reportFailure("a", "dial_error")
	if s.Now() != "c" {
		t.Fatalf("expected c (lowest healthy), got %q", s.Now())
	}
	// One switch, not two: UDP mirrors TCP and must not be counted a second time.
	if n := f.counters().Switches["dial_error"]; n != 1 {
		t.Fatalf("one switch expected, got %d", n)
	}
	t.Logf("SCENARIO name=dial_error_with_candidates probes=%d switches=%d recovery_ms=%d", len(p.callList()), f.counters().Switches["dial_error"], c.Now().Sub(start).Milliseconds())
}

func TestScenarioAllUnknownRescue(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b", "c", "d", "e", "f", "g")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	p.set("f", 400)
	start := c.Now()
	f.reportFailure("a", "stall")
	if !f.waitIdle(3*time.Second) || s.Now() != "f" {
		t.Fatalf("now=%q", s.Now())
	}
	recovery := c.Now().Sub(start)
	if recovery <= 0 {
		t.Fatalf("a rescue scan costs probe time, recovery_ms must be positive, got %d", recovery.Milliseconds())
	}
	t.Logf("SCENARIO name=rescue_unknown_pool probes=%d switches=%d recovery_ms=%d", len(p.callList()), f.counters().Switches["stall"], recovery.Milliseconds())
}

func TestScenarioLatencyFlapDoesNotSwitch(t *testing.T) {
	f, s, _, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(200, c.Now()), "b": measured(210, c.Now())})
	for i := 0; i < 48; i++ { // 24 virtual hours of half-hourly sweeps with +-100 ms jitter
		c.Advance(30 * time.Minute)
		da, db := uint16(200), uint16(210)
		if i%2 == 0 {
			da, db = 300, 190
		}
		s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(da, c.Now()), "b": measured(db, c.Now())})
	}
	f.drainEvents()
	if s.Now() != "a" {
		t.Fatalf("jitter under tolerance must not switch, got %q", s.Now())
	}
	if n := f.counters().Switches["better_latency"]; n != 0 {
		t.Fatalf("jitter under tolerance must not switch, got %d switches", n)
	}
	// Positive arm: b is better than a by more than the tolerance and stays there for longer
	// than min_dwell, so the balancer must move.
	for i := 0; i < 4; i++ {
		c.Advance(30 * time.Minute)
		s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(400, c.Now()), "b": measured(150, c.Now())})
	}
	f.drainEvents()
	if s.Now() != "b" {
		t.Fatalf("a sustained excursion beyond the tolerance must switch, got %q", s.Now())
	}
	if n := f.counters().Switches["better_latency"]; n != 1 {
		t.Fatalf("the excursion is one switch, got %d", n)
	}
	// ... and jitter under the tolerance must not send it back.
	for i := 0; i < 8; i++ {
		c.Advance(30 * time.Minute)
		da, db := uint16(200), uint16(210)
		if i%2 == 0 {
			da, db = 300, 190
		}
		s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(da, c.Now()), "b": measured(db, c.Now())})
	}
	f.drainEvents()
	if s.Now() != "b" {
		t.Fatalf("jitter under tolerance must not switch back, got %q", s.Now())
	}
	t.Logf("SCENARIO name=latency_jitter_24h probes=0 switches=%d recovery_ms=0", f.counters().Switches["better_latency"])
}
