package monitoring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

func newTestMonitor(t *testing.T, opts option.MonitoringOptions) *OutboundMonitoring {
	t.Helper()
	m, err := NewOutboundMonitoring(context.Background(), log.NewNOPFactory().NewLogger("test"), opts)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNegativeIntervalDisablesSweep(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{Interval: badoption.Duration(-1)})
	if m.SweepEnabled() {
		t.Fatal("negative interval must disable the sweep")
	}
	m.started = true
	m.Touch() // must not panic and must not start a ticker
	if m.mainTicker != nil {
		t.Fatal("ticker must not start when the sweep is disabled")
	}
}

func TestZeroIntervalKeepsDefault(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	if !m.SweepEnabled() || m.mainInterval != 5*time.Minute {
		t.Fatalf("enabled=%v interval=%v", m.SweepEnabled(), m.mainInterval)
	}
}

func TestInterfaceSweepCanBeDisabled(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{DisableInterfaceSweep: true})
	m.InterfaceUpdated()
	if s := m.Stats(); s.CyclesStarted != 0 || s.CyclesSkipped != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestTestAndWaitUnknownTag(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	if _, err := m.TestAndWait(context.Background(), "nope", time.Second); err == nil {
		t.Fatal("unknown tag must return an error")
	}
}

func TestTestAndWaitProbePathAndCounters(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	m.outbounds["test-tag"] = &outboundState{}

	fixedHistory := adapter.URLTestHistory{Delay: 42}
	probeErr := errors.New("probe failed")
	calls := 0
	m.probe = func(ctx context.Context, tag string) (adapter.URLTestHistory, error) {
		calls++
		if calls == 1 {
			return fixedHistory, nil
		}
		return adapter.URLTestHistory{}, probeErr
	}

	his, err := m.TestAndWait(context.Background(), "test-tag", time.Second)
	if err != nil {
		t.Fatalf("first call: unexpected error %v", err)
	}
	if his.Delay != fixedHistory.Delay {
		t.Fatalf("first call: history = %+v, want delay %v", his, fixedHistory.Delay)
	}

	_, err = m.TestAndWait(context.Background(), "test-tag", time.Second)
	if !errors.Is(err, probeErr) {
		t.Fatalf("second call: err = %v, want %v", err, probeErr)
	}

	s := m.Stats()
	if s.ProbesDirect != 2 {
		t.Fatalf("ProbesDirect = %d, want 2", s.ProbesDirect)
	}
	if s.ProbesOK != 1 {
		t.Fatalf("ProbesOK = %d, want 1", s.ProbesOK)
	}
	if s.ProbesFailed != 1 {
		t.Fatalf("ProbesFailed = %d, want 1", s.ProbesFailed)
	}
}

func TestTestAndWaitAfterCloseReturnsError(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	m.outbounds["test-tag"] = &outboundState{}

	probeCalls := 0
	m.probe = func(ctx context.Context, tag string) (adapter.URLTestHistory, error) {
		probeCalls++
		return adapter.URLTestHistory{}, nil
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := m.TestAndWait(context.Background(), "test-tag", time.Second); err == nil {
		t.Fatal("TestAndWait after Close must return an error")
	}
	if probeCalls != 0 {
		t.Fatalf("probe must not run after Close, got %d calls", probeCalls)
	}
}

// TestTestAndWaitMarksStateTesting pins R5: while a direct probe is in flight the sweep must not
// pick the same outbound up again, or two probes race and the older result can land last.
func TestTestAndWaitMarksStateTesting(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	m.outbounds["test-tag"] = &outboundState{}

	contains := func(tags []string, want string) bool {
		for _, tag := range tags {
			if tag == want {
				return true
			}
		}
		return false
	}

	if !contains(m.collectCycleTargets(), "test-tag") {
		t.Fatal("baseline: an untested outbound must be a sweep target")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	m.probe = func(ctx context.Context, tag string) (adapter.URLTestHistory, error) {
		close(entered)
		<-release
		return adapter.URLTestHistory{Delay: 42}, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.TestAndWait(context.Background(), "test-tag", time.Second)
	}()

	<-entered
	if tags := m.collectCycleTargets(); contains(tags, "test-tag") {
		t.Fatalf("an outbound under a direct probe must not be queued by the sweep: %v", tags)
	}

	close(release)
	<-done

	m.outbounds["test-tag"].mu.Lock()
	stillTesting := m.outbounds["test-tag"].testing
	m.outbounds["test-tag"].mu.Unlock()
	if stillTesting {
		t.Fatal("the testing flag must be cleared once the probe is applied")
	}
}

// TestTestAndWaitDoesNotClobberAnotherTest pins the other half: a caller that arrives while a
// queued test already holds the flag still gets its answer, but must not clear a flag it did not
// set.
func TestTestAndWaitDoesNotClobberAnotherTest(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	state := &outboundState{}
	m.outbounds["test-tag"] = state
	state.mu.Lock()
	state.testing = true // stands in for a queued test already running
	state.mu.Unlock()

	m.probe = func(ctx context.Context, tag string) (adapter.URLTestHistory, error) {
		return adapter.URLTestHistory{Delay: 7}, nil
	}
	his, err := m.TestAndWait(context.Background(), "test-tag", time.Second)
	if err != nil || his.Delay != 7 {
		t.Fatalf("the caller must still get an answer: his=%+v err=%v", his, err)
	}
	state.mu.Lock()
	stillTesting := state.testing
	state.mu.Unlock()
	if !stillTesting {
		t.Fatal("the flag belongs to the queued test and must survive")
	}
}

// TestTestAndWaitClearsTestingOnProbePanic pins the fix for a stuck testing flag: if the probe
// panics, the clear must still run, or collectCycleTargets skips the tag on every later sweep.
func TestTestAndWaitClearsTestingOnProbePanic(t *testing.T) {
	m := newTestMonitor(t, option.MonitoringOptions{})
	m.outbounds["test-tag"] = &outboundState{}

	m.probe = func(ctx context.Context, tag string) (adapter.URLTestHistory, error) {
		panic("probe blew up")
	}

	func() {
		defer func() { recover() }()
		m.TestAndWait(context.Background(), "test-tag", time.Second)
	}()

	m.outbounds["test-tag"].mu.Lock()
	stillTesting := m.outbounds["test-tag"].testing
	m.outbounds["test-tag"].mu.Unlock()
	if stillTesting {
		t.Fatal("the testing flag must be cleared even when the probe panics")
	}

	contains := func(tags []string, want string) bool {
		for _, tag := range tags {
			if tag == want {
				return true
			}
		}
		return false
	}
	if !contains(m.collectCycleTargets(), "test-tag") {
		t.Fatal("the tag must still be a sweep target after the panic")
	}
}
