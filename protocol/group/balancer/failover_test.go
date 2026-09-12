package balancer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type scriptedProber struct {
	mu     sync.Mutex
	delays map[string]uint16 // missing or 0 => error
	calls  []string
	block  map[string]chan struct{} // optional: probe waits until channel closed
	// clock, when set, is advanced by the cost of every probe: the measured delay on
	// success, the full timeout on failure. Without it the virtual clock never moves and
	// every recovery time a scenario reports is 0.
	clock *fakeClock
}

// spend advances the attached clock by what the probe cost.
func (p *scriptedProber) spend(d time.Duration) {
	if p.clock != nil {
		p.clock.Advance(d)
	}
}

func (p *scriptedProber) Probe(ctx context.Context, tag string, timeout time.Duration) (uint16, error) {
	p.mu.Lock()
	p.calls = append(p.calls, tag)
	d := p.delays[tag]
	b := p.block[tag]
	p.mu.Unlock()
	if b != nil {
		select {
		case <-b:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		p.mu.Lock()
		d = p.delays[tag]
		p.mu.Unlock()
	}
	if d == 0 {
		p.spend(timeout)
		return 0, errors.New("probe failed")
	}
	p.spend(time.Duration(d) * time.Millisecond)
	return d, nil
}

func (p *scriptedProber) set(tag string, delay uint16) {
	p.mu.Lock()
	p.delays[tag] = delay
	p.mu.Unlock()
}
func (p *scriptedProber) count(tag string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if c == tag {
			n++
		}
	}
	return n
}

func (p *scriptedProber) callList() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

type memLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *memLogger) Info(args ...any) { l.add(args...) }
func (l *memLogger) Warn(args ...any) { l.add(args...) }

// add mirrors the sing-box logger: fmt.Sprint over the arguments, so the spaces the caller
// puts inside the string fragments are the spaces that reach the log line.
func (l *memLogger) add(args ...any) {
	line := fmt.Sprint(args...)
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
}

func (l *memLogger) has(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ln := range l.lines {
		if strings.Contains(ln, sub) {
			return true
		}
	}
	return false
}

func (l *memLogger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

func virtualSleep(clock *fakeClock) func(ctx context.Context, d time.Duration) error {
	return func(ctx context.Context, d time.Duration) error {
		clock.Advance(d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			return nil
		}
	}
}

func newHarness(t *testing.T, tags ...string) (*failover, *LowestDelay, *scriptedProber, *memLogger, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	strategy := NewLowestDelay(fakeOutbounds(tags...), option.BalancerOutboundOptions{
		RescueBatch: 2, RescueTimeout: badoption.Duration(50 * time.Millisecond), ActiveCheckInterval: badoption.Duration(-1),
	})
	strategy.setClock(clock.Now)
	p := &scriptedProber{delays: map[string]uint16{}, block: map[string]chan struct{}{}, clock: clock}
	l := &memLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := newFailover(ctx, strategy.cfg, strategy, p, l, func() {})
	f.setClock(clock.Now, virtualSleep(clock))
	t.Cleanup(func() { cancel(); f.waitIdle(time.Second) })
	return f, strategy, p, l, clock
}

func TestFailureWithCandidateSwitchesWithoutProbe(t *testing.T) {
	f, s, p, l, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	f.reportFailure("a", reasonDialError)
	if s.Now() != "b" || len(p.callList()) != 0 {
		t.Fatalf("now=%q probes=%v", s.Now(), p.callList())
	}
	if !l.has("failover: a -> b reason=dial_error took=0ms") {
		t.Fatalf("log lines: %v", l.snapshot())
	}
}

func TestFailureForNonCurrentIsIgnored(t *testing.T) {
	f, s, _, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	f.reportFailure("b", reasonDialError)
	if s.Now() != "a" || !s.Healthy("a") {
		t.Fatal("a failure of a non-selected server must not move the selection")
	}
}

func TestRescuePicksFirstResponderInBatches(t *testing.T) {
	f, s, p, l, c := newHarness(t, "a", "b", "c", "d", "e")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	p.set("d", 300) // only d answers; batches: [b c] then [d e]
	f.reportFailure("a", reasonStall)
	if !f.waitIdle(2 * time.Second) {
		t.Fatal("rescue did not finish")
	}
	if s.Now() != "d" {
		t.Fatalf("now=%q calls=%v", s.Now(), p.callList())
	}
	if p.count("e") > 1 || p.count("b") != 1 || p.count("c") != 1 {
		t.Fatalf("batching wrong: %v", p.callList())
	}
	if !l.has("failover: a -> d reason=stall") {
		t.Fatalf("lines: %v", l.snapshot())
	}
	if got := f.counters(); got.Rescues != 1 || got.ProbesRescue < 3 {
		t.Fatalf("counters = %+v", got)
	}
}

func TestRescueExhaustedBacksOffThenRecovers(t *testing.T) {
	f, s, p, l, c := newHarness(t, "a", "b", "c")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	f.reportFailure("a", reasonDialError)
	deadline := time.Now().Add(2 * time.Second)
	for p.count("b") < 2 && time.Now().Before(deadline) { // second attempt reached => backoff slept once
		time.Sleep(5 * time.Millisecond)
	}
	if !l.has("reason=rescue_exhausted") {
		t.Fatalf("lines: %v", l.snapshot())
	}
	p.set("c", 250)
	if !f.waitIdle(2*time.Second) || s.Now() != "c" {
		t.Fatalf("now=%q lines=%v", s.Now(), l.snapshot())
	}
	if f.counters().RescueExhausted == 0 {
		t.Fatal("exhausted attempts must be counted")
	}
}

func TestRescueIsSingleFlight(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	gate := make(chan struct{})
	p.block["b"] = gate
	f.reportFailure("a", reasonDialError)
	f.reportFailure("a", reasonStall)
	f.reportFailure("a", reasonProbeFailed)
	time.Sleep(20 * time.Millisecond)
	if p.count("b") != 1 {
		t.Fatalf("concurrent failure reports must not start parallel rescues: %v", p.callList())
	}
	p.set("b", 120)
	close(gate)
	if !f.waitIdle(2*time.Second) || s.Now() != "b" {
		t.Fatalf("now=%q", s.Now())
	}
}

func TestRescueStopsWhenCurrentRevives(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	f.reportFailure("a", reasonDialError) // b unknown and failing -> exhausted, backoff
	time.Sleep(20 * time.Millisecond)
	c.Advance(time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(90, c.Now())}) // sweep says a is fine
	if !f.waitIdle(3 * time.Second) {
		t.Fatal("rescue must stop once the current server is healthy again")
	}
	_ = p
}

func TestActiveCheckFailureTriggersSwitch(t *testing.T) {
	clock := newFakeClock()
	strategy := NewLowestDelay(fakeOutbounds("a", "b"), option.BalancerOutboundOptions{
		ActiveCheckInterval: badoption.Duration(time.Minute), RescueTimeout: badoption.Duration(50 * time.Millisecond),
	})
	strategy.setClock(clock.Now)
	strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, clock.Now()), "b": measured(200, clock.Now())})
	p := &scriptedProber{delays: map[string]uint16{"b": 200}, block: map[string]chan struct{}{}}
	l := &memLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFailover(ctx, strategy.cfg, strategy, p, l, func() {})
	f.setClock(clock.Now, virtualSleep(clock))
	f.start()
	defer f.stop()
	deadline := time.Now().Add(2 * time.Second)
	for strategy.Now() != "b" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if strategy.Now() != "b" || p.count("a") == 0 {
		t.Fatalf("now=%q calls=%v", strategy.Now(), p.callList())
	}
	if !l.has("reason=probe_failed") {
		t.Fatalf("lines: %v", l.snapshot())
	}
}

func TestInterfaceChangeProbesOnlyCurrent(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b", "c")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(150, c.Now())})
	p.set("a", 110)
	f.onInterfaceChange()
	time.Sleep(20 * time.Millisecond)
	if p.count("a") != 1 || p.count("b") != 0 || p.count("c") != 0 || s.Now() != "a" {
		t.Fatalf("calls=%v now=%q", p.callList(), s.Now())
	}
}

func TestDiagLineHasCountsOnly(t *testing.T) {
	f, _, _, l, _ := newHarness(t, "a", "b")
	f.logDiag()
	if !l.has("diag: current=a probes_active=0") || l.has("http") {
		t.Fatalf("lines: %v", l.snapshot())
	}
}

func TestRescuePicksLowestDelayInBatch(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b", "c")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	// Both candidates fit in one batch of two and both answer; the faster one wins whatever
	// order Candidates returned them in.
	p.set("b", 300)
	p.set("c", 120)
	f.reportFailure("a", reasonDialError)
	if !f.waitIdle(2 * time.Second) {
		t.Fatal("rescue did not finish")
	}
	if p.count("b") != 1 || p.count("c") != 1 {
		t.Fatalf("both members of the batch must be probed: %v", p.callList())
	}
	if s.Now() != "c" {
		t.Fatalf("the batch winner must be the lowest delay: now=%q calls=%v", s.Now(), p.callList())
	}
}

func TestFailureRightAfterRescueStartsANewRescue(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	gate := make(chan struct{})
	p.block["b"] = gate
	f.reportFailure("a", reasonDialError)
	p.set("b", 120)
	close(gate)
	if !f.waitIdle(2*time.Second) || s.Now() != "b" {
		t.Fatalf("first rescue did not select b: now=%q", s.Now())
	}
	// b is only synthetically healthy; a dial error arriving right now must not be swallowed
	// by a single-flight flag that outlives the idle state.
	before := f.counters()
	f.reportFailure("b", reasonDialError)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := f.counters(); got.ProbesRescue > before.ProbesRescue && got.RescueExhausted > before.RescueExhausted {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("a second rescue must start: before=%+v after=%+v calls=%v", before, f.counters(), p.callList())
}

func TestStallsCountedOnlyForTheCurrentTag(t *testing.T) {
	f, s, _, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	f.reportFailure("b", reasonStall)
	if got := f.counters().Stalls; got != 0 {
		t.Fatalf("a stall on a non-selected server must not be counted: stalls=%d", got)
	}
	f.reportFailure("a", reasonStall)
	if got := f.counters().Stalls; got != 1 {
		t.Fatalf("stalls=%d, want 1", got)
	}
}

// pausedHarness builds a started controller with a one minute active check and a sleep that
// counts the active check ticks.
func pausedHarness(t *testing.T, paused func() bool) (*failover, *scriptedProber, *atomic.Int64) {
	t.Helper()
	clock := newFakeClock()
	strategy := NewLowestDelay(fakeOutbounds("a", "b"), option.BalancerOutboundOptions{
		ActiveCheckInterval: badoption.Duration(time.Minute), RescueTimeout: badoption.Duration(50 * time.Millisecond),
	})
	strategy.setClock(clock.Now)
	strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, clock.Now()), "b": measured(200, clock.Now())})
	p := &scriptedProber{delays: map[string]uint16{}, block: map[string]chan struct{}{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := newFailover(ctx, strategy.cfg, strategy, p, &memLogger{}, func() {})
	var ticks atomic.Int64
	f.setClock(clock.Now, func(ctx context.Context, d time.Duration) error {
		if d == time.Minute {
			ticks.Add(1)
		}
		clock.Advance(d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			return nil
		}
	})
	f.setPaused(paused)
	return f, p, &ticks
}

func waitTicks(t *testing.T, ticks *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for ticks.Load() < want && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if ticks.Load() < want {
		t.Fatalf("active check ticked %d times, want at least %d", ticks.Load(), want)
	}
}

func TestPausedActiveCheckDoesNotProbe(t *testing.T) {
	f, p, ticks := pausedHarness(t, func() bool { return true })
	f.start()
	defer f.stop()
	waitTicks(t, ticks, 3)
	// Reinstalling the hook while the loop is running is what the Balancer does; the setter
	// and the loop must not touch the field unsynchronised.
	f.setPaused(func() bool { return true })
	f.setResetStalls(func() {})
	waitTicks(t, ticks, 6)
	if calls := p.callList(); len(calls) != 0 {
		t.Fatalf("a paused controller must not probe: %v", calls)
	}
}

func TestStopIsIdempotentAndEndsARescueInBackoff(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	f.start()
	f.reportFailure("a", reasonDialError) // b never answers: exhausted, then backoff forever
	deadline := time.Now().Add(2 * time.Second)
	for f.counters().RescueExhausted == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if f.counters().RescueExhausted == 0 {
		t.Fatalf("the rescue never reached the backoff: %v", p.callList())
	}
	done := make(chan struct{})
	go func() {
		f.stop()
		f.stop() // idempotent
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop did not return while a rescue was parked in backoff")
	}
	if !f.waitIdle(time.Second) {
		t.Fatal("the rescue goroutine outlived stop")
	}
	// After stop no new rescue may be launched.
	f.reportFailure("a", reasonDialError)
	if !f.waitIdle(time.Second) {
		t.Fatal("a rescue was started after stop")
	}
}

// waitUntil polls cond, which is what the asynchronous halves of the controller (the stall
// confirmation probe, a rescue) need instead of a fixed sleep.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// --- R1: the rescue scan waits out a network pause ---

func TestRescueDoesNotProbeWhileNetworkPaused(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b", "c")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	var netPaused atomic.Bool
	netPaused.Store(true)
	f.setNetworkPaused(netPaused.Load)

	f.reportFailure("a", reasonDialError) // nothing healthy left -> rescue
	// The rescue loop spins through its wait; give it many turns and prove that none of them
	// probed and none of them burned an attempt.
	time.Sleep(60 * time.Millisecond)
	if calls := p.callList(); len(calls) != 0 {
		t.Fatalf("a rescue must not probe while the network is paused: %v", calls)
	}
	if got := f.counters().RescueExhausted; got != 0 {
		t.Fatalf("waiting for the network must not spend rescue attempts, exhausted=%d", got)
	}

	p.set("b", 120)
	netPaused.Store(false)
	if !f.waitIdle(3*time.Second) || s.Now() != "b" {
		t.Fatalf("the rescue must resume once the network is back: now=%q calls=%v", s.Now(), p.callList())
	}
}

func TestNetworkPausedRescueStopsOnCancel(t *testing.T) {
	f, s, _, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	f.setNetworkPaused(func() bool { return true })
	f.start()
	f.reportFailure("a", reasonDialError)
	time.Sleep(20 * time.Millisecond)
	done := make(chan struct{})
	go func() { f.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stop must return while a rescue is parked waiting for the network")
	}
}

// --- R3: a failure on a tag that is current for UDP only ---

// udpOnlyHarness selects a TCP-only server for TCP and "u1" for UDP, so "u1" is current for UDP
// and not for TCP.
func udpOnlyHarness(t *testing.T) (*failover, *LowestDelay, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	outs := []adapter.Outbound{
		newFakeOutboundNetworks("tcponly", N.NetworkTCP),
		newFakeOutboundNetworks("u1", N.NetworkTCP, N.NetworkUDP),
		newFakeOutboundNetworks("u2", N.NetworkTCP, N.NetworkUDP),
	}
	strategy := NewLowestDelay(outs, option.BalancerOutboundOptions{ActiveCheckInterval: badoption.Duration(-1)})
	strategy.setClock(clock.Now)
	// Seed u2 as measured without an UpdateOutboundsInfo, which would move the TCP selection
	// too and destroy the split this test needs.
	strategy.mu.Lock()
	strategy.history["u2"] = measured(200, clock.Now())
	strategy.mu.Unlock()
	if strategy.Now() != "tcponly" {
		t.Fatalf("tcp selection = %q, want tcponly", strategy.Now())
	}
	if udp := strategy.Select(adapter.InboundContext{}, N.NetworkUDP, false); udp.Tag() != "u1" {
		t.Fatalf("udp selection = %q, want u1", udp.Tag())
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := &scriptedProber{delays: map[string]uint16{}, block: map[string]chan struct{}{}}
	f := newFailover(ctx, strategy.cfg, strategy, p, &memLogger{}, func() {})
	f.setClock(clock.Now, virtualSleep(clock))
	t.Cleanup(func() { cancel(); f.waitIdle(time.Second) })
	return f, strategy, clock
}

func TestIsSelectedCoversUDP(t *testing.T) {
	_, s, _ := udpOnlyHarness(t)
	if !s.IsSelected("tcponly") || !s.IsSelected("u1") {
		t.Fatal("both the TCP and the UDP selection must count as selected")
	}
	if s.IsSelected("u2") {
		t.Fatal("u2 is selected for neither network")
	}
}

func TestUDPOnlyFailureSwitchesUDPAndIsCounted(t *testing.T) {
	f, s, _ := udpOnlyHarness(t)
	f.reportFailure("u1", reasonDialError)
	if s.Now() != "tcponly" {
		t.Fatalf("the TCP selection must not move, now=%q", s.Now())
	}
	if udp := s.Select(adapter.InboundContext{}, N.NetworkUDP, false); udp.Tag() != "u2" {
		t.Fatalf("udp selection = %q, want u2", udp.Tag())
	}
	if n := f.counters().Switches[reasonDialError]; n != 1 {
		t.Fatalf("a UDP-only switch must be counted once, got %d", n)
	}
}

func TestMirroredSwitchIsCountedOnce(t *testing.T) {
	f, s, _, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	f.reportFailure("a", reasonDialError)
	if n := f.counters().Switches[reasonDialError]; n != 1 {
		t.Fatalf("the TCP+UDP pair of one switch must be counted once, got %d", n)
	}
}

func TestListenPacketErrorReportsFailure(t *testing.T) {
	f, s, _, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	// monitor is nil on purpose: that is the Clash-API-off case, where InvalidateTest used to
	// dereference nil before the failure could ever be reported.
	b := &Balancer{
		ctx:            context.Background(),
		logger:         log.NewNOPFactory().NewLogger("test"),
		strategyFn:     s,
		failover:       f,
		interruptGroup: interrupt.NewGroup(),
	}
	if _, err := b.ListenPacket(context.Background(), M.Socksaddr{}); err == nil {
		t.Fatal("the fake outbound must fail to listen")
	}
	if s.Now() != "b" {
		t.Fatalf("a UDP listen error must move the selection, now=%q", s.Now())
	}
	if n := f.counters().Switches[reasonDialError]; n != 1 {
		t.Fatalf("switches[dial_error] = %d, want 1", n)
	}
}

// --- R4: a stall is confirmed by a probe before it switches ---

func TestStallWithHealthyProbeIsSuppressed(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	p.set("a", 90) // the "stalled" server still answers: an idle long poll, not a dead server
	f.reportFailure("a", reasonStall)
	if !f.waitIdle(3 * time.Second) {
		t.Fatal("the confirmation probe did not finish")
	}
	if got := f.counters(); got.StallsSuppressed != 1 || got.Stalls != 1 {
		t.Fatalf("counters = %+v, want stalls=1 stalls_suppressed=1", got)
	}
	if s.Now() != "a" {
		t.Fatalf("a confirmed-alive server must keep the selection, now=%q", s.Now())
	}
	if n := f.counters().Switches[reasonStall]; n != 0 {
		t.Fatalf("no switch may be recorded, got %d", n)
	}
	if p.count("a") != 1 {
		t.Fatalf("the confirmation must be exactly one probe, calls=%v", p.callList())
	}
}

// TestStallConfirmationIsSingleFlight pins the fix for a duplicate confirmation probe: the stall
// tracker can fire again for the same tag while its first confirmation is still in flight, and
// the second report must be dropped rather than spawn a parallel probe.
func TestStallConfirmationIsSingleFlight(t *testing.T) {
	f, s, p, _, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	gate := make(chan struct{})
	p.block["a"] = gate
	p.set("a", 90) // the confirmation succeeds once released, so the selection is kept

	f.reportFailure("a", reasonStall)
	f.reportFailure("a", reasonStall)

	close(gate)
	if !f.waitIdle(3 * time.Second) {
		t.Fatal("the confirmation probe did not finish")
	}

	if p.count("a") != 1 {
		t.Fatalf("the duplicate stall report must not start a second probe, calls=%v", p.callList())
	}
	if got := f.counters(); got.Stalls != 1 {
		t.Fatalf("Stalls = %d, want 1 (the dropped duplicate must not be counted)", got.Stalls)
	}
	if got := f.counters().StallsSuppressed; got != 1 {
		t.Fatalf("StallsSuppressed = %d, want 1", got)
	}
	if s.Now() != "a" {
		t.Fatalf("a confirmed-alive server must keep the selection, now=%q", s.Now())
	}
}

func TestStallWithFailingProbeSwitches(t *testing.T) {
	f, s, p, l, c := newHarness(t, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	// "a" is not in the prober's table, so the confirmation probe fails.
	f.reportFailure("a", reasonStall)
	waitUntil(t, "the stall to be confirmed and switched", func() bool { return s.Now() == "b" })
	if !f.waitIdle(3 * time.Second) {
		t.Fatal("the confirmation probe did not finish")
	}
	if got := f.counters(); got.Stalls != 1 || got.StallsSuppressed != 0 {
		t.Fatalf("counters = %+v, want stalls=1 stalls_suppressed=0", got)
	}
	if !l.has("failover: a -> b reason=stall") {
		t.Fatalf("lines: %v", l.snapshot())
	}
	if p.count("a") != 1 {
		t.Fatalf("calls=%v", p.callList())
	}
}

func TestDiagLineCarriesSuppressedStalls(t *testing.T) {
	f, _, _, l, _ := newHarness(t, "a", "b")
	f.logDiag()
	if !l.has(" stalls=0 stalls_suppressed=0 rss_mb=") {
		t.Fatalf("lines: %v", l.snapshot())
	}
}

func TestDiagLineCarriesProcessStats(t *testing.T) {
	f, _, _, l, _ := newHarness(t, "a", "b")
	f.readStats = func() procStats {
		return procStats{rssBytes: 100 << 20, cpu: 12500 * time.Millisecond, ok: true}
	}
	f.logDiag()
	if !l.has(" rss_mb=100.0 cpu_s=12.5 switches=") {
		t.Fatalf("lines: %v", l.snapshot())
	}
}

func TestDiagLineWithoutProcessStats(t *testing.T) {
	f, _, _, l, _ := newHarness(t, "a", "b")
	f.readStats = func() procStats { return procStats{} }
	f.logDiag()
	if !l.has(" rss_mb=n/a cpu_s=n/a switches=") {
		t.Fatalf("lines: %v", l.snapshot())
	}
}
