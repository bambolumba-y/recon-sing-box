package balancer

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	N "github.com/sagernet/sing/common/network"
)

// errZeroDelay marks a probe that returned no error and no delay. ForceSelect with a delay of 0
// would leave the tag unmeasured, so such a result is not a usable rescue answer.
var errZeroDelay = errors.New("probe returned a zero delay")

// prober measures one outbound. The implementation lives in the monitoring package.
type prober interface {
	Probe(ctx context.Context, tag string, timeout time.Duration) (delay uint16, err error)
}

type failoverLogger interface {
	Info(args ...any)
	Warn(args ...any)
}

// failoverCounters is a snapshot of the controller counters. Counts only, never addresses.
type failoverCounters struct {
	ProbesActive, ProbesRescue, ProbesInterface uint64 // probes started, by origin
	ProbesOK, ProbesFailed                      uint64
	Rescues, RescueExhausted                    uint64
	Switches                                    map[string]uint64 // by reason
	Stalls, StallsSuppressed                    uint64
}

type failover struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cfg      failoverConfig
	strategy *LowestDelay
	probe    prober
	logger   failoverLogger
	onSwitch func()

	clockMu sync.Mutex
	now     func() time.Time
	sleepFn func(ctx context.Context, d time.Duration) error

	// hookMu guards the seams the Balancer installs after construction; the loops read them
	// from their own goroutines.
	hookMu        sync.Mutex
	paused        func() bool
	networkPaused func() bool
	resetStalls   func()

	// eventsMu serialises draining the strategy event buffer, so a switch is logged once.
	eventsMu sync.Mutex

	// rescueMu guards the single-flight state, the closed flag and every wg.Add, so no
	// goroutine is added after stop has started waiting.
	rescueMu      sync.Mutex
	rescueRunning atomic.Bool
	rescueDone    chan struct{} // non-nil while a rescue goroutine is alive
	closed        bool

	wg       sync.WaitGroup
	stopOnce sync.Once

	cProbesActive, cProbesRescue, cProbesInterface atomic.Uint64
	cProbesOK, cProbesFailed                       atomic.Uint64
	cRescues, cRescueExhausted                     atomic.Uint64
	cStalls, cStallsSuppressed                     atomic.Uint64

	// inflight counts the short-lived probe goroutines registered through spawn, so waitIdle
	// reports idle only once they are gone too, not just the rescue scan.
	inflight atomic.Int64

	switchMu sync.Mutex
	switches map[string]uint64
}

func newFailover(ctx context.Context, cfg failoverConfig, strategy *LowestDelay, probe prober, logger failoverLogger, onSwitch func()) *failover {
	ctx, cancel := context.WithCancel(ctx)
	if onSwitch == nil {
		onSwitch = func() {}
	}
	return &failover{
		ctx:           ctx,
		cancel:        cancel,
		cfg:           cfg,
		strategy:      strategy,
		probe:         probe,
		logger:        logger,
		onSwitch:      onSwitch,
		paused:        func() bool { return false },
		networkPaused: func() bool { return false },
		resetStalls:   func() {},
		now:           time.Now,
		sleepFn:       sleepCtx,
		switches:      map[string]uint64{},
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	// A non-positive duration lets the timer fire at once; returning early here would turn a
	// misconfigured interval into a busy loop that never observes cancellation.
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// setClock injects the clock and the sleep used by every loop, so tests run without real time.
func (f *failover) setClock(now func() time.Time, sleep func(ctx context.Context, d time.Duration) error) {
	f.clockMu.Lock()
	defer f.clockMu.Unlock()
	if now != nil {
		f.now = now
	}
	if sleep != nil {
		f.sleepFn = sleep
	}
}

// setPaused installs the predicate that suspends the active check. The Balancer passes the pause
// manager's IsPaused: device pause (screen off, no traffic) or network pause (no usable network).
func (f *failover) setPaused(paused func() bool) {
	if paused == nil {
		return
	}
	f.hookMu.Lock()
	f.paused = paused
	f.hookMu.Unlock()
}

// setNetworkPaused installs the predicate that reports "there is no network". The rescue scan
// waits on it instead of probing: with the radio down every probe is a guaranteed failure that
// still costs a wakeup, and the exhausted counter would climb for a reason that is not the
// servers' fault.
func (f *failover) setNetworkPaused(paused func() bool) {
	if paused == nil {
		return
	}
	f.hookMu.Lock()
	f.networkPaused = paused
	f.hookMu.Unlock()
}

// setResetStalls installs the hook that clears the stall detector state on an interface change.
func (f *failover) setResetStalls(reset func()) {
	if reset == nil {
		return
	}
	f.hookMu.Lock()
	f.resetStalls = reset
	f.hookMu.Unlock()
}

func (f *failover) isPaused() bool {
	f.hookMu.Lock()
	paused := f.paused
	f.hookMu.Unlock()
	return paused()
}

func (f *failover) isNetworkPaused() bool {
	f.hookMu.Lock()
	paused := f.networkPaused
	f.hookMu.Unlock()
	return paused()
}

func (f *failover) runResetStalls() {
	f.hookMu.Lock()
	reset := f.resetStalls
	f.hookMu.Unlock()
	reset()
}

func (f *failover) timeNow() time.Time {
	f.clockMu.Lock()
	defer f.clockMu.Unlock()
	return f.now()
}

func (f *failover) sleep(ctx context.Context, d time.Duration) error {
	f.clockMu.Lock()
	sleep := f.sleepFn
	f.clockMu.Unlock()
	return sleep(ctx, d)
}

func (f *failover) start() {
	if f.cfg.activeCheckInterval > 0 {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			f.activeCheckLoop()
		}()
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		f.diagLoop()
	}()
}

// stop cancels every loop and in-flight probe, waits for the goroutines and logs the final diag
// line. It is safe to call once after start.
func (f *failover) stop() {
	f.stopOnce.Do(func() {
		// closed is raised under rescueMu before the wait, so no goroutine can be added to
		// the wait group once stop is under way.
		f.rescueMu.Lock()
		f.closed = true
		f.rescueMu.Unlock()
		f.cancel()
		f.wg.Wait()
		f.logDiag()
	})
}

func (f *failover) activeCheckLoop() {
	for {
		if err := f.sleep(f.ctx, f.cfg.activeCheckInterval); err != nil {
			return
		}
		if f.isPaused() {
			continue
		}
		tag := f.strategy.Now()
		if tag == "" {
			continue
		}
		if _, err := f.probeOnce(tag, &f.cProbesActive); err != nil {
			if f.ctx.Err() != nil {
				return
			}
			f.reportFailure(tag, reasonProbeFailed)
		}
	}
}

func (f *failover) diagLoop() {
	for {
		if err := f.sleep(f.ctx, diagInterval); err != nil {
			return
		}
		f.logDiag()
	}
}

// reportFailure is the single entry point for dial errors, stalls and failed probes.
func (f *failover) reportFailure(tag, reason string) {
	// IsSelected, not Now(): Now() is the TCP selection, and a UDP dial error on a tag that is
	// current for UDP alone is a real failure of a server in use.
	if tag == "" || !f.strategy.IsSelected(tag) {
		// The failure belongs to a server that is no longer selected. Invalidating its
		// monitoring history is the caller's job; there is nothing to switch here.
		return
	}
	if reason == reasonStall {
		f.cStalls.Add(1)
		f.confirmStall(tag)
		return
	}
	f.handleFailure(tag, reason)
}

// confirmStall probes the stalled server once before acting on it. Silent bytes are not proof of
// a dead server: an idle long poll writes a request and waits, which the byte counters cannot
// tell apart from a server that stopped answering. A server that still answers a probe keeps the
// selection, and the switch (a reconnect for every live connection) is not paid for nothing.
// The probe is charged to ProbesActive: like the active check, it is the controller checking the
// server it is already on.
func (f *failover) confirmStall(tag string) {
	f.spawn(func() {
		if _, err := f.probeOnce(tag, &f.cProbesActive); err == nil {
			f.cStallsSuppressed.Add(1)
			return
		}
		if f.ctx.Err() != nil {
			return
		}
		// The selection may have moved while the confirmation probe ran.
		if !f.strategy.IsSelected(tag) {
			return
		}
		f.handleFailure(tag, reasonStall)
	})
}

// handleFailure is the switch-or-rescue half of reportFailure, reached directly for every reason
// but stall, and after the confirmation probe for a stall.
func (f *failover) handleFailure(tag, reason string) {
	switched, hasCandidate := f.strategy.MarkFailed(tag, reason)
	if switched {
		f.drainEvents()
		f.onSwitch()
		return
	}
	if !hasCandidate {
		f.startRescue(reason)
	}
}

// spawn runs fn on a goroutine registered under rescueMu, so stop cannot start waiting after the
// wait group was already left behind, and waitIdle counts it. A closed controller starts nothing.
func (f *failover) spawn(fn func()) {
	f.rescueMu.Lock()
	if f.closed {
		f.rescueMu.Unlock()
		return
	}
	f.inflight.Add(1)
	f.wg.Add(1)
	f.rescueMu.Unlock()
	go func() {
		defer func() {
			f.inflight.Add(-1)
			f.wg.Done()
		}()
		fn()
	}()
}

// startRescue launches the rescue scan. It is single flight: while a scan runs, further calls do
// nothing. The flag and the idle channel are flipped together under rescueMu, so the moment
// waitIdle reports idle is the moment startRescue starts accepting again: a dial error for the
// tag a rescue just force-selected is never swallowed.
func (f *failover) startRescue(reason string) {
	f.rescueMu.Lock()
	if f.closed || f.rescueRunning.Load() {
		f.rescueMu.Unlock()
		return
	}
	f.rescueRunning.Store(true)
	done := make(chan struct{})
	f.rescueDone = done
	f.wg.Add(1)
	f.rescueMu.Unlock()

	go func() {
		defer func() {
			f.rescueMu.Lock()
			f.rescueRunning.Store(false)
			f.rescueDone = nil
			f.rescueMu.Unlock()
			close(done)
			f.wg.Done()
		}()
		f.runRescue(reason)
	}()
}

func (f *failover) runRescue(reason string) {
	started := f.timeNow()
	for attempt := 0; ; {
		if f.ctx.Err() != nil {
			return
		}
		if f.isNetworkPaused() {
			// No usable network: a scan would probe every server for a guaranteed
			// timeout. Wait and look again without spending an attempt, so the backoff
			// still starts from the short end once the network is back.
			if err := f.sleep(f.ctx, rescueBackoff[0]); err != nil {
				return
			}
			continue
		}
		from := f.strategy.Now()
		if from == "" || f.strategy.Healthy(from) {
			// A monitoring sweep revived the current server while the scan was running.
			return
		}
		if f.scanOnce(from, reason, started) {
			return
		}
		if f.ctx.Err() != nil {
			// The scan was cut short by stop, not by unreachable servers.
			return
		}
		f.cRescueExhausted.Add(1)
		f.logSwitchLine(from, from, reasonRescueExhausted, f.elapsedMS(started))
		backoff := rescueBackoff[min(attempt, len(rescueBackoff)-1)]
		attempt++
		if err := f.sleep(f.ctx, backoff); err != nil {
			return
		}
	}
}

// scanOnce probes the candidates batch by batch and force-selects the fastest responder of the
// first batch that answers. It reports whether the scan found a server.
func (f *failover) scanOnce(from, reason string, started time.Time) bool {
	candidates := f.strategy.Candidates(from)
	batch := f.cfg.rescueBatch
	if batch <= 0 {
		batch = defaultRescueBatch
	}
	for start := 0; start < len(candidates); start += batch {
		if f.ctx.Err() != nil {
			return false
		}
		end := min(start+batch, len(candidates))
		tag, delay, ok := f.probeBatch(candidates[start:end])
		if !ok {
			continue
		}
		// The force select and the draining of its events happen under eventsMu, so a
		// concurrent drainEvents cannot pick those events up and log the same switch a
		// second time. The explicit line below carries the elapsed time instead.
		f.eventsMu.Lock()
		selected := f.strategy.ForceSelect(tag, reason, delay)
		if selected {
			f.consumeEventsLocked(false)
		}
		f.eventsMu.Unlock()
		if !selected {
			continue
		}
		f.cRescues.Add(1)
		f.logSwitchLine(from, tag, reason, f.elapsedMS(started))
		f.onSwitch()
		return true
	}
	return false
}

// probeBatch probes every tag of the batch in parallel and returns the successful tag with the
// lowest delay.
func (f *failover) probeBatch(tags []string) (string, uint16, bool) {
	type result struct {
		tag   string
		delay uint16
		ok    bool
	}
	results := make([]result, len(tags))
	var wg sync.WaitGroup
	for i, tag := range tags {
		wg.Add(1)
		go func(i int, tag string) {
			defer wg.Done()
			delay, err := f.probeOnce(tag, &f.cProbesRescue)
			results[i] = result{tag: tag, delay: delay, ok: err == nil && delay > 0}
		}(i, tag)
	}
	wg.Wait()
	var (
		bestTag   string
		bestDelay uint16
	)
	for _, r := range results {
		if !r.ok {
			continue
		}
		if bestTag == "" || r.delay < bestDelay {
			bestTag, bestDelay = r.tag, r.delay
		}
	}
	return bestTag, bestDelay, bestTag != ""
}

// probeOnce runs one probe under the rescue timeout and bumps the origin counter plus the
// outcome counters.
func (f *failover) probeOnce(tag string, origin *atomic.Uint64) (uint16, error) {
	origin.Add(1)
	ctx, cancel := context.WithTimeout(f.ctx, f.cfg.rescueTimeout)
	defer cancel()
	delay, err := f.probe.Probe(ctx, tag, f.cfg.rescueTimeout)
	if err != nil || delay == 0 {
		f.cProbesFailed.Add(1)
		if err == nil {
			err = errZeroDelay
		}
		return 0, err
	}
	f.cProbesOK.Add(1)
	return delay, nil
}

// onInterfaceChange resets the stall detector and probes the current server once: a new interface
// invalidates the sockets, not necessarily the server.
func (f *failover) onInterfaceChange() {
	f.runResetStalls()
	tag := f.strategy.Now()
	if tag == "" {
		return
	}
	f.spawn(func() {
		if _, err := f.probeOnce(tag, &f.cProbesInterface); err != nil {
			if f.ctx.Err() != nil {
				return
			}
			f.reportFailure(tag, reasonNetworkChange)
		}
	})
}

// drainEvents logs the strategy switch events as failover lines. Only TCP events are logged; a
// UDP event that mirrors one is the same switch seen twice.
func (f *failover) drainEvents() {
	f.eventsMu.Lock()
	defer f.eventsMu.Unlock()
	f.consumeEventsLocked(true)
}

// consumeEventsLocked drains the strategy event buffer. The caller holds eventsMu.
//
// A normal switch produces a mirrored pair, TCP first then UDP, carrying the same from/to/reason:
// that is one switch and is counted once. A UDP-only change (the tag was current for UDP alone)
// has no TCP twin, so it is counted on its own instead of being dropped. Logging stays TCP-only:
// the UDP line of a mirrored pair would just repeat the TCP one, and a lone UDP switch is not
// worth a line on the device.
func (f *failover) consumeEventsLocked(log bool) {
	events := f.strategy.Events()
	seen := make(map[switchEvent]struct{}, len(events))
	for _, ev := range events {
		key := switchEvent{From: ev.From, To: ev.To, Reason: ev.Reason}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		f.switchMu.Lock()
		f.switches[ev.Reason]++
		f.switchMu.Unlock()
		if log && ev.Network == N.NetworkTCP {
			f.logSwitchLine(ev.From, ev.To, ev.Reason, 0)
		}
	}
}

func (f *failover) logSwitchLine(from, to, reason string, tookMS int64) {
	f.logger.Info("failover: ", from, " -> ", to, " reason=", reason, " took=", tookMS, "ms")
}

func (f *failover) elapsedMS(started time.Time) int64 {
	d := f.timeNow().Sub(started)
	if d < 0 {
		d = 0
	}
	return d.Milliseconds()
}

func (f *failover) logDiag() {
	c := f.counters()
	reasons := make([]string, 0, len(c.Switches))
	for reason := range c.Switches {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, reason+"="+strconv.FormatUint(c.Switches[reason], 10))
	}
	f.logger.Info(
		"diag: current=", f.strategy.Now(),
		" probes_active=", c.ProbesActive,
		" probes_rescue=", c.ProbesRescue,
		" probes_interface=", c.ProbesInterface,
		" probes_ok=", c.ProbesOK,
		" probes_failed=", c.ProbesFailed,
		" rescues=", c.Rescues,
		" rescue_exhausted=", c.RescueExhausted,
		" stalls=", c.Stalls,
		" stalls_suppressed=", c.StallsSuppressed,
		" switches=", strings.Join(parts, ","),
	)
}

func (f *failover) counters() failoverCounters {
	f.switchMu.Lock()
	switches := make(map[string]uint64, len(f.switches))
	for reason, n := range f.switches {
		switches[reason] = n
	}
	f.switchMu.Unlock()
	return failoverCounters{
		ProbesActive:     f.cProbesActive.Load(),
		ProbesRescue:     f.cProbesRescue.Load(),
		ProbesInterface:  f.cProbesInterface.Load(),
		ProbesOK:         f.cProbesOK.Load(),
		ProbesFailed:     f.cProbesFailed.Load(),
		Rescues:          f.cRescues.Load(),
		RescueExhausted:  f.cRescueExhausted.Load(),
		Switches:         switches,
		Stalls:           f.cStalls.Load(),
		StallsSuppressed: f.cStallsSuppressed.Load(),
	}
}

// waitIdle reports whether the controller has nothing in flight: no rescue scan and no spawned
// probe (stall confirmation, interface check). It returns true only after those goroutines have
// fully exited, which makes the tests deterministic. inflight is raised before the goroutine
// starts and lowered after it returns, and a confirmation that decides to rescue has already
// published rescueDone by then, so the two never both read idle in the gap.
func (f *failover) waitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		f.rescueMu.Lock()
		done := f.rescueDone
		f.rescueMu.Unlock()
		if done == nil {
			if f.inflight.Load() == 0 {
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(time.Millisecond)
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-done:
		case <-timer.C:
			timer.Stop()
			return false
		}
		timer.Stop()
	}
}
