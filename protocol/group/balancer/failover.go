package balancer

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	N "github.com/sagernet/sing/common/network"
)

// Failure reasons used by the controller itself. Callers may pass their own.
const (
	reasonStall           = "stall"
	reasonProbeFailed     = "probe_failed"
	reasonNetworkChange   = "network_change"
	reasonRescueExhausted = "rescue_exhausted"
)

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
	Stalls                                      uint64
}

type failover struct {
	ctx         context.Context
	cancel      context.CancelFunc
	cfg         failoverConfig
	strategy    *LowestDelay
	probe       prober
	logger      failoverLogger
	onSwitch    func()
	paused      func() bool
	resetStalls func()

	clockMu sync.Mutex
	now     func() time.Time
	sleepFn func(ctx context.Context, d time.Duration) error

	rescueMu      sync.Mutex
	rescueRunning atomic.Bool
	rescueDone    chan struct{} // non-nil while a rescue goroutine is alive

	wg       sync.WaitGroup
	stopOnce sync.Once

	cProbesActive, cProbesRescue, cProbesInterface atomic.Uint64
	cProbesOK, cProbesFailed                       atomic.Uint64
	cRescues, cRescueExhausted, cStalls            atomic.Uint64

	switchMu sync.Mutex
	switches map[string]uint64
}

func newFailover(ctx context.Context, cfg failoverConfig, strategy *LowestDelay, probe prober, logger failoverLogger, onSwitch func()) *failover {
	ctx, cancel := context.WithCancel(ctx)
	if onSwitch == nil {
		onSwitch = func() {}
	}
	return &failover{
		ctx:         ctx,
		cancel:      cancel,
		cfg:         cfg,
		strategy:    strategy,
		probe:       probe,
		logger:      logger,
		onSwitch:    onSwitch,
		paused:      func() bool { return false },
		resetStalls: func() {},
		now:         time.Now,
		sleepFn:     sleepCtx,
		switches:    map[string]uint64{},
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
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

// setPaused installs the predicate that suspends the active check (the Balancer pauses it while
// the tunnel is idle or the screen is off).
func (f *failover) setPaused(paused func() bool) {
	if paused != nil {
		f.paused = paused
	}
}

// setResetStalls installs the hook that clears the stall detector state on an interface change.
func (f *failover) setResetStalls(reset func()) {
	if reset != nil {
		f.resetStalls = reset
	}
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
		if f.paused() {
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
	if reason == reasonStall {
		f.cStalls.Add(1)
	}
	if tag == "" || tag != f.strategy.Now() {
		// The failure belongs to a server that is no longer selected. Invalidating its
		// monitoring history is the caller's job; there is nothing to switch here.
		return
	}
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

// startRescue launches the rescue scan. It is single flight: while a scan runs, further calls do
// nothing. The flag is released only after the goroutine has fully exited, so a failure reported
// right after a rescue starts a new one.
func (f *failover) startRescue(reason string) {
	f.rescueMu.Lock()
	if f.rescueRunning.Load() {
		f.rescueMu.Unlock()
		return
	}
	if f.ctx.Err() != nil {
		f.rescueMu.Unlock()
		return
	}
	f.rescueRunning.Store(true)
	done := make(chan struct{})
	f.rescueDone = done
	f.rescueMu.Unlock()

	f.wg.Add(1)
	go func() {
		defer func() {
			f.rescueMu.Lock()
			f.rescueDone = nil
			f.rescueMu.Unlock()
			f.rescueRunning.Store(false)
			close(done)
			f.wg.Done()
		}()
		f.runRescue(reason)
	}()
}

func (f *failover) runRescue(reason string) {
	started := f.timeNow()
	for attempt := 0; ; attempt++ {
		if f.ctx.Err() != nil {
			return
		}
		from := f.strategy.Now()
		if from == "" || f.strategy.Healthy(from) {
			// A monitoring sweep revived the current server while the scan was running.
			return
		}
		if f.scanOnce(from, reason, started) {
			return
		}
		f.cRescueExhausted.Add(1)
		f.logSwitchLine(from, from, reasonRescueExhausted, f.elapsedMS(started))
		backoff := rescueBackoff[min(attempt, len(rescueBackoff)-1)]
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
		if !f.strategy.ForceSelect(tag, reason, delay) {
			continue
		}
		// The explicit line below carries the elapsed time, so the events the force select
		// produced are counted but not logged again.
		f.consumeEvents(false)
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
			err = context.DeadlineExceeded
		}
		return 0, err
	}
	f.cProbesOK.Add(1)
	return delay, nil
}

// onInterfaceChange resets the stall detector and probes the current server once: a new interface
// invalidates the sockets, not necessarily the server.
func (f *failover) onInterfaceChange() {
	f.resetStalls()
	if f.ctx.Err() != nil {
		return
	}
	tag := f.strategy.Now()
	if tag == "" {
		return
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		if _, err := f.probeOnce(tag, &f.cProbesInterface); err != nil {
			if f.ctx.Err() != nil {
				return
			}
			f.reportFailure(tag, reasonNetworkChange)
		}
	}()
}

// drainEvents logs the strategy switch events as failover lines. Only TCP events are logged; UDP
// mirrors TCP and is counted only.
func (f *failover) drainEvents() { f.consumeEvents(true) }

func (f *failover) consumeEvents(log bool) {
	for _, ev := range f.strategy.Events() {
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
		ProbesActive:    f.cProbesActive.Load(),
		ProbesRescue:    f.cProbesRescue.Load(),
		ProbesInterface: f.cProbesInterface.Load(),
		ProbesOK:        f.cProbesOK.Load(),
		ProbesFailed:    f.cProbesFailed.Load(),
		Rescues:         f.cRescues.Load(),
		RescueExhausted: f.cRescueExhausted.Load(),
		Switches:        switches,
		Stalls:          f.cStalls.Load(),
	}
}

// waitIdle reports whether no rescue is running. It returns true only after the rescue goroutine
// has fully exited, which makes the tests deterministic.
func (f *failover) waitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		f.rescueMu.Lock()
		done := f.rescueDone
		f.rescueMu.Unlock()
		if done == nil {
			return true
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
