package balancer

import (
	"time"

	"github.com/sagernet/sing-box/option"
)

const (
	defaultThroughputTestURL                 = "https://speed.cloudflare.com/__down?bytes=3000000"
	defaultThroughputProbeBytes        int64 = 3_000_000
	defaultThroughputRecheck                 = 2 * time.Hour
	defaultThroughputDailyBudgetMB     int64 = 100
	defaultThroughputShortlist               = 3
	defaultThroughputHysteresisPercent       = 25
	defaultThroughputMinDwell                = 10 * time.Minute

	// minThroughputProbeBytes is the failure floor as well: below it the "fewer than 524288
	// bytes is a failed measurement" rule could never be satisfied.
	minThroughputProbeBytes        int64 = 524_288
	maxThroughputProbeBytes        int64 = 100_000_000
	maxThroughputHysteresisPercent       = 500

	// Measurement shape. MB is decimal, 1,000,000 bytes, the unit link rates and data caps are
	// quoted in; the budget uses the same unit.
	throughputTimeout              = 8 * time.Second
	throughputWarmupBytes    int64 = 262_144
	throughputMinBytes       int64 = 524_288
	throughputReadBuffer           = 32 * 1024
	throughputTick                 = 30 * time.Second
	throughputRoundFloor           = 5 * time.Minute
	throughputGateTick             = time.Second
	throughputFailBackoffMin       = 10 * time.Minute
	throughputFailBackoffMax       = 2 * time.Hour
)

type throughputConfig struct {
	testURL       string
	probeBytes    int64
	recheck       time.Duration // 0 = no periodic recheck
	budgetBytes   int64
	disabled      bool // no measurement at all; the strategy ranks by latency
	shortlist     int
	hysteresisPct int
	minDwell      time.Duration
}

// normalizeThroughput follows the convention of normalizeFailover: zero means default, negative
// disables where the option table says so. outbounds is the size of the pool, which caps the
// shortlist.
func normalizeThroughput(o option.BalancerOutboundOptions, outbounds int) throughputConfig {
	cfg := throughputConfig{
		testURL:       o.ThroughputTestURL,
		probeBytes:    o.ThroughputProbeBytes,
		recheck:       durationOr(o.ThroughputRecheckInterval.Build(), defaultThroughputRecheck),
		shortlist:     o.ThroughputShortlist,
		hysteresisPct: o.ThroughputHysteresisPercent,
		minDwell:      durationOr(o.ThroughputMinDwell.Build(), defaultThroughputMinDwell),
	}
	if cfg.testURL == "" {
		cfg.testURL = defaultThroughputTestURL
	}
	if cfg.probeBytes == 0 {
		cfg.probeBytes = defaultThroughputProbeBytes
	}
	if cfg.probeBytes < minThroughputProbeBytes {
		cfg.probeBytes = minThroughputProbeBytes
	}
	if cfg.probeBytes > maxThroughputProbeBytes {
		cfg.probeBytes = maxThroughputProbeBytes
	}
	switch {
	case o.ThroughputDailyBudgetMB < 0:
		cfg.disabled = true
	case o.ThroughputDailyBudgetMB == 0:
		cfg.budgetBytes = defaultThroughputDailyBudgetMB * 1_000_000
	default:
		cfg.budgetBytes = o.ThroughputDailyBudgetMB * 1_000_000
	}
	if cfg.shortlist <= 0 {
		cfg.shortlist = defaultThroughputShortlist
	}
	if outbounds > 0 && cfg.shortlist > outbounds {
		cfg.shortlist = outbounds
	}
	// Zero is "not set" for every option here, so it takes the default; a caller that really
	// wants to switch on any improvement asks for it with a negative value.
	switch {
	case cfg.hysteresisPct == 0:
		cfg.hysteresisPct = defaultThroughputHysteresisPercent
	case cfg.hysteresisPct < 0:
		cfg.hysteresisPct = 0
	case cfg.hysteresisPct > maxThroughputHysteresisPercent:
		cfg.hysteresisPct = maxThroughputHysteresisPercent
	}
	return cfg
}
