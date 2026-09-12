package balancer

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

func TestNormalizeThroughputDefaults(t *testing.T) {
	cfg := normalizeThroughput(option.BalancerOutboundOptions{}, 5)
	if cfg.testURL != defaultThroughputTestURL {
		t.Fatalf("testURL = %q", cfg.testURL)
	}
	if cfg.probeBytes != 3_000_000 || cfg.recheck != 2*time.Hour || cfg.budgetBytes != 100_000_000 ||
		cfg.disabled || cfg.shortlist != 3 || cfg.hysteresisPct != 25 || cfg.minDwell != 10*time.Minute {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestNormalizeThroughputClamps(t *testing.T) {
	cfg := normalizeThroughput(option.BalancerOutboundOptions{
		ThroughputProbeBytes:        1000,
		ThroughputShortlist:         99,
		ThroughputHysteresisPercent: 900,
	}, 4)
	if cfg.probeBytes != 524288 {
		t.Fatalf("probe bytes below the failure floor must be clamped up, got %d", cfg.probeBytes)
	}
	if cfg.shortlist != 4 {
		t.Fatalf("the shortlist cannot exceed the number of outbounds, got %d", cfg.shortlist)
	}
	if cfg.hysteresisPct != 500 {
		t.Fatalf("hysteresis = %d, want the 500 clamp", cfg.hysteresisPct)
	}
	if got := normalizeThroughput(option.BalancerOutboundOptions{ThroughputProbeBytes: 200_000_000}, 4).probeBytes; got != 100_000_000 {
		t.Fatalf("probe bytes = %d, want the 100000000 clamp", got)
	}
}

func TestNormalizeThroughputNegativesDisable(t *testing.T) {
	cfg := normalizeThroughput(option.BalancerOutboundOptions{
		ThroughputRecheckInterval:   badoption.Duration(-1),
		ThroughputDailyBudgetMB:     -1,
		ThroughputMinDwell:          badoption.Duration(-1),
		ThroughputHysteresisPercent: -1,
	}, 4)
	if cfg.recheck != 0 {
		t.Fatalf("a negative recheck interval must disable the periodic recheck, got %v", cfg.recheck)
	}
	if !cfg.disabled {
		t.Fatal("a negative daily budget must disable measurement entirely")
	}
	if cfg.minDwell != 0 {
		t.Fatalf("a negative min dwell becomes 0, got %v", cfg.minDwell)
	}
	if cfg.hysteresisPct != 0 {
		t.Fatalf("a negative hysteresis becomes 0, got %d", cfg.hysteresisPct)
	}
}

func TestNormalizeThroughputOverrides(t *testing.T) {
	cfg := normalizeThroughput(option.BalancerOutboundOptions{
		ThroughputTestURL:           "https://example.invalid/down",
		ThroughputProbeBytes:        5_000_000,
		ThroughputRecheckInterval:   badoption.Duration(30 * time.Minute),
		ThroughputDailyBudgetMB:     250,
		ThroughputShortlist:         2,
		ThroughputHysteresisPercent: 10,
		ThroughputMinDwell:          badoption.Duration(90 * time.Second),
	}, 6)
	if cfg.testURL != "https://example.invalid/down" || cfg.probeBytes != 5_000_000 ||
		cfg.recheck != 30*time.Minute || cfg.budgetBytes != 250_000_000 || cfg.shortlist != 2 ||
		cfg.hysteresisPct != 10 || cfg.minDwell != 90*time.Second {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestThroughputReasonAndStrategyNames(t *testing.T) {
	if reasonBetterThroughput != "better_throughput" {
		t.Fatalf("reason = %q", reasonBetterThroughput)
	}
	if StrategyThroughput != "throughput" {
		t.Fatalf("strategy = %q", StrategyThroughput)
	}
}
