package balancer

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

func TestNormalizeFailoverDefaults(t *testing.T) {
	cfg := normalizeFailover(option.BalancerOutboundOptions{})
	if cfg.tolerance != 150 || cfg.minDwell != 60*time.Second || cfg.stallTimeout != 8*time.Second ||
		cfg.stallThreshold != 3 || cfg.stallWindow != 30*time.Second || cfg.rescueBatch != 6 ||
		cfg.rescueTimeout != 5*time.Second || cfg.activeCheckInterval != 3*time.Minute {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestNormalizeFailoverOverrides(t *testing.T) {
	cfg := normalizeFailover(option.BalancerOutboundOptions{
		Tolerance:           40,
		MinDwell:            badoption.Duration(5 * time.Second),
		StallTimeout:        badoption.Duration(2 * time.Second),
		StallThreshold:      1,
		StallWindow:         badoption.Duration(10 * time.Second),
		RescueBatch:         2,
		RescueTimeout:       badoption.Duration(time.Second),
		ActiveCheckInterval: badoption.Duration(-1),
	})
	if cfg.tolerance != 40 || cfg.minDwell != 5*time.Second || cfg.stallThreshold != 1 || cfg.rescueBatch != 2 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.activeCheckInterval != 0 {
		t.Fatalf("negative active_check_interval must disable the check, got %v", cfg.activeCheckInterval)
	}
}
