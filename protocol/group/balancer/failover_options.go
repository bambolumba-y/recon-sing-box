package balancer

import (
	"time"

	"github.com/sagernet/sing-box/option"
)

const (
	defaultTolerance           uint16 = 150
	defaultMinDwell                   = 60 * time.Second
	defaultStallTimeout               = 8 * time.Second
	defaultStallThreshold             = 3
	defaultStallWindow                = 30 * time.Second
	defaultRescueBatch                = 6
	defaultRescueTimeout              = 5 * time.Second
	defaultActiveCheckInterval        = 3 * time.Minute
	stallTick                         = time.Second
	diagInterval                      = 15 * time.Minute
)

var rescueBackoff = []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second, 120 * time.Second}

type failoverConfig struct {
	tolerance           uint16
	minDwell            time.Duration
	stallTimeout        time.Duration
	stallThreshold      int
	stallWindow         time.Duration
	rescueBatch         int
	rescueTimeout       time.Duration
	activeCheckInterval time.Duration // 0 = disabled
}

func durationOr(v time.Duration, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	if v < 0 {
		return 0
	}
	return v
}

func normalizeFailover(o option.BalancerOutboundOptions) failoverConfig {
	cfg := failoverConfig{
		tolerance:           o.Tolerance,
		minDwell:            durationOr(o.MinDwell.Build(), defaultMinDwell),
		stallTimeout:        durationOr(o.StallTimeout.Build(), defaultStallTimeout),
		stallThreshold:      o.StallThreshold,
		stallWindow:         durationOr(o.StallWindow.Build(), defaultStallWindow),
		rescueBatch:         o.RescueBatch,
		rescueTimeout:       durationOr(o.RescueTimeout.Build(), defaultRescueTimeout),
		activeCheckInterval: durationOr(o.ActiveCheckInterval.Build(), defaultActiveCheckInterval),
	}
	if cfg.tolerance == 0 {
		cfg.tolerance = defaultTolerance
	}
	if cfg.stallThreshold <= 0 {
		cfg.stallThreshold = defaultStallThreshold
	}
	if cfg.rescueBatch <= 0 {
		cfg.rescueBatch = defaultRescueBatch
	}
	return cfg
}
