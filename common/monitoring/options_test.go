package monitoring

import (
	"context"
	"testing"
	"time"

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
