package balancer

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
)

func newLD(clock *fakeClock, tags ...string) *LowestDelay {
	s := NewLowestDelay(fakeOutbounds(tags...), option.BalancerOutboundOptions{})
	s.setClock(clock.Now)
	return s
}

func TestInitialSelectionIsProvisionalUntilMeasured(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	if s.Now() != "a" {
		t.Fatalf("initial = %q", s.Now())
	}
	// only b measured: switch without dwell
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"b": measured(300, c.Now())})
	if s.Now() != "b" {
		t.Fatalf("provisional selection must yield to the first measured server, got %q", s.Now())
	}
}

func TestNoSwitchUnderTolerance(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(200, c.Now()), "b": measured(400, c.Now())})
	c.Advance(10 * time.Minute)
	changed := s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(200, c.Now()), "b": measured(100, c.Now())})
	if changed || s.Now() != "a" {
		t.Fatalf("100 ms gain is under tolerance 150: changed=%v now=%q", changed, s.Now())
	}
}

func TestNoLatencySwitchInsideDwell(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(200, c.Now())}) // provisional -> a (lastSwitch = now)
	c.Advance(30 * time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(500, c.Now()), "b": measured(50, c.Now())})
	if s.Now() != "a" {
		t.Fatalf("dwell 60s not elapsed, got %q", s.Now())
	}
	c.Advance(31 * time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(500, c.Now()), "b": measured(50, c.Now())})
	if s.Now() != "b" {
		t.Fatalf("after dwell the better server must be selected, got %q", s.Now())
	}
	ev := s.Events()
	if len(ev) == 0 || ev[len(ev)-1].Reason != "better_latency" {
		t.Fatalf("events = %+v", ev)
	}
}

func TestFailureSwitchIgnoresDwell(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(900, c.Now())})
	switched, has := s.MarkFailed("a", "dial_error")
	if !switched || !has || s.Now() != "b" {
		t.Fatalf("switched=%v has=%v now=%q", switched, has, s.Now())
	}
	if s.Healthy("a") {
		t.Fatal("a must be unhealthy after MarkFailed")
	}
}

func TestUnknownNeverSelectedBlind(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b", "c")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now())})
	switched, has := s.MarkFailed("a", "stall")
	if switched || has || s.Now() != "a" {
		t.Fatalf("no measured candidate: must stay on a and report no candidate; switched=%v has=%v now=%q", switched, has, s.Now())
	}
	got := s.Candidates("a")
	if len(got) != 2 {
		t.Fatalf("candidates = %v", got)
	}
}

func TestCandidatesOrderMeasuredFirst(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b", "c", "d")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"a": measured(100, c.Now()), "b": measured(50, c.Now()), "c": {Time: c.Now(), Delay: 10, IsFromCache: true},
	})
	got := s.Candidates("a")
	if len(got) != 3 || got[0] != "b" || (got[1] != "c" && got[2] != "c") {
		t.Fatalf("order = %v (measured b first, cached/unknown c,d last)", got)
	}
}

func TestSuccessfulProbeClearsFailure(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	s.MarkFailed("a", "dial_error")
	c.Advance(time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(90, c.Now()), "b": measured(200, c.Now())})
	if !s.Healthy("a") {
		t.Fatal("a probe newer than the failure mark must make a healthy again")
	}
}

func TestForceSelect(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	if !s.ForceSelect("b", "rescue", 120) || s.Now() != "b" {
		t.Fatalf("force select failed, now=%q", s.Now())
	}
	if !s.Healthy("b") {
		t.Fatal("a forced tag with a delay must be healthy right away")
	}
	if s.ForceSelect("zzz", "rescue", 120) {
		t.Fatal("unknown tag must be rejected")
	}
}

func TestLatencySwitchMovesBothNetworks(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(200, c.Now()), "b": measured(400, c.Now())})
	c.Advance(61 * time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(500, c.Now()), "b": measured(50, c.Now())})
	if s.Now() != "b" {
		t.Fatalf("tcp did not switch, now=%q", s.Now())
	}
	udp := s.Select(adapter.InboundContext{}, N.NetworkUDP, false)
	if udp.Tag() != s.Now() {
		t.Fatalf("udp %q lags behind tcp %q: both networks must move in the same update", udp.Tag(), s.Now())
	}
}

func TestConfirmedProvisionalStartsDwellWindow(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	// The provisional pick a is confirmed by its own first measurement: dwell starts now.
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(200, c.Now()), "b": measured(400, c.Now())})
	c.Advance(30 * time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(500, c.Now()), "b": measured(50, c.Now())})
	if s.Now() != "a" {
		t.Fatalf("dwell must run from the confirmation, not from the zero time, got %q", s.Now())
	}
	c.Advance(31 * time.Second)
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(500, c.Now()), "b": measured(50, c.Now())})
	if s.Now() != "b" {
		t.Fatalf("after 60s the better server must be selected, got %q", s.Now())
	}
}

func TestMarkFailedOnNonCurrentTagHasNothingToRescue(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "a", "b")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"a": measured(100, c.Now()), "b": measured(200, c.Now())})
	switched, has := s.MarkFailed("b", "dial_error")
	if switched || !has || s.Now() != "a" {
		t.Fatalf("failure of a non-current tag: switched=%v has=%v now=%q", switched, has, s.Now())
	}
}
