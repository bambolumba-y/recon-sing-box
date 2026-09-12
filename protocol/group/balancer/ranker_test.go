package balancer

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

// reverseRanker ranks the latency order backwards. It exists to prove the seam is really
// consulted: no production ranker behaves like this. It ignores the network argument, which is
// safe here because the fake outbounds of these tests support both networks.
type reverseRanker struct{ ld *LowestDelay }

func (r reverseRanker) order(exclude string) []string {
	base := r.ld.orderByLatencyLocked(exclude)
	out := make([]string, 0, len(base))
	for i := len(base) - 1; i >= 0; i-- {
		out = append(out, base[i])
	}
	return out
}

func (r reverseRanker) best(network, exclude string) (adapter.Outbound, uint16) {
	for _, tag := range r.order(exclude) {
		if !r.ld.healthyLocked(tag) {
			continue
		}
		delay, _ := r.ld.measuredLocked(tag)
		return r.ld.byTag[tag], delay
	}
	return nil, 0
}

func TestLowestDelaySatisfiesFailoverStrategy(t *testing.T) {
	var _ failoverStrategy = (*LowestDelay)(nil)
	c := newFakeClock()
	s := newLD(c, "s1", "s2")
	if s.config().minDwell != defaultMinDwell {
		t.Fatalf("config() must expose the normalised failover config, got %v", s.config().minDwell)
	}
}

func TestRankerSeamDecidesOrderAndPromotion(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "s1", "s2", "s3")
	s.setRanker(reverseRanker{s})
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"s1": measured(100, c.Now()), "s2": measured(200, c.Now()), "s3": measured(300, c.Now()),
	})
	if got := s.Candidates(""); len(got) != 3 || got[0] != "s3" {
		t.Fatalf("the installed ranker must decide the candidate order, got %v", got)
	}
	if s.Now() != "s3" {
		t.Fatalf("the installed ranker must decide the promotion, got %q", s.Now())
	}
}

func TestSinceLastSwitchMeasuresFromTheLastMove(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "s1", "s2")
	s.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{"s1": measured(100, c.Now())})
	c.Advance(90 * time.Second)
	if got := s.sinceLastSwitch(c.Now()); got != 90*time.Second {
		t.Fatalf("sinceLastSwitch = %v, want 1m30s", got)
	}
}

func TestIngestAndPromoteHealthyReproduceUpdate(t *testing.T) {
	c := newFakeClock()
	s := newLD(c, "s1", "s2")
	s.ingest(map[string]*adapter.URLTestHistory{"s2": measured(300, c.Now())})
	if !s.promoteHealthy() || s.Now() != "s2" {
		t.Fatalf("ingest + promoteHealthy must replace the provisional pick, now=%q", s.Now())
	}
}
