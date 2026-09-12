package balancer

import (
	"sort"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/monitoring"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
)

// switchEvent describes one selection change, drained by the failover controller.
type switchEvent struct {
	Network, From, To, Reason string
}

type LowestDelay struct {
	outbounds   map[string][]adapter.Outbound
	byTag       map[string]adapter.Outbound
	selected    map[string]adapter.Outbound
	provisional bool
	cfg         failoverConfig
	now         func() time.Time
	lastSwitch  time.Time
	history     map[string]*adapter.URLTestHistory
	failedAt    map[string]time.Time
	events      []switchEvent
	mu          sync.Mutex
}

var _ Strategy = (*LowestDelay)(nil)

func NewLowestDelay(outbounds []adapter.Outbound, options option.BalancerOutboundOptions) *LowestDelay {
	couts := convertOutbounds(outbounds)
	byTag := make(map[string]adapter.Outbound, len(outbounds))
	for _, o := range outbounds {
		byTag[o.Tag()] = o
	}
	return &LowestDelay{
		outbounds: couts,
		byTag:     byTag,
		selected: map[string]adapter.Outbound{
			N.NetworkUDP: couts[N.NetworkUDP][0],
			N.NetworkTCP: couts[N.NetworkTCP][0],
		},
		provisional: true,
		cfg:         normalizeFailover(options),
		now:         time.Now,
		history:     map[string]*adapter.URLTestHistory{},
		failedAt:    map[string]time.Time{},
	}
}

func (s *LowestDelay) setClock(now func() time.Time) { s.mu.Lock(); s.now = now; s.mu.Unlock() }

func (s *LowestDelay) Now() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selected[N.NetworkTCP].Tag()
}

func (s *LowestDelay) Select(metadata adapter.InboundContext, network string, touch bool) adapter.Outbound {
	s.mu.Lock()
	defer s.mu.Unlock()
	if network != N.NetworkTCP && network != N.NetworkUDP {
		network = N.NetworkTCP
	}
	return s.selected[network]
}

// Events returns the switch events accumulated since the last call and clears the buffer.
func (s *LowestDelay) Events() []switchEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev := s.events
	s.events = nil
	return ev
}

// measuredLocked: latest probe succeeded and is not a cache entry.
func (s *LowestDelay) measuredLocked(tag string) (uint16, bool) {
	h := s.history[tag]
	if h == nil || h.IsFromCache || h.Delay == 0 || h.Delay >= monitoring.TimeoutDelay {
		return 0, false
	}
	return h.Delay, true
}

func (s *LowestDelay) healthyLocked(tag string) bool {
	d, ok := s.measuredLocked(tag)
	if !ok || d == 0 {
		return false
	}
	if at, failed := s.failedAt[tag]; failed && !s.history[tag].Time.After(at) {
		return false
	}
	return true
}

func (s *LowestDelay) Healthy(tag string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthyLocked(tag)
}

// bestLocked returns the healthy outbound with the lowest delay for network, excluding tag.
func (s *LowestDelay) bestLocked(network, exclude string) (adapter.Outbound, uint16) {
	var best adapter.Outbound
	bestDelay := monitoring.TimeoutDelay
	for _, o := range s.outbounds[network] {
		if o.Tag() == exclude || !s.healthyLocked(o.Tag()) {
			continue
		}
		d, _ := s.measuredLocked(o.Tag())
		if best == nil || d < bestDelay {
			best, bestDelay = o, d
		}
	}
	return best, bestDelay
}

func (s *LowestDelay) switchLocked(network string, to adapter.Outbound, reason string) {
	from := s.selected[network]
	if from != nil && from.Tag() == to.Tag() {
		return
	}
	s.selected[network] = to
	fromTag := ""
	if from != nil {
		fromTag = from.Tag()
	}
	s.events = append(s.events, switchEvent{Network: network, From: fromTag, To: to.Tag(), Reason: reason})
	if network == N.NetworkTCP {
		s.lastSwitch = s.now()
		s.provisional = false
	}
}

func (s *LowestDelay) UpdateOutboundsInfo(history map[string]*adapter.URLTestHistory) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for tag, h := range history {
		if h != nil {
			copyH := *h
			s.history[tag] = &copyH
		}
	}
	changed := false
	now := s.now()
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		cur := s.selected[network]
		best, bestDelay := s.bestLocked(network, "")
		if best == nil {
			continue
		}
		switch {
		case s.provisional || !s.healthyLocked(cur.Tag()):
			if best.Tag() != cur.Tag() {
				reason := "probe_failed"
				if s.provisional {
					reason = "first_measurement"
				}
				s.switchLocked(network, best, reason)
				changed = true
			} else if network == N.NetworkTCP {
				// The provisional pick turned out to be the best measured server: it stops
				// being provisional and starts the dwell window from now.
				s.provisional = false
				s.lastSwitch = now
			}
		default:
			curDelay, _ := s.measuredLocked(cur.Tag())
			if uint32(bestDelay)+uint32(s.cfg.tolerance) < uint32(curDelay) && now.Sub(s.lastSwitch) >= s.cfg.minDwell {
				s.switchLocked(network, best, "better_latency")
				changed = true
			}
		}
	}
	return changed
}

// MarkFailed records a failure signal for tag. If tag is the current selection, it switches to the best
// healthy candidate right away. hasCandidate=false tells the caller to start a rescue scan.
func (s *LowestDelay) MarkFailed(tag, reason string) (switched bool, hasCandidate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedAt[tag] = s.now()
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		cur := s.selected[network]
		if cur == nil || cur.Tag() != tag {
			continue
		}
		best, _ := s.bestLocked(network, tag)
		if best == nil {
			continue
		}
		hasCandidate = true
		s.switchLocked(network, best, reason)
		switched = true
	}
	return switched, hasCandidate
}

// ForceSelect selects tag for both networks (rescue result or manual pick). The tag is treated as alive.
func (s *LowestDelay) ForceSelect(tag, reason string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byTag[tag]
	if !ok {
		return false
	}
	delete(s.failedAt, tag)
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		s.switchLocked(network, o, reason)
	}
	return true
}

// Candidates lists all tags except exclude: healthy measured first by delay, then unknown/failed in
// configuration order.
func (s *LowestDelay) Candidates(exclude string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var measuredTags, unknownTags []string
	delays := map[string]uint16{}
	for _, o := range s.outbounds[N.NetworkTCP] {
		tag := o.Tag()
		if tag == exclude {
			continue
		}
		if d, ok := s.measuredLocked(tag); ok && s.healthyLocked(tag) {
			measuredTags = append(measuredTags, tag)
			delays[tag] = d
		} else {
			unknownTags = append(unknownTags, tag)
		}
	}
	sort.SliceStable(measuredTags, func(i, j int) bool { return delays[measuredTags[i]] < delays[measuredTags[j]] })
	return append(measuredTags, unknownTags...)
}
