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
	rk          ranker
	mu          sync.Mutex
}

var _ Strategy = (*LowestDelay)(nil)

func NewLowestDelay(outbounds []adapter.Outbound, options option.BalancerOutboundOptions) *LowestDelay {
	couts := convertOutbounds(outbounds)
	byTag := make(map[string]adapter.Outbound, len(outbounds))
	for _, o := range outbounds {
		byTag[o.Tag()] = o
	}
	s := &LowestDelay{
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
	s.rk = latencyRanker{s}
	return s
}

// setRanker installs the candidate ordering. Throughput replaces the default latency ranker with
// one that puts measured bandwidth first.
func (s *LowestDelay) setRanker(r ranker) { s.mu.Lock(); s.rk = r; s.mu.Unlock() }

// lock and unlock expose the strategy lock to the throughput value table, which lives in another
// type but must be read by the ranker while this lock is already held. One lock, because two
// would be a lock-order cycle.
func (s *LowestDelay) lock()   { s.mu.Lock() }
func (s *LowestDelay) unlock() { s.mu.Unlock() }

// config returns the normalised failover config. It is immutable after construction.
func (s *LowestDelay) config() failoverConfig { return s.cfg }

func (s *LowestDelay) outboundByTag(tag string) adapter.Outbound {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byTag[tag]
}

// latencyRanker is the default ranker: the behaviour LowestDelay had before the seam existed.
type latencyRanker struct{ ld *LowestDelay }

func (r latencyRanker) best(network, exclude string) (adapter.Outbound, uint16) {
	return r.ld.bestByLatencyLocked(network, exclude)
}

func (r latencyRanker) order(exclude string) []string {
	return r.ld.orderByLatencyLocked(exclude)
}

func (s *LowestDelay) setClock(now func() time.Time) { s.mu.Lock(); s.now = now; s.mu.Unlock() }

func (s *LowestDelay) Now() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentLocked()
}

// currentLocked is the TCP selection. The caller holds s.mu.
func (s *LowestDelay) currentLocked() string {
	cur := s.selected[N.NetworkTCP]
	if cur == nil {
		return ""
	}
	return cur.Tag()
}

// IsSelected reports whether tag is the current selection for TCP or for UDP. A failure on a tag
// that is current for UDP alone is still a failure of a server in use, so it must not be dropped
// just because Now() (which is TCP) names someone else.
func (s *LowestDelay) IsSelected(tag string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		if cur := s.selected[network]; cur != nil && cur.Tag() == tag {
			return true
		}
	}
	return false
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
	if _, ok := s.measuredLocked(tag); !ok {
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

// bestLocked asks the installed ranker for the best candidate.
func (s *LowestDelay) bestLocked(network, exclude string) (adapter.Outbound, uint16) {
	return s.rk.best(network, exclude)
}

// bestByLatencyLocked returns the healthy outbound with the lowest delay for network, excluding
// tag. This is the body bestLocked used to have.
func (s *LowestDelay) bestByLatencyLocked(network, exclude string) (adapter.Outbound, uint16) {
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

// ingest copies the monitoring history into the strategy.
func (s *LowestDelay) ingest(history map[string]*adapter.URLTestHistory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestLocked(history)
}

func (s *LowestDelay) ingestLocked(history map[string]*adapter.URLTestHistory) {
	for tag, h := range history {
		if h != nil {
			copyH := *h
			s.history[tag] = &copyH
		}
	}
}

// promoteHealthy replaces a provisional or unhealthy selection with the ranker's best candidate,
// for both networks. It is the arm that must run for every strategy: a dead or unmeasured server
// is never kept.
func (s *LowestDelay) promoteHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.promoteHealthyLocked(s.now(), s.provisional)
}

// promoteHealthyLocked takes wasProvisional as a snapshot: the TCP iteration clears the flag and
// UDP must still see the state the update started from.
func (s *LowestDelay) promoteHealthyLocked(now time.Time, wasProvisional bool) bool {
	changed := false
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		cur := s.selected[network]
		if cur != nil && !wasProvisional && s.healthyLocked(cur.Tag()) {
			continue
		}
		best, _ := s.bestLocked(network, "")
		if best == nil {
			continue
		}
		if cur == nil || best.Tag() != cur.Tag() {
			reason := reasonProbeFailed
			if wasProvisional {
				reason = reasonInitial
			}
			s.switchLocked(network, best, reason)
			changed = true
		} else if network == N.NetworkTCP {
			// The provisional pick turned out to be the best measured server: it stops
			// being provisional and starts the dwell window from now.
			s.provisional = false
			s.lastSwitch = now
		}
	}
	return changed
}

func (s *LowestDelay) sinceLastSwitch(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sinceLastSwitchLocked(now)
}

func (s *LowestDelay) sinceLastSwitchLocked(now time.Time) time.Duration {
	return now.Sub(s.lastSwitch)
}

func (s *LowestDelay) UpdateOutboundsInfo(history map[string]*adapter.URLTestHistory) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestLocked(history)
	now := s.now()
	// The dwell decision is taken once for the whole update: switchLocked moves lastSwitch on
	// the TCP iteration, and without a snapshot UDP would stay behind for a full dwell.
	dwellOK := s.sinceLastSwitchLocked(now) >= s.cfg.minDwell
	wasProvisional := s.provisional
	changed := s.promoteHealthyLocked(now, wasProvisional)
	if wasProvisional {
		// Every network took the promote arm; the tolerance arm has nothing to add.
		return changed
	}
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		cur := s.selected[network]
		if cur == nil || !s.healthyLocked(cur.Tag()) {
			// Handled by promoteHealthyLocked, or nothing healthy exists to move to.
			continue
		}
		best, bestDelay := s.bestLocked(network, "")
		if best == nil {
			continue
		}
		curDelay, _ := s.measuredLocked(cur.Tag())
		if uint32(bestDelay)+uint32(s.cfg.tolerance) < uint32(curDelay) && dwellOK {
			s.switchLocked(network, best, reasonBetterLatency)
			changed = true
		}
	}
	return changed
}

// MarkFailed records a failure signal for tag. Three outcomes:
//   - (true, true)   tag was the current selection and was replaced by a healthy candidate;
//   - (false, false) tag was the current selection and nothing healthy is left: start a rescue scan;
//   - (false, true)  tag was not the current selection, so there is nothing to rescue.
func (s *LowestDelay) MarkFailed(tag, reason string) (switched bool, hasCandidate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedAt[tag] = s.now()
	current := false
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		cur := s.selected[network]
		if cur == nil || cur.Tag() != tag {
			continue
		}
		current = true
		best, _ := s.bestLocked(network, tag)
		if best == nil {
			continue
		}
		hasCandidate = true
		s.switchLocked(network, best, reason)
		switched = true
	}
	if !current {
		return false, true
	}
	return switched, hasCandidate
}

// ForceSelect selects tag for both networks (rescue result or manual pick). The failure mark is
// cleared; a delay > 0 also stores it as a fresh measurement, so the tag is healthy right away and
// the next UpdateOutboundsInfo does not move off it. A delay of 0 only clears the mark.
func (s *LowestDelay) ForceSelect(tag, reason string, delay uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.byTag[tag]
	if !ok {
		return false
	}
	delete(s.failedAt, tag)
	if delay > 0 {
		s.history[tag] = &adapter.URLTestHistory{Time: s.now(), Delay: delay}
	}
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		s.switchLocked(network, o, reason)
	}
	return true
}

// Candidates lists all tags except exclude in the order the installed ranker decides.
func (s *LowestDelay) Candidates(exclude string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rk.order(exclude)
}

// orderByLatency is Candidates with the latency order forced, whatever ranker is installed. The
// throughput shortlist uses it: measuring only the servers that already have the best values
// would never discover a faster one.
func (s *LowestDelay) orderByLatency(exclude string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orderByLatencyLocked(exclude)
}

// orderByLatencyLocked lists all tags except exclude: healthy measured first by delay, then
// unknown or failed in configuration order. This is the body Candidates used to have.
func (s *LowestDelay) orderByLatencyLocked(exclude string) []string {
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
