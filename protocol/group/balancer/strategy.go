package balancer

import "github.com/sagernet/sing-box/adapter"

type Strategy interface {
	UpdateOutboundsInfo(outbounds map[string]*adapter.URLTestHistory) (changed bool)
	Select(metadata adapter.InboundContext, network string, touch bool) adapter.Outbound
	Now() string
}

// failoverStrategy is everything the failover controller and the Balancer wiring need from a
// strategy. LowestDelay and Throughput implement it; round-robin, consistent-hashing and
// sticky-sessions do not, which is exactly what keeps them out of the failover path.
//
// The first seven methods are what failover.go already called on *LowestDelay. config() is here
// because Balancer.Start needs the normalised failover config to build the stall tracker and the
// controller, and a type assertion on the concrete strategy is what this interface removes.
type failoverStrategy interface {
	Now() string
	IsSelected(tag string) bool
	Healthy(tag string) bool
	Candidates(exclude string) []string
	MarkFailed(tag, reason string) (switched bool, hasCandidate bool)
	ForceSelect(tag, reason string, delay uint16) bool
	Events() []switchEvent
	config() failoverConfig
}

// ranker decides which candidate is the best one and in what order the rest are tried. It
// replaces the two places where LowestDelay used to hardcode "lowest delay wins". Both methods
// are called with LowestDelay.mu held, so an implementation must not take that lock again.
type ranker interface {
	best(network, exclude string) (adapter.Outbound, uint16)
	order(exclude string) []string
}
