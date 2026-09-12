package balancer

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/monitoring"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	tun "github.com/sagernet/sing-tun"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func RegisterLoadBalance(registry *outbound.Registry) {
	outbound.Register[option.BalancerOutboundOptions](registry, C.TypeBalancer, NewLoadBalance)
}

var (
	_ adapter.OutboundGroup           = (*Balancer)(nil)
	_ adapter.InterfaceUpdateListener = (*Balancer)(nil)
)

const (
	StrategyRoundRobin        = "round-robin"
	StrategyConsistentHashing = "consistent-hashing"
	StrategyStickySessions    = "sticky-sessions"
	StrategyLowestDelay       = "lowest-delay"
)

type Balancer struct {
	outbound.Adapter
	ctx                          context.Context
	router                       adapter.Router
	outbound                     adapter.OutboundManager
	connection                   adapter.ConnectionManager
	logger                       log.ContextLogger
	tags                         []string
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	strategyFn                   Strategy
	options                      option.BalancerOutboundOptions
	interruptExternalConnections bool

	monitor *monitoring.OutboundMonitoring

	availbleOutbounds []adapter.Outbound
	close             chan struct{}
	interruptGroup    *interrupt.Group

	// failover and stalls are nil unless the strategy is lowest-delay; every call site is
	// nil-guarded so the other strategies keep their previous behaviour.
	failover  *failover
	stalls    *stallTracker
	closeOnce sync.Once
}

// monitorProber adapts the outbound monitoring to the prober the failover controller needs.
type monitorProber struct{ m *monitoring.OutboundMonitoring }

func (p monitorProber) Probe(ctx context.Context, tag string, timeout time.Duration) (uint16, error) {
	his, err := p.m.TestAndWait(ctx, tag, timeout)
	if err != nil {
		return 0, err
	}
	return his.Delay, nil
}

func NewLoadBalance(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.BalancerOutboundOptions) (adapter.Outbound, error) {
	outbound := &Balancer{
		Adapter:                      outbound.NewAdapter(C.TypeBalancer, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		router:                       router,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		tolerance:                    options.Tolerance,
		interruptExternalConnections: options.InterruptExistConnections,
		options:                      options,
		interruptGroup:               interrupt.NewGroup(),
		close:                        make(chan struct{}),
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}

	return outbound, nil
}

func (s *Balancer) Strategy() string {
	return s.options.Strategy
}

func (s *Balancer) Start() error {
	s.monitor = monitoring.Get(s.ctx)
	s.logger.Info("starting load balance, monitoring enabled: ", s.monitor != nil)
	outbounds := make([]adapter.Outbound, 0, len(s.tags))
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		outbounds = append(outbounds, detour)
	}
	switch s.options.Strategy {
	case StrategyRoundRobin:
		s.strategyFn = NewRoundRobin(outbounds, s.options)
	case StrategyConsistentHashing:
		s.strategyFn = NewConsistentHashing(outbounds, s.options)
	case StrategyStickySessions:
		s.strategyFn = NewStickySession(outbounds, s.options)
	case StrategyLowestDelay:
		s.strategyFn = NewLowestDelay(outbounds, s.options)
	default:
		return E.New("unknown load balance strategy: ", s.options.Strategy)
	}

	if s.options.Strategy == StrategyLowestDelay {
		ld := s.strategyFn.(*LowestDelay)
		s.stalls = newStallTracker(ld.cfg, time.Now, func(tag string) {
			// reportFailure owns the Stalls counter; do not bump it here.
			s.failover.reportFailure(tag, reasonStall)
		})
		s.failover = newFailover(s.ctx, ld.cfg, ld, monitorProber{s.monitor}, s.logger, func() {
			s.interruptGroup.Interrupt(s.interruptExternalConnections)
		})
		s.failover.setResetStalls(s.stalls.reset)
		if pm := service.FromContext[pause.Manager](s.ctx); pm != nil {
			s.failover.setPaused(pm.IsDevicePaused)
		}
	}

	return nil
}

func (s *Balancer) PostStart() error {
	go s.worker()
	if s.failover != nil {
		s.failover.start()
		go s.stallLoop()
	}

	return nil
}

// stallLoop drives the stall detector. The tick is cheap: it only walks the tracker when there
// are wrapped connections.
func (s *Balancer) stallLoop() {
	ticker := time.NewTicker(stallTick)
	defer ticker.Stop()
	for {
		select {
		case <-s.close:
			return
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.stalls.size() > 0 {
				s.stalls.tick(s.strategyFn.Now())
			}
		}
	}
}

func (s *Balancer) worker() {
	observer, err := s.monitor.SubscribeGroup(s.Tag())
	if err != nil {
		s.logger.Error("failed to observe monitoring group: ", err)
		return
	}
	defer s.monitor.UnsubscribeGroup(s.Tag(), observer)

	for {
		select {
		case <-s.close:
			return

		case <-s.ctx.Done():
			return
		case _, ok := <-observer:
			if !ok {
				return
			}
			outbounds := s.monitor.OutboundsHistory(s.Tag())
			if s.strategyFn.UpdateOutboundsInfo(outbounds) {
				if s.failover != nil {
					s.failover.drainEvents()
				}
				s.interruptGroup.Interrupt(s.interruptExternalConnections)
			}

		}
	}
}
// InterfaceUpdated implements [adapter.InterfaceUpdateListener].
func (s *Balancer) InterfaceUpdated() {
	if s.failover != nil {
		s.failover.onInterfaceChange()
	}
}

func (s *Balancer) Close() error {
	s.closeOnce.Do(func() {
		// Close the channel first so worker and stallLoop leave, then stop the controller:
		// stop waits for its own goroutines, including a rescue scan in flight.
		if s.close != nil {
			close(s.close)
		}
		if s.failover != nil {
			s.failover.stop()
		}
	})
	return nil
}

func (s *Balancer) Now() string {
	if s.strategyFn == nil {
		return ""
	}
	return s.strategyFn.Now()
}

func (s *Balancer) All() []string {
	return s.tags
}

func (s *Balancer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	metadata := adapter.ContextFrom(ctx)
	if metadata == nil {
		metadata = &adapter.InboundContext{}
	}
	outbound := s.strategyFn.Select(*metadata, network, true)
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	if metadata != nil {
		metadata.SetRealOutbound(outbound.Tag())
	}

	conn, err := outbound.DialContext(ctx, network, destination)
	if err == nil {
		if s.stalls != nil {
			conn = s.stalls.wrap(conn, outbound.Tag())
		}
		return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.monitor.InvalidateTest(outbound.Tag())
	if s.failover != nil {
		s.failover.reportFailure(outbound.Tag(), "dial_error")
	}

	return nil, err
}

func (s *Balancer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	metadata := adapter.ContextFrom(ctx)
	if metadata == nil {
		metadata = &adapter.InboundContext{}
	}
	outbound := s.strategyFn.Select(*metadata, N.NetworkUDP, true)
	if outbound == nil {
		return nil, E.New("missing supported outbound")
	}
	if metadata != nil {
		metadata.SetRealOutbound(outbound.Tag())
	}

	conn, err := outbound.ListenPacket(ctx, destination)
	if err == nil {
		return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
	s.logger.ErrorContext(ctx, err)
	s.monitor.InvalidateTest(outbound.Tag())
	return nil, err
}

func (s *Balancer) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	selected := s.strategyFn.Select(metadata, metadata.Network, true)
	conn = s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx))
	if outboundHandler, isHandler := selected.(adapter.ConnectionHandlerEx); isHandler {
		outboundHandler.NewConnectionEx(ctx, conn, metadata, onClose)
	} else {
		s.connection.NewConnection(ctx, selected, conn, metadata, onClose)
	}
}

func (s *Balancer) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	selected := s.strategyFn.Select(metadata, metadata.Network, true)
	if selected == nil {
		return
	}
	metadata.SetRealOutbound(selected.Tag())
	conn = s.interruptGroup.NewSingPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx))
	if outboundHandler, isHandler := selected.(adapter.PacketConnectionHandlerEx); isHandler {
		outboundHandler.NewPacketConnectionEx(ctx, conn, metadata, onClose)
	} else {
		s.connection.NewPacketConnection(ctx, selected, conn, metadata, onClose)
	}
}

func (s *Balancer) NewDirectRouteConnection(metadata adapter.InboundContext, routeContext tun.DirectRouteContext, timeout time.Duration) (tun.DirectRouteDestination, error) {
	selected := s.strategyFn.Select(metadata, metadata.Network, true)
	if selected == nil {
		return nil, E.New(metadata.Network, " is not supported by outbound: ")
	}
	metadata.SetRealOutbound(selected.Tag())
	return selected.(adapter.DirectRouteOutbound).NewDirectRouteConnection(metadata, routeContext, timeout)
}
