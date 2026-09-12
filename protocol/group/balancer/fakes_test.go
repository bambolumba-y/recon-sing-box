package balancer

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type fakeOutbound struct {
	outbound.Adapter
	mu   sync.Mutex
	dial func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)
}

func newFakeOutbound(tag string) *fakeOutbound {
	return &fakeOutbound{Adapter: outbound.NewAdapter("fake", tag, []string{N.NetworkTCP, N.NetworkUDP}, nil)}
}

func (f *fakeOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	f.mu.Lock()
	dial := f.dial
	f.mu.Unlock()
	if dial == nil {
		c, _ := net.Pipe()
		return c, nil
	}
	return dial(ctx, network, destination)
}

func (f *fakeOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func fakeOutbounds(tags ...string) []adapter.Outbound {
	res := make([]adapter.Outbound, 0, len(tags))
	for _, tag := range tags {
		res = append(res, newFakeOutbound(tag))
	}
	return res
}

func measured(delay uint16, at time.Time) *adapter.URLTestHistory {
	return &adapter.URLTestHistory{Time: at, Delay: delay}
}

func failedHistory(at time.Time) *adapter.URLTestHistory {
	return &adapter.URLTestHistory{Time: at, Delay: 65535}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }
