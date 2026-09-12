package balancer

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type stallTracker struct {
	cfg     failoverConfig
	now     func() time.Time
	onStall func(tag string)

	mu         sync.Mutex
	conns      map[*stallConn]struct{}
	stalls     []time.Time
	readSeen   atomic.Bool
	currentTag atomic.Pointer[string] // last tag passed to tick; nil before the first tick
}

type stallConn struct {
	net.Conn
	tracker        *stallTracker
	tag            string
	lastWriteNano  atomic.Int64
	readSinceWrite atomic.Int64
	counted        atomic.Bool
	closeOnce      sync.Once
}

func newStallTracker(cfg failoverConfig, now func() time.Time, onStall func(tag string)) *stallTracker {
	return &stallTracker{cfg: cfg, now: now, onStall: onStall, conns: map[*stallConn]struct{}{}}
}

func (t *stallTracker) wrap(conn net.Conn, tag string) net.Conn {
	c := &stallConn{Conn: conn, tracker: t, tag: tag}
	t.mu.Lock()
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	return c
}

func (t *stallTracker) size() int { t.mu.Lock(); defer t.mu.Unlock(); return len(t.conns) }

func (t *stallTracker) reset() {
	t.mu.Lock()
	t.stalls = nil
	for c := range t.conns {
		c.counted.Store(false)
	}
	t.mu.Unlock()
}

func (t *stallTracker) tick(currentTag string) {
	now := t.now()
	tag := currentTag
	t.currentTag.Store(&tag) // remember it so Read can tell whether a conn belongs to the current server
	t.mu.Lock()
	if t.readSeen.Swap(false) {
		t.stalls = nil
	}
	for c := range t.conns {
		if c.tag != currentTag {
			continue
		}
		lw := c.lastWriteNano.Load()
		if lw == 0 || c.readSinceWrite.Load() > 0 || c.counted.Load() {
			continue
		}
		if now.Sub(time.Unix(0, lw)) < t.cfg.stallTimeout {
			continue
		}
		c.counted.Store(true)
		t.stalls = append(t.stalls, now)
	}
	// trim window
	kept := t.stalls[:0]
	for _, at := range t.stalls {
		if now.Sub(at) <= t.cfg.stallWindow {
			kept = append(kept, at)
		}
	}
	t.stalls = kept
	fire := len(t.stalls) >= t.cfg.stallThreshold
	if fire {
		t.stalls = nil
	}
	t.mu.Unlock()
	// onStall is called after releasing mu (not while holding it) so the
	// failover controller's reportFailure may take its own locks, including
	// ones that call back into this tracker (e.g. reset), without deadlocking.
	// It is called synchronously, not on a spawned goroutine: dispatching it
	// via "go" here left onStall's completion unordered with respect to the
	// caller of tick, which is a real, reproducible data race against a
	// caller that inspects onStall's side effects right after tick returns
	// (confirmed with go test -race; see task-7-report.md).
	if fire {
		t.onStall(currentTag)
	}
}

func (c *stallConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.lastWriteNano.Store(c.tracker.now().UnixNano())
		c.readSinceWrite.Store(0)
		c.counted.Store(false)
	}
	return n, err
}

func (c *stallConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.readSinceWrite.Add(int64(n))
		// Only a read on a conn of the tag tick last saw as current means the
		// current server is alive. A read on a conn left over from a previous
		// tag (e.g. draining after a switch) must not mask a stalled current
		// server, so it must not clear the window.
		if cur := c.tracker.currentTag.Load(); cur != nil && *cur == c.tag {
			c.tracker.readSeen.Store(true)
		}
	}
	return n, err
}

func (c *stallConn) Close() error {
	c.closeOnce.Do(func() {
		c.tracker.mu.Lock()
		delete(c.tracker.conns, c)
		c.tracker.mu.Unlock()
	})
	return c.Conn.Close()
}

func (c *stallConn) Upstream() any { return c.Conn }
