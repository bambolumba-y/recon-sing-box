package balancer

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	N "github.com/sagernet/sing/common/network"
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

// stallConn carries the stall bookkeeping of one connection. It follows sing's counter
// contract (see common/bufio.CounterConn): the bookkeeping is exposed as N.CountFunc through
// UnwrapReader/UnwrapWriter and the wrapper declares itself replaceable, so bufio.Copy unwraps
// down to the socket and keeps splice, the read waiter and vectorised writes while still
// calling our counters. Intercepting Read/Write instead would hide the socket from
// N.UnwrapReader/CastReader and cost every balancer TCP connection those fast paths.
type stallConn struct {
	net.Conn
	tracker        *stallTracker
	tag            string
	lastWriteNano  atomic.Int64
	readSinceWrite atomic.Int64
	counted        atomic.Bool
	closeOnce      sync.Once
}

var (
	_ N.ReadCounter        = (*stallConn)(nil)
	_ N.WriteCounter       = (*stallConn)(nil)
	_ N.ReaderWithUpstream = (*stallConn)(nil)
	_ N.WriterWithUpstream = (*stallConn)(nil)
)

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
	t.readSeen.Store(false)
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
		if !c.counted.CompareAndSwap(false, true) {
			continue
		}
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
	// onStall is called after releasing mu (not while holding it) because it reenters this
	// tracker: onStall -> failover.reportFailure -> onSwitch -> interrupt.Group.Interrupt ->
	// stallConn.Close -> t.mu. Interrupt holds g.access for the whole walk, so holding mu
	// across onStall would be a lock cycle between t.mu and g.access, not just a re-entry.
	// It is also called after releasing mu so the
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

// countWrite is the write side of the stall bookkeeping. bufio.Copy calls it with the number
// of bytes written after an unwrapped, fast-path write; the direct Write below calls it too.
func (c *stallConn) countWrite(n int64) {
	if n <= 0 {
		return
	}
	c.lastWriteNano.Store(c.tracker.now().UnixNano())
	c.readSinceWrite.Store(0)
	c.counted.Store(false)
}

// countRead is the read side. A read means the server answered.
func (c *stallConn) countRead(n int64) {
	if n <= 0 {
		return
	}
	c.readSinceWrite.Add(n)
	// Only a read on a conn of the tag tick last saw as current means the current server is
	// alive. A read on a conn left over from a previous tag (e.g. draining after a switch)
	// must not mask a stalled current server, so it must not clear the window.
	if cur := c.tracker.currentTag.Load(); cur != nil && *cur == c.tag {
		c.tracker.readSeen.Store(true)
	}
}

// UnwrapReader implements [N.ReadCounter]: N.UnwrapCountReader collects countRead and keeps
// descending towards the socket.
func (c *stallConn) UnwrapReader() (io.Reader, []N.CountFunc) {
	return c.Conn, []N.CountFunc{c.countRead}
}

// UnwrapWriter implements [N.WriteCounter].
func (c *stallConn) UnwrapWriter() (io.Writer, []N.CountFunc) {
	return c.Conn, []N.CountFunc{c.countWrite}
}

func (c *stallConn) ReaderReplaceable() bool { return true }
func (c *stallConn) WriterReplaceable() bool { return true }

// Write keeps the direct path working for callers that never unwrap.
func (c *stallConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.countWrite(int64(n))
	return n, err
}

// Read keeps the direct path working for callers that never unwrap.
func (c *stallConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.countRead(int64(n))
	return n, err
}

func (c *stallConn) Close() error {
	// Deregister exactly once. Close can arrive twice: from the caller and from
	// interrupt.Group.Interrupt, which is itself reachable from tick via onStall.
	c.closeOnce.Do(func() {
		c.tracker.mu.Lock()
		delete(c.tracker.conns, c)
		c.tracker.mu.Unlock()
	})
	return c.Conn.Close()
}

func (c *stallConn) Upstream() any { return c.Conn }
