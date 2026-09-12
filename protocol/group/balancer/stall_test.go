package balancer

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"
)

func newTracker(clock *fakeClock) (*stallTracker, *[]string) {
	var fired []string
	cfg := normalizeFailover(optionWith(8*time.Second, 3, 30*time.Second))
	return newStallTracker(cfg, clock.Now, func(tag string) { fired = append(fired, tag) }), &fired
}

func TestStallCountsOnlyWrittenUnreadConns(t *testing.T) {
	clock := newFakeClock()
	tr, fired := newTracker(clock)
	conns := make([]net.Conn, 0, 3)
	peers := make([]net.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		c, peer := net.Pipe()
		conns = append(conns, tr.wrap(c, "a"))
		peers = append(peers, peer)
	}
	for _, c := range conns {
		go c.Write([]byte("x"))
	}
	for _, p := range peers {
		buf := make([]byte, 1)
		p.Read(buf) // peer consumes the write so Write returns; nothing is ever written back
	}
	time.Sleep(10 * time.Millisecond)
	clock.Advance(7 * time.Second)
	tr.tick("a")
	if len(*fired) != 0 {
		t.Fatal("under stall_timeout nothing may fire")
	}
	clock.Advance(2 * time.Second)
	tr.tick("a")
	if len(*fired) != 1 || (*fired)[0] != "a" {
		t.Fatalf("three stalled conns >= threshold 3 must fire once, got %v", *fired)
	}
	tr.tick("a")
	if len(*fired) != 1 {
		t.Fatal("a conn contributes one stall until it reads again")
	}
}

func TestReadClearsWindow(t *testing.T) {
	clock := newFakeClock()
	tr, fired := newTracker(clock)
	c1, p1 := net.Pipe()
	c2, p2 := net.Pipe()
	w1, w2 := tr.wrap(c1, "a"), tr.wrap(c2, "a")
	go w1.Write([]byte("x"))
	go w2.Write([]byte("x"))
	p1.Read(make([]byte, 1))
	p2.Read(make([]byte, 1))
	time.Sleep(10 * time.Millisecond)
	clock.Advance(9 * time.Second)
	tr.tick("a") // two stalls recorded, below threshold
	go p1.Write([]byte("y"))
	w1.Read(make([]byte, 1)) // server answered
	tr.tick("a")
	c3, p3 := net.Pipe()
	w3 := tr.wrap(c3, "a")
	go w3.Write([]byte("x"))
	p3.Read(make([]byte, 1))
	time.Sleep(10 * time.Millisecond)
	clock.Advance(9 * time.Second)
	tr.tick("a")
	if len(*fired) != 0 {
		t.Fatalf("a read must clear the window; fired=%v", *fired)
	}
}

func TestOtherTagAndClosedConnsIgnored(t *testing.T) {
	clock := newFakeClock()
	tr, fired := newTracker(clock)
	c1, p1 := net.Pipe()
	w := tr.wrap(c1, "b")
	go w.Write([]byte("x"))
	p1.Read(make([]byte, 1))
	clock.Advance(20 * time.Second)
	tr.tick("a")
	if len(*fired) != 0 || tr.size() != 1 {
		t.Fatalf("fired=%v size=%d", *fired, tr.size())
	}
	w.Close()
	if tr.size() != 0 {
		t.Fatal("closed conns must be untracked")
	}
}

// TestReadClearsWindowOnlyForCurrentTag guards against a conn left over from
// a previous server (still draining after a switch) masking a stall on the
// new current server just because it happens to receive data.
func TestReadClearsWindowOnlyForCurrentTag(t *testing.T) {
	setup := func() (tr *stallTracker, fired *[]string, clock *fakeClock, wa, pa, wb1, pb1 net.Conn) {
		clock = newFakeClock()
		var f []string
		cfg := normalizeFailover(optionWith(8*time.Second, 2, 30*time.Second))
		tr = newStallTracker(cfg, clock.Now, func(tag string) { f = append(f, tag) })
		fired = &f

		ca, pa2 := net.Pipe()
		wa = tr.wrap(ca, "a")
		pa = pa2

		cb1, pb12 := net.Pipe()
		wb1 = tr.wrap(cb1, "b")
		pb1 = pb12

		tr.tick("b") // tracker learns "b" is current before anything stalls

		go wb1.Write([]byte("x"))
		pb1.Read(make([]byte, 1))
		time.Sleep(10 * time.Millisecond)
		clock.Advance(9 * time.Second)
		tr.tick("b") // one stall recorded for "b", below threshold 2
		return
	}

	secondStall := func(tr *stallTracker, clock *fakeClock) {
		cb2, pb2 := net.Pipe()
		wb2 := tr.wrap(cb2, "b")
		go wb2.Write([]byte("x"))
		pb2.Read(make([]byte, 1))
		time.Sleep(10 * time.Millisecond)
		clock.Advance(9 * time.Second)
		tr.tick("b")
	}

	t.Run("read on a non-current tag does not clear the window", func(t *testing.T) {
		tr, fired, clock, wa, pa, _, _ := setup()
		go pa.Write([]byte("y"))
		wa.Read(make([]byte, 1)) // a read on tag "a" while "b" is current
		secondStall(tr, clock)
		if len(*fired) != 1 || (*fired)[0] != "b" {
			t.Fatalf("a read on a non-current tag must not clear the current tag's window, got %v", *fired)
		}
	})

	t.Run("read on the current tag clears the window", func(t *testing.T) {
		tr, fired, clock, _, _, wb1, pb1 := setup()
		go pb1.Write([]byte("y"))
		wb1.Read(make([]byte, 1)) // a read on tag "b" while "b" is current
		secondStall(tr, clock)
		if len(*fired) != 0 {
			t.Fatalf("a read on the current tag must clear the window, got %v", *fired)
		}
	})
}

func optionWith(timeout time.Duration, threshold int, window time.Duration) option.BalancerOutboundOptions {
	return option.BalancerOutboundOptions{
		StallTimeout: badoption.Duration(timeout), StallThreshold: threshold, StallWindow: badoption.Duration(window),
	}
}

// TestStallConnUnwrapsToSocketKeepingCounters pins the reason stallConn is built on sing's
// counter contract: bufio.Copy must be able to unwrap interrupt.Conn -> stallConn down to the
// raw socket (so splice, the read waiter and vectorised writes survive) while still collecting
// the stall bookkeeping as N.CountFunc.
func TestStallConnUnwrapsToSocketKeepingCounters(t *testing.T) {
	clock := newFakeClock()
	tr, _ := newTracker(clock)
	socket, peer := net.Pipe()
	defer socket.Close()
	defer peer.Close()
	wrapped := tr.wrap(socket, "a")
	top := interrupt.NewGroup().NewConn(wrapped, false)

	// The plain (non-counting) unwrap is what CastReader uses to find the splice path and the
	// read waiter, and it only descends through a wrapper that declares itself replaceable.
	if plain := N.UnwrapReader(top); plain != io.Reader(socket) {
		t.Fatalf("N.UnwrapReader must reach the socket, got %T", plain)
	}
	if plain := N.UnwrapWriter(top); plain != io.Writer(socket) {
		t.Fatalf("N.UnwrapWriter must reach the socket, got %T", plain)
	}

	reader, readCounters := N.UnwrapCountReader(top, nil)
	if reader != io.Reader(socket) {
		t.Fatalf("reader must unwrap to the socket, got %T", reader)
	}
	if len(readCounters) != 1 {
		t.Fatalf("the stall read counter must survive unwrapping, got %d counters", len(readCounters))
	}
	writer, writeCounters := N.UnwrapCountWriter(top, nil)
	if writer != io.Writer(socket) {
		t.Fatalf("writer must unwrap to the socket, got %T", writer)
	}
	if len(writeCounters) != 1 {
		t.Fatalf("the stall write counter must survive unwrapping, got %d counters", len(writeCounters))
	}

	// The counters returned are the stall bookkeeping, not some unrelated pair: the write
	// counter stamps the write clock and the read counter clears the stall window.
	tr.tick("a")
	writeCounters[0](4)
	clock.Advance(20 * time.Second)
	tr.tick("a")
	if tr.size() != 1 || len(tr.stalls) != 1 {
		t.Fatalf("the write counter must arm the stall clock, stalls=%d", len(tr.stalls))
	}
	readCounters[0](4)
	if !tr.readSeen.Load() {
		t.Fatal("the read counter must record that the server answered")
	}
}

// TestStallClockDrivenThroughBufioCopy is the end-to-end version: real bufio.Copy through the
// wrapper, no read back, and the stall fires.
func TestStallClockDrivenThroughBufioCopy(t *testing.T) {
	clock := newFakeClock()
	var fired []string
	cfg := normalizeFailover(optionWith(8*time.Second, 1, 30*time.Second))
	tr := newStallTracker(cfg, clock.Now, func(tag string) { fired = append(fired, tag) })

	socket, peer := net.Pipe()
	defer peer.Close()
	wrapped := tr.wrap(socket, "a")
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		io.Copy(io.Discard, peer) // the peer consumes but never answers
	}()

	if _, err := bufio.Copy(wrapped, strings.NewReader("request")); err != nil {
		t.Fatalf("copy: %v", err)
	}
	clock.Advance(9 * time.Second)
	tr.tick("a")
	if len(fired) != 1 || fired[0] != "a" {
		t.Fatalf("a write with no answer must stall through bufio.Copy, fired=%v", fired)
	}
	wrapped.Close()
	<-drained
}
