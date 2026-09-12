package balancer

import (
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

type scriptConn struct {
	net.Conn
	readCh chan []byte
}

func newScriptConn() (*scriptConn, net.Conn) {
	a, b := net.Pipe()
	return &scriptConn{Conn: a}, b
}

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
