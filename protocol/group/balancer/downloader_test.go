package balancer

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// newTestDownloader wires the production downloader to an httptest server through the package's
// fake outbound: the only thing the downloader needs from an outbound is DialContext, and the
// fake's dial hook returns a real socket to the test server.
func newTestDownloader(t *testing.T, handler http.HandlerFunc) *httpDownloader {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	out := newFakeOutbound("s1")
	out.dial = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	d := newHTTPDownloader(func(tag string) adapter.Outbound {
		if tag != "s1" {
			return nil
		}
		return out
	})
	// Small thresholds keep the test in kilobytes and milliseconds; production uses the
	// throughput* constants of throughput_options.go.
	d.timeout = 2 * time.Second
	d.warmupBytes = 4096
	d.minBytes = 8192
	return d
}

func writeBytes(w http.ResponseWriter, n int) {
	chunk := make([]byte, 4096)
	for written := 0; written < n; {
		size := min(len(chunk), n-written)
		c, err := w.Write(chunk[:size])
		if err != nil {
			return
		}
		written += c
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

func TestDownloaderMeasuresACompleteBody(t *testing.T) {
	d := newTestDownloader(t, func(w http.ResponseWriter, r *http.Request) { writeBytes(w, 65536) })
	res, err := d.Download(context.Background(), "s1", "http://test.invalid/down")
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}
	if reason := throughputFailure(res, err, d.minBytes); reason != "" {
		t.Fatalf("a complete body must not be a failure, got %q", reason)
	}
	if res.Bytes != 65536 {
		t.Fatalf("Bytes = %d, want 65536", res.Bytes)
	}
	if res.Timed <= 0 || res.Timed >= res.Bytes {
		t.Fatalf("the warm-up bytes must stay out of Timed: timed=%d bytes=%d", res.Timed, res.Bytes)
	}
	if res.Elapsed <= 0 || res.mbps() <= 0 {
		t.Fatalf("elapsed=%v mbps=%v", res.Elapsed, res.mbps())
	}
}

func TestDownloaderShortBody(t *testing.T) {
	d := newTestDownloader(t, func(w http.ResponseWriter, r *http.Request) { writeBytes(w, 5000) })
	res, err := d.Download(context.Background(), "s1", "http://test.invalid/down")
	if err != nil {
		t.Fatalf("a short but complete body is not a transport error: %v", err)
	}
	if reason := throughputFailure(res, err, d.minBytes); reason != "short" {
		t.Fatalf("reason = %q, want short (bytes=%d)", reason, res.Bytes)
	}
}

func TestDownloaderTimeoutAfterTheFloor(t *testing.T) {
	d := newTestDownloader(t, func(w http.ResponseWriter, r *http.Request) {
		writeBytes(w, 20000)
		<-r.Context().Done()
	})
	d.timeout = 300 * time.Millisecond
	res, err := d.Download(context.Background(), "s1", "http://test.invalid/down")
	if err == nil {
		t.Fatal("a body that never ends must end in an error")
	}
	if res.Bytes < d.minBytes {
		t.Fatalf("Bytes = %d, want at least the floor %d", res.Bytes, d.minBytes)
	}
	if reason := throughputFailure(res, err, d.minBytes); reason != "timeout" {
		t.Fatalf("reason = %q, want timeout", reason)
	}
}

func TestDownloaderHTTPStatus(t *testing.T) {
	d := newTestDownloader(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) })
	res, err := d.Download(context.Background(), "s1", "http://test.invalid/down")
	if err != nil {
		t.Fatalf("a 403 is an answer, not a transport error: %v", err)
	}
	if reason := throughputFailure(res, err, d.minBytes); reason != "http_403" {
		t.Fatalf("reason = %q, want http_403", reason)
	}
}

func TestDownloaderDialError(t *testing.T) {
	out := newFakeOutbound("s1")
	out.dial = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("refused")
	}
	d := newHTTPDownloader(func(tag string) adapter.Outbound { return out })
	d.timeout = time.Second
	res, err := d.Download(context.Background(), "s1", "http://test.invalid/down")
	if err == nil {
		t.Fatal("a refused dial must surface as an error")
	}
	if reason := throughputFailure(res, err, d.minBytes); reason != reasonDialError {
		t.Fatalf("reason = %q, want dial_error", reason)
	}
}

func TestDownloaderUnknownTag(t *testing.T) {
	d := newHTTPDownloader(func(tag string) adapter.Outbound { return nil })
	if _, err := d.Download(context.Background(), "s9", "http://test.invalid/down"); !errors.Is(err, errNoOutbound) {
		t.Fatalf("err = %v, want errNoOutbound", err)
	}
}
