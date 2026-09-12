package balancer

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/ntp"
)

// downloader measures one server. Production dials through that server's outbound; tests inject
// a fake. One method, because that is all the strategy needs.
type downloader interface {
	Download(ctx context.Context, tag, url string) (throughputResult, error)
}

// throughputResult is one measurement. Bytes is everything that arrived, warm-up included, and is
// what the budget is charged; Timed and Elapsed cover only the part after the warm-up threshold,
// which is what the rate is computed from.
type throughputResult struct {
	Bytes   int64
	Timed   int64
	Elapsed time.Duration
	Status  int // HTTP status, 0 if no response arrived
}

// mbps is decimal megabytes per second: 1 MB is 1,000,000 bytes, the unit link rates and data caps
// are quoted in, and the same unit the budget counts in.
func (r throughputResult) mbps() float64 {
	if r.Elapsed <= 0 || r.Timed <= 0 {
		return 0
	}
	return float64(r.Timed) / 1e6 / r.Elapsed.Seconds()
}

var errNoOutbound = errors.New("outbound not found")

// throughputFailure names the failure of a measurement, or "" when it succeeded. A transfer that
// ends early is short below the byte floor and a timeout above it; a transfer that never got a
// response is a dial error; a non-2xx answer carries its status.
func throughputFailure(res throughputResult, err error, minBytes int64) string {
	switch {
	case res.Status != 0 && (res.Status < 200 || res.Status > 299):
		return "http_" + strconv.Itoa(res.Status)
	case err != nil && res.Status == 0:
		return reasonDialError
	case res.Bytes < minBytes:
		return "short"
	case err != nil:
		return "timeout"
	default:
		return ""
	}
}

// httpDownloader runs one HTTP GET through the candidate outbound. Keep-alives are off and
// redirects are refused, so one call is one connection to one host and nothing is reused between
// candidates. Latency is never derived from this request; it keeps coming from the monitoring
// history.
type httpDownloader struct {
	lookup      func(tag string) adapter.Outbound
	timeout     time.Duration
	warmupBytes int64
	minBytes    int64
	bufSize     int
}

func newHTTPDownloader(lookup func(tag string) adapter.Outbound) *httpDownloader {
	return &httpDownloader{
		lookup:      lookup,
		timeout:     throughputTimeout,
		warmupBytes: throughputWarmupBytes,
		minBytes:    throughputMinBytes,
		bufSize:     throughputReadBuffer,
	}
}

func (d *httpDownloader) Download(ctx context.Context, tag, url string) (throughputResult, error) {
	out := d.lookup(tag)
	if out == nil {
		return throughputResult{}, errNoOutbound
	}
	// One timeout covers connect, TLS and body: a measurement that needs longer than this is
	// not a measurement worth having.
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return out.DialContext(ctx, network, M.ParseSocksaddr(addr))
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
			DisableKeepAlives: true,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return throughputResult{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return throughputResult{}, err
	}
	defer resp.Body.Close()
	res := throughputResult{Status: resp.StatusCode}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return res, nil
	}
	buf := make([]byte, d.bufSize)
	var started time.Time
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			res.Bytes += int64(n)
			switch {
			case !started.IsZero():
				res.Timed += int64(n)
				res.Elapsed = time.Since(started)
			case res.Bytes >= d.warmupBytes:
				// Timing starts here, so TCP slow start and the handshake stay
				// outside the measured window. The chunk that crossed the
				// threshold is not counted either.
				started = time.Now()
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return res, nil
			}
			return res, readErr
		}
	}
}
