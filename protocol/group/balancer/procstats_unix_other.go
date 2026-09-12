//go:build unix && !linux

package balancer

import (
	"runtime/metrics"
	"syscall"
	"time"
)

// readProcStats gets CPU time from getrusage, same as linux. RSS does not come from
// getrusage here: Rusage.Maxrss on darwin/bsd is a high-water mark set once and never
// lowered, not the current resident set, so it would only ever grow across the process
// lifetime. The Go runtime's own total is used instead, which under-counts non-Go memory
// (cgo, mmap'd libraries) but tracks current usage rather than a peak.
func readProcStats() procStats {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return procStats{}
	}

	sample := []metrics.Sample{{Name: "/memory/classes/total:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return procStats{}
	}

	return procStats{
		rssBytes: sample[0].Value.Uint64(),
		cpu:      time.Duration(ru.Utime.Nano() + ru.Stime.Nano()),
		ok:       true,
	}
}
