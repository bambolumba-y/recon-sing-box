//go:build linux

package balancer

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// readProcStats reads RSS from /proc/self/statm (field 2, resident pages) and CPU time from
// getrusage. Android builds GOOS=linux, so this file covers the production target too.
func readProcStats() procStats {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return procStats{}
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return procStats{}
	}
	residentPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return procStats{}
	}

	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return procStats{}
	}

	return procStats{
		rssBytes: residentPages * uint64(os.Getpagesize()),
		cpu:      time.Duration(ru.Utime.Nano() + ru.Stime.Nano()),
		ok:       true,
	}
}
