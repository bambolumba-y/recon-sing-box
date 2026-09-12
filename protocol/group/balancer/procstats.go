package balancer

import "time"

// procStats reports the process resident set size in bytes and the CPU time
// (user+system) consumed by the process since start. ok is false when the
// platform does not provide either value.
//
// readProcStats has one implementation per platform (procstats_linux.go,
// procstats_unix_other.go, procstats_windows.go, procstats_other.go),
// selected by build tags.
type procStats struct {
	rssBytes uint64
	cpu      time.Duration
	ok       bool
}
