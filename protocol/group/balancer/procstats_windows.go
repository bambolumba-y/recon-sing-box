//go:build windows

package balancer

import (
	"runtime/metrics"
	"syscall"
	"time"
)

// readProcStats gets CPU time from GetProcessTimes. RSS comes from runtime/metrics rather
// than golang.org/x/sys/windows.GetProcessMemoryInfo: that function wraps a psapi.dll call
// x/sys does not currently expose, so this falls back to the Go runtime's own total, which
// under-counts non-Go memory (cgo, mmap'd libraries).
func readProcStats() procStats {
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return procStats{}
	}
	var creation, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return procStats{}
	}

	sample := []metrics.Sample{{Name: "/memory/classes/total:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return procStats{}
	}

	return procStats{
		rssBytes: sample[0].Value.Uint64(),
		cpu:      filetimeDuration(kernel) + filetimeDuration(user),
		ok:       true,
	}
}

// filetimeDuration converts a FILETIME that holds an elapsed time (not a timestamp), such as
// the kernel/user time from GetProcessTimes, to a time.Duration. Filetime.Nanoseconds instead
// treats its argument as an absolute time and subtracts the Windows epoch, which is wrong here.
func filetimeDuration(ft syscall.Filetime) time.Duration {
	hundredNsUnits := uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime)
	return time.Duration(hundredNsUnits * 100)
}
