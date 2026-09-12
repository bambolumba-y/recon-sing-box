//go:build !unix && !windows

package balancer

// readProcStats has no implementation on this platform: neither RSS nor CPU time is
// available, so the diag line falls back to n/a for both fields.
func readProcStats() procStats {
	return procStats{}
}
