package balancer

// Reasons carried by a switch event and by the diag line. One table, so the strings the
// controller emits, the strategy emits and the tests assert cannot drift apart.
//
//	initial          the provisional pick yielded to the first measured server
//	better_latency   a sweep found a server faster than the current one by more than the tolerance
//	dial_error       the outbound refused a connection
//	stall            the stall detector saw writes with no answer, and the confirmation probe failed
//	probe_failed     the active check probe of the current server failed
//	network_change   the interface changed and the probe of the current server failed
//	rescue_exhausted a full rescue scan found no reachable server
//	manual           reserved: a selection made by the user, not by the controller
//	better_throughput a full measurement round found a server faster by more than the hysteresis
const (
	reasonInitial          = "initial"
	reasonBetterLatency    = "better_latency"
	reasonDialError        = "dial_error"
	reasonStall            = "stall"
	reasonProbeFailed      = "probe_failed"
	reasonNetworkChange    = "network_change"
	reasonRescueExhausted  = "rescue_exhausted"
	reasonManual           = "manual"
	reasonBetterThroughput = "better_throughput"
)
