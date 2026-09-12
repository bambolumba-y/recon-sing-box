package option

import "github.com/sagernet/sing/common/json/badoption"

type BalancerOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds"`
	Tolerance                 uint16             `json:"tolerance,omitempty"` //not implemented yet
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
	Strategy                  string             `json:"strategy,omitempty"`
	DelayAcceptableRatio      float64            `json:"delay_acceptable_ratio,omitempty"`
	TTL                       badoption.Duration `json:"ttl,omitempty"`
	MaxRetry                  int                `json:"max_retry,omitempty"` //not implemented yet

	// Recon failover (lowest-delay strategy only). Zero means default, negative disables where noted.
	MinDwell            badoption.Duration `json:"min_dwell,omitempty"`
	StallTimeout        badoption.Duration `json:"stall_timeout,omitempty"`
	StallThreshold      int                `json:"stall_threshold,omitempty"`
	StallWindow         badoption.Duration `json:"stall_window,omitempty"`
	RescueBatch         int                `json:"rescue_batch,omitempty"`
	RescueTimeout       badoption.Duration `json:"rescue_timeout,omitempty"`
	ActiveCheckInterval badoption.Duration `json:"active_check_interval,omitempty"` // negative disables
}
