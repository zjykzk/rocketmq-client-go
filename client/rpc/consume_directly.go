package rpc

import "time"

// ConsumeDirectlyResult the flag of the consuming directly
type ConsumeDirectlyResult int

// ConsumeMessageDirectlyResult consume the message directly, sending by the broker
type ConsumeMessageDirectlyResult struct {
	Order      bool
	AutoCommit bool
	Remark     string
	TimeCost   time.Duration
	Result     ConsumeDirectlyResult
}
