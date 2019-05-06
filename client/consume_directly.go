package client

import "time"

// ConsumeDirectlyResult the flag of the consuming directly
type ConsumeDirectlyResult int

// predefined `ConsumeDirectlyResult` values
const (
	Success ConsumeDirectlyResult = iota
	Later
	Rollback
	Commit
	Error
	ReturnNil
)

// ConsumeMessageDirectlyResult consume the message directly, sending by the broker
type ConsumeMessageDirectlyResult struct {
	Order      bool
	AutoCommit bool
	remark     string
	timeCost   time.Duration
	Result     ConsumeDirectlyResult
}
