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

var consumeDirectlyResultStrings = []string{"Success", "Later", "Rollback", "Commit", "Error"}

func (r ConsumeDirectlyResult) String() string {
	if int(r) >= len(consumeDirectlyResultStrings) || r < 0 {
		return "UNKNOW ConsumeDirectlyResult"
	}
	return consumeDirectlyResultStrings[r]
}

// ConsumeMessageDirectlyResult consume the message directly, sending by the broker
type ConsumeMessageDirectlyResult struct {
	Order      bool
	AutoCommit bool
	Remark     string
	TimeCost   time.Duration
	Result     ConsumeDirectlyResult
}
