package consumer

import (
	"fmt"
	"strconv"

	"github.com/zjykzk/rocketmq-client-go/message"
)

// PullStatus pull status
type PullStatus int8

func (s PullStatus) String() string {
	if s < 0 || int(s) > len(pullStatusDescs) {
		panic("BUG:unknown status:" + strconv.Itoa(int(s)))
	}

	return pullStatusDescs[s]
}

const (
	// Found find the message
	Found PullStatus = iota
	// NoNewMessage no message in the broker
	NoNewMessage
	// NoMatchedMessage no matched with expr
	NoMatchedMessage
	// OffsetIllegal illegal offset
	OffsetIllegal
)

var pullStatusDescs = []string{"FOUND", "NO_NEW_MSG", "NO_MATCHED_MSG", "OFFSET_ILLEGAL"}

// PullResult pull result
type PullResult struct {
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	Messages        []*message.MessageExt
	Status          PullStatus
}

func (pr *PullResult) String() string {
	return fmt.Sprintf("PullResult:[NextBeginOffset=%d,MinOffset=%d,MaxOffset=%d,Messages=%v,Status=%s]",
		pr.NextBeginOffset, pr.MinOffset, pr.MaxOffset, pr.Messages, pr.Status.String())
}
