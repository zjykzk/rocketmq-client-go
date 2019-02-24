package producer

import (
	"fmt"
	"strconv"

	"github.com/zjykzk/rocketmq-client-go/message"
)

type SendStatus int8

const (
	OK SendStatus = iota
	FlushDiskTimeout
	FlushSlaveTimeout
	SlaveNotAvailable
)

var sendStatusDescs = []string{"OK", "flush disk timeout", "flush slave timeout", "slave not available"}

func (s SendStatus) String() string {
	if s < 0 || int(s) >= len(sendStatusDescs) {
		panic("BUG:unknow status" + strconv.FormatInt(int64(s), 10))
	}
	return sendStatusDescs[s]
}

// SendResult the send message result
type SendResult struct {
	Status        SendStatus
	UniqID        string
	QueueOffset   int64
	Queue         *message.Queue
	RegionID      string
	OffsetID      string
	TraceOn       bool
	TransactionID string
}

func (s *SendResult) String() string {
	return fmt.Sprintf("SendResult: [status=%s,UniqID="+s.UniqID+
		",QueueOffset=%d,queue=%v,RegionID="+s.RegionID+
		",OffsetID="+s.OffsetID+
		",TraceOn=%t,TransactionID="+s.TransactionID+"]",
		s.Status.String(), s.QueueOffset, s.Queue, s.TraceOn)
}
