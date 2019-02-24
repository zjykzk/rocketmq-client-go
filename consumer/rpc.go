package consumer

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

type rpc interface {
	GetConsumerIDs(addr, group string, to time.Duration) ([]string, error)
	PullMessageSync(addr string, header *remote.PullHeader, to time.Duration) (*remote.PullResponse, error)
	SendBack(addr string, h *remote.SendBackHeader, to time.Duration) error
	UpdateConsumerOffset(addr, topic, group string, queueID int, offset int64, to time.Duration) error
	UpdateConsumerOffsetOneway(addr, topic, group string, queueID int, offset int64) error
	QueryConsumerOffset(addr, topic, group string, queueID int, to time.Duration) (int64, error)
	MaxOffset(addr, topic string, queueID uint8, to time.Duration) (int64, *remote.RPCError)
	SearchOffsetByTimestamp(addr, broker, topic string, queueID uint8, timestamp time.Time, to time.Duration) (int64, *remote.RPCError)
}
