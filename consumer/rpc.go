package consumer

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/remote/rpc"
)

type rpcI interface {
	GetConsumerIDs(addr, group string, to time.Duration) ([]string, error)
	PullMessageSync(addr string, header *rpc.PullHeader, to time.Duration) (*rpc.PullResponse, error)
	SendBack(addr string, h *rpc.SendBackHeader, to time.Duration) error
	UpdateConsumerOffset(addr, topic, group string, queueID int, offset int64, to time.Duration) error
	UpdateConsumerOffsetOneway(addr, topic, group string, queueID int, offset int64) error
	QueryConsumerOffset(addr, topic, group string, queueID int, to time.Duration) (int64, *remote.RPCError)
	MaxOffset(addr, topic string, queueID uint8, to time.Duration) (int64, *remote.RPCError)
	SearchOffsetByTimestamp(addr, broker, topic string, queueID uint8, timestamp time.Time, to time.Duration) (int64, *remote.RPCError)
}
