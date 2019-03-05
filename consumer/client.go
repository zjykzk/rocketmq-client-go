package consumer

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
)

type mqClient interface {
	Start() error
	Shutdown()

	RegisterConsumer(co client.Consumer) error
	UnregisterConsumer(group string)

	UpdateTopicRouterInfoFromNamesrv(topic string) error
	FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (*client.FindBrokerResult, error)

	GetConsumerIDs(addr, group string, to time.Duration) ([]string, error)
	PullMessageSync(addr string, header *rpc.PullHeader, to time.Duration) (*rpc.PullResponse, error)
	SendBack(addr string, h *rpc.SendBackHeader, to time.Duration) error
	UpdateConsumerOffset(addr, topic, group string, queueID int, offset int64, to time.Duration) error
	UpdateConsumerOffsetOneway(addr, topic, group string, queueID int, offset int64) error
	QueryConsumerOffset(addr, topic, group string, queueID int, to time.Duration) (int64, *rpc.Error)
	MaxOffset(addr, topic string, queueID uint8, to time.Duration) (int64, *rpc.Error)
	SearchOffsetByTimestamp(addr, topic string, queueID uint8, timestamp time.Time, to time.Duration) (int64, *rpc.Error)
	SendHeartbeat()
}
