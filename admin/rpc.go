package admin

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type rpc interface {
	CreateOrUpdateTopic(addr string, header *remote.CreateOrUpdateTopicHeader, to time.Duration) error
	DeleteTopicInBroker(addr, topic string, timeout time.Duration) error
	DeleteTopicInNamesrv(addr, topic string, timeout time.Duration) error
	GetBrokerClusterInfo(addr string, timeout time.Duration) (*route.ClusterInfo, error)
	QueryMessageByOffset(addr string, offset int64, timeout time.Duration) (*message.MessageExt, error)
	MaxOffset(addr, topic string, queueID uint8, timeout time.Duration) (int64, *remote.RPCError)
	GetConsumerIDs(addr, group string, timeout time.Duration) ([]string, error)
}
