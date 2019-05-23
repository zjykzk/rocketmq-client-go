package admin

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type mqClient interface {
	RegisterAdmin(a client.Admin) error
	UnregisterAdmin(group string)
	AdminCount() int

	CreateOrUpdateTopic(addr string, header *rpc.CreateOrUpdateTopicHeader, to time.Duration) error
	DeleteTopicInBroker(addr, topic string, timeout time.Duration) error
	DeleteTopicInNamesrv(addr, topic string, timeout time.Duration) error
	GetBrokerClusterInfo(addr string, timeout time.Duration) (*route.ClusterInfo, error)
	QueryMessageByOffset(addr string, offset int64, timeout time.Duration) (*message.Ext, error)
	MaxOffset(addr, topic string, queueID uint8, timeout time.Duration) (int64, *rpc.Error)
	GetConsumerIDs(addr, group string, timeout time.Duration) ([]string, error)

	UpdateTopicRouterInfoFromNamesrv(topic string) error
	FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (*client.FindBrokerResult, error)
	ResetConsumeOffset(addr, topic, group string, timestamp time.Time, isForce bool, timeout time.Duration) (map[message.Queue]int64, error)
	ConsumeMessageDirectly(addr, group, clientID, offsetID string) (client.ConsumeMessageDirectlyResult, error)
}
