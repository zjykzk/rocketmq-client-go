package admin

import (
	"errors"
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type fakeMQClient struct {
	fakeBrokerAddrs       map[string]string
	createTopicErrorCount int
}

func (c *fakeMQClient) AdminCount() int                    { return 0 }
func (c *fakeMQClient) RegisterAdmin(a client.Admin) error { return nil }
func (c *fakeMQClient) UnregisterAdmin(group string)       {}
func (c *fakeMQClient) FindBrokerAddr(broker string, hintID int32, lock bool) (
	*client.FindBrokerResult, error,
) {
	addr, exist := c.fakeBrokerAddrs[broker]
	if !exist {
		return nil, errors.New("not exist")
	}

	return &client.FindBrokerResult{Addr: addr}, nil
}

func (c *fakeMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	c.fakeBrokerAddrs[broker] = "fake address"
	return nil
}
func (c *fakeMQClient) CreateOrUpdateTopic(addr string, header *rpc.CreateOrUpdateTopicHeader, to time.Duration) error {
	if c.createTopicErrorCount == 0 {
		return nil
	}
	c.createTopicErrorCount--
	return errors.New("waiting")
}
func (c *fakeMQClient) DeleteTopicInBroker(addr, topic string, timeout time.Duration) error {
	return nil
}
func (c *fakeMQClient) DeleteTopicInNamesrv(addr, topic string, timeout time.Duration) error {
	return nil
}
func (c *fakeMQClient) GetBrokerClusterInfo(addr string, timeout time.Duration) (*route.ClusterInfo, error) {
	return nil, nil
}
func (c *fakeMQClient) QueryMessageByOffset(addr string, offset int64, timeout time.Duration) (*message.Ext, error) {
	return nil, nil
}
func (c *fakeMQClient) MaxOffset(addr, topic string, queueID uint8, timeout time.Duration) (int64, *rpc.Error) {
	return maxOffset, nil
}
func (c *fakeMQClient) GetConsumerIDs(addr, group string, timeout time.Duration) ([]string, error) {
	return nil, nil
}
func (c *fakeMQClient) ResetConsumeOffset(addr, topic, group string, timestamp time.Time, isForce bool, timeout time.Duration) (map[message.Queue]int64, error) {
	return nil, nil
}
func (c *fakeMQClient) ConsumeMessageDirectly(addr, group, clientID, offsetID string) (ret client.ConsumeMessageDirectlyResult, err error) {
	return
}
