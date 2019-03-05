package admin

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

var (
	broker    = "admin broker"
	topic     = "admin topic"
	queueID   = uint8(128)
	maxOffset = int64(110)
)

func TestAdmin(t *testing.T) {
	logger := &log.MockLogger{}
	namesrvAddrs := []string{"10.200.20.54:9988", "10.200.20.25:9988"}
	a := NewAdmin(namesrvAddrs, logger)
	assert.Nil(t, a.Start())

	assert.Equal(t, a.state, rocketmq.StateRunning)
	assert.Equal(t, 1, a.client.AdminCount())
	assert.True(t, a.ClientID != "")
	assert.True(t, a.InstanceName != "")
	assert.True(t, a.ClientIP != "")

	client := a.client
	t.Run("maxoffset", func(t *testing.T) {
		testMaxoffset(a, t)
	})
	t.Run("createOrUpdateTopic", func(t *testing.T) {
		createTopicOrUpdate(a, t)
	})

	a.client = client
	a.Shutdown()
	assert.Equal(t, 0, a.client.AdminCount())
}

type mockMQClient struct {
	*client.EmptyMQClient

	mockBrokerAddrs       map[string]string
	createTopicErrorCount int
}

func (c *mockMQClient) FindBrokerAddr(broker string, hintID int32, lock bool) (
	*client.FindBrokerResult, error,
) {
	addr, exist := c.mockBrokerAddrs[broker]
	if !exist {
		return nil, errors.New("not exist")
	}

	return &client.FindBrokerResult{Addr: addr}, nil
}

func (c *mockMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	c.mockBrokerAddrs[broker] = "mock address"
	return nil
}
func (c *mockMQClient) CreateOrUpdateTopic(addr string, header *rpc.CreateOrUpdateTopicHeader, to time.Duration) error {
	if c.createTopicErrorCount == 0 {
		return nil
	}
	c.createTopicErrorCount--
	return errors.New("waiting")
}
func (c *mockMQClient) DeleteTopicInBroker(addr, topic string, timeout time.Duration) error {
	return nil
}
func (c *mockMQClient) DeleteTopicInNamesrv(addr, topic string, timeout time.Duration) error {
	return nil
}
func (c *mockMQClient) GetBrokerClusterInfo(addr string, timeout time.Duration) (*route.ClusterInfo, error) {
	return nil, nil
}
func (c *mockMQClient) QueryMessageByOffset(addr string, offset int64, timeout time.Duration) (*message.Ext, error) {
	return nil, nil
}
func (c *mockMQClient) MaxOffset(addr, topic string, queueID uint8, timeout time.Duration) (int64, *rpc.Error) {
	return maxOffset, nil
}
func (c *mockMQClient) GetConsumerIDs(addr, group string, timeout time.Duration) ([]string, error) {
	return nil, nil
}

func testMaxoffset(a *Admin, t *testing.T) {
	a.client = &mockMQClient{
		mockBrokerAddrs: make(map[string]string),
	}
	offset, err := a.MaxOffset(&message.Queue{BrokerName: broker, Topic: topic, QueueID: queueID})
	assert.Nil(t, err)
	assert.Equal(t, maxOffset, offset)
}

func createTopicOrUpdate(a *Admin, t *testing.T) {
	a.client = &mockMQClient{createTopicErrorCount: 0}
	assert.Nil(t, a.CreateOrUpdateTopic("", "", 0, 1))

	a.client = &mockMQClient{createTopicErrorCount: 3}
	assert.Nil(t, a.CreateOrUpdateTopic("", "", 0, 1))

	a.client = &mockMQClient{createTopicErrorCount: 6}
	assert.NotNil(t, a.CreateOrUpdateTopic("", "", 0, 1))
}
