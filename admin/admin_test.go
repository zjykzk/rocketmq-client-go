package admin

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
	"qiniu.com/dora-cloud/boots/broker/mq"
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

	assert.Equal(t, a.state, mq.StateRunning)
	assert.Equal(t, 1, a.client.AdminCount())
	assert.True(t, a.ClientID != "")
	assert.True(t, a.InstanceName != "")
	assert.True(t, a.ClientIP != "")

	rpc, client := a.rpc, a.client
	t.Run("maxoffset", func(t *testing.T) {
		testMaxoffset(a, t)
	})
	t.Run("createOrUpdateTopic", func(t *testing.T) {
		createTopicOrUpdate(a, t)
	})

	a.rpc, a.client = rpc, client
	a.Shutdown()
	assert.Equal(t, 0, a.client.AdminCount())
}

type mockRPC struct {
	createTopicErrorCount int
}

func (r *mockRPC) CreateOrUpdateTopic(addr string, header *remote.CreateOrUpdateTopicHeader, to time.Duration) error {
	if r.createTopicErrorCount == 0 {
		return nil
	}
	r.createTopicErrorCount--
	return errors.New("waiting")
}
func (r *mockRPC) DeleteTopicInBroker(addr, topic string, timeout time.Duration) error  { return nil }
func (r *mockRPC) DeleteTopicInNamesrv(addr, topic string, timeout time.Duration) error { return nil }
func (r *mockRPC) GetBrokerClusterInfo(addr string, timeout time.Duration) (*route.ClusterInfo, error) {
	return nil, nil
}
func (r *mockRPC) QueryMessageByOffset(addr string, offset int64, timeout time.Duration) (*message.MessageExt, error) {
	return nil, nil
}
func (r *mockRPC) MaxOffset(addr, topic string, queueID uint8, timeout time.Duration) (int64, *remote.RPCError) {
	return maxOffset, nil
}
func (r *mockRPC) GetConsumerIDs(addr, group string, timeout time.Duration) ([]string, error) {
	return nil, nil
}

type mockMQClient struct {
	*client.EmptyMQClient

	mockBrokerAddrs map[string]string
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

func testMaxoffset(a *Admin, t *testing.T) {
	a.rpc = &mockRPC{}
	a.client = &mockMQClient{
		mockBrokerAddrs: make(map[string]string),
	}
	offset, err := a.MaxOffset(&message.Queue{BrokerName: broker, Topic: topic, QueueID: queueID})
	assert.Nil(t, err)
	assert.Equal(t, maxOffset, offset)
}

func createTopicOrUpdate(a *Admin, t *testing.T) {
	a.rpc = &mockRPC{createTopicErrorCount: 0}
	assert.Nil(t, a.CreateOrUpdateTopic("", "", 0, 1))

	a.rpc = &mockRPC{createTopicErrorCount: 3}
	assert.Nil(t, a.CreateOrUpdateTopic("", "", 0, 1))

	a.rpc = &mockRPC{createTopicErrorCount: 6}
	assert.NotNil(t, a.CreateOrUpdateTopic("", "", 0, 1))
}
