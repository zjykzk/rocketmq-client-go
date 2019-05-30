package admin

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

var (
	broker    = "admin broker"
	topic     = "admin topic"
	queueID   = uint8(128)
	maxOffset = int64(110)
)

func TestAdmin(t *testing.T) {
	logger := log.Std
	namesrvAddrs := []string{"10.200.20.54:9988", "10.200.20.25:9988"}
	a := NewAdmin(namesrvAddrs, logger)
	assert.Nil(t, a.Start())

	assert.Equal(t, a.State, rocketmq.StateRunning)
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

func testMaxoffset(a *Admin, t *testing.T) {
	a.client = &fakeMQClient{
		fakeBrokerAddrs: make(map[string]string),
	}
	offset, err := a.MaxOffset(&message.Queue{BrokerName: broker, Topic: topic, QueueID: queueID})
	assert.Nil(t, err)
	assert.Equal(t, maxOffset, offset)
}

func createTopicOrUpdate(a *Admin, t *testing.T) {
	a.client = &fakeMQClient{
		fakeBrokerAddrs: make(map[string]string),
	}
	err := a.CreateOrUpdateTopic("Topic", 6, 16)
	assert.Nil(t, err)
}
