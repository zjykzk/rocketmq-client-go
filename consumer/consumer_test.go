package consumer

import (
	"testing"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/route"

	"github.com/stretchr/testify/assert"
)

func TestSchedule(t *testing.T) {
	c := &consumer{exitChan: make(chan struct{})}

	scheduled := make(chan struct{}, 100)

	c.schedule(0, 10, func() {
		scheduled <- struct{}{}
	})

	<-scheduled
	close(c.exitChan)
	c.Wait()
}

func TestUpdateTopicSubscribe(t *testing.T) {
	c := &consumer{
		subscribeQueues: client.NewQueueTable(),
		subscribeData:   client.NewDataTable(),
		topicRouters:    route.NewTopicRouterTable(),
	}

	assert.False(t, c.NeedUpdateTopicSubscribe("topic"))

	topic := "test"

	c.UpdateTopicSubscribe(topic, nil)
	assert.Equal(t, 0, len(c.topicRouters.Routers()))

	c.subscribeData.Put(topic, &client.SubscribeData{})
	assert.True(t, c.NeedUpdateTopicSubscribe("test"))

	c.UpdateTopicSubscribe(topic, &route.TopicRouter{
		Queues: []*route.TopicQueue{
			&route.TopicQueue{ReadCount: 1, Perm: route.PermRead},
			&route.TopicQueue{Perm: route.PermWrite},
		},
	})

	assert.Equal(t, 1, len(c.subscribeQueues.Get(topic)))
	assert.NotNil(t, c.topicRouters.Get(topic))
}

func TestFindBrokerAddr(t *testing.T) {
	c := &consumer{
		topicRouters: route.NewTopicRouterTable(),
		Logger:       log.Std,
	}

	topic := "test"
	assert.Equal(t, "", c.findBrokerAddrByTopic(topic))
	c.topicRouters.Put(topic, &route.TopicRouter{
		Brokers: []*route.Broker{
			&route.Broker{
				Addresses: map[int32]string{
					rocketmq.MasterID: "abc",
				},
			},
		},
	})

	assert.Equal(t, "abc", c.findBrokerAddrByTopic(topic))
}

func TestSubscribe(t *testing.T) {
	c := &consumer{
		subscribeData:   client.NewDataTable(),
		subscribeQueues: client.NewQueueTable(),
		topicRouters:    route.NewTopicRouterTable(),
	}
	c.Subscribe("topic")
	assert.Equal(t, 1, len(c.subscribeData.Topics()))
	c.Subscribe("topic")
	assert.Equal(t, 1, len(c.subscribeData.Topics()))

	assert.Equal(t, []string{"topic"}, c.SubscribeTopics())

	c.Unsubscribe("topic")
	assert.Equal(t, 0, len(c.subscribeData.Topics()))

	assert.Equal(t, []string{}, c.SubscribeTopics())
}

func TestStart(t *testing.T) {
	c := &consumer{
		Logger: log.Std,
		Config: defaultConfig,
	}
	c.StartFunc, c.ShutdownFunc = c.start, c.shutdown

	c.NameServerAddrs = []string{"mock addr"}
	c.GroupName = "test"
	assert.Nil(t, c.Start())
	assert.NotNil(t, c.Start())
	assert.Equal(t, c.GroupName, c.Group())

	c.Shutdown()
}
