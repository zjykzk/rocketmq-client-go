package client

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/zjykzk/rocketmq-client-go/route"
	"qiniu.com/dora-cloud/boots/broker/utils"

	"github.com/stretchr/testify/assert"
)

func TestUnion(t *testing.T) {
	assert.Equal(t, []string{"1", "2"}, union([]string{"1"}, []string{"2"}))
	assert.Equal(t, []string{"1", "2"}, union([]string{"1", "2"}, []string{"2"}))
	assert.Equal(t, []string{"1", "2"}, union([]string{"1", "2"}, nil))
	assert.Equal(t, []string{"1", "2"}, union(nil, []string{"1", "2"}))
}

func TestMQClient(t *testing.T) {
	_, err := NewMQClient(&Config{}, "", nil)
	assert.NotNil(t, err)
	_, err = NewMQClient(&Config{}, "clientid", nil)
	assert.NotNil(t, err)

	logger := utils.CreateDefaultLogger()
	client, err := NewMQClient(&Config{NameServerAddrs: []string{"addr"}}, "clientid", logger)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(mqClients.eles))

	client1, err := NewMQClient(&Config{NameServerAddrs: []string{"addr"}}, "clientid", logger)
	assert.Equal(t, client, client1)

	client.Start()
	defer client.Shutdown()

	t.Run("[un]register consumer", func(t *testing.T) {
		assert.NotNil(t, client.RegisterConsumer(&mockConsumer{}))
		assert.Nil(t, client.RegisterConsumer(&mockConsumer{"group"}))
		assert.NotNil(t, client.RegisterConsumer(&mockConsumer{"group"}))
		assert.Equal(t, 1, client.ConsumerCount())
		assert.Nil(t, client.RegisterConsumer(&mockConsumer{"1group"}))
		assert.Equal(t, 2, client.ConsumerCount())

		client.UnregisterConsumer("group")
		assert.Equal(t, 1, client.ConsumerCount())
		client.UnregisterConsumer("1group")
		assert.Equal(t, 0, client.ConsumerCount())
	})

	t.Run("[un]register producer", func(t *testing.T) {
		assert.NotNil(t, client.RegisterProducer(&mockProducer{}))
		assert.Nil(t, client.RegisterProducer(&mockProducer{"group"}))
		assert.NotNil(t, client.RegisterProducer(&mockProducer{"group"}))
		assert.Equal(t, 1, client.ProducerCount())
		assert.Nil(t, client.RegisterProducer(&mockProducer{"1group"}))
		assert.Equal(t, 2, client.ProducerCount())

		client.UnregisterProducer("group")
		assert.Equal(t, 1, client.ProducerCount())
		client.UnregisterProducer("1group")
	})

	t.Run("[un]register admin", func(t *testing.T) {
		assert.NotNil(t, client.RegisterAdmin(&mockAdmin{}))
		assert.Nil(t, client.RegisterAdmin(&mockAdmin{"group"}))
		assert.Equal(t, 1, client.AdminCount())
		assert.Nil(t, client.RegisterAdmin(&mockAdmin{"1group"}))
		assert.Equal(t, 2, client.AdminCount())

		client.UnregisterAdmin("group")
		assert.Equal(t, 1, client.AdminCount())
		client.UnregisterAdmin("1group")
	})

	t.Run("prepare heartbeat data", func(t *testing.T) {
		err := client1.RegisterProducer(&mockProducer{"p0"})
		if err != nil {
			t.Fatal(err)
		}
		err = client1.RegisterProducer(&mockProducer{"p1"})
		mc := &mockConsumer{"c0"}
		client1.RegisterConsumer(mc)

		hd := client1.(*mqClient).prepareHeartbeatData()
		assert.Equal(t, "clientid", hd.ClientID)
		assert.Equal(t, 1, len(hd.Consumers))
		assert.Equal(t, 2, len(hd.Producers))
		if hd.Producers[0].Group == "p0" {
			assert.Equal(t, "p1", hd.Producers[1].Group)
		} else {
			assert.Equal(t, "p0", hd.Producers[1].Group)
		}
		c := hd.Consumers[0]
		assert.Equal(t, mc.group, c.Group)
		assert.Equal(t, mc.Subscriptions(), c.Subscription)
		assert.Equal(t, mc.ConsumeFromWhere(), c.FromWhere)
		assert.Equal(t, mc.Model(), c.Model)
		assert.Equal(t, mc.Type(), c.Type)
		assert.Equal(t, mc.UnitMode(), c.UnitMode)

		client1.UnregisterConsumer("c0")
		client1.UnregisterProducer("p0")
		client1.UnregisterProducer("p1")
	})

	t.Run("broker addr", func(t *testing.T) {
		impl := client.(*mqClient)
		impl.brokerAddrs.put("b1", map[int32]string{0: "master", 2: "slave"})
		impl.brokerAddrs.put("b2", map[int32]string{0: "master2", 2: "slave"})
		assert.Equal(t, "master", impl.GetMasterBrokerAddr("b1"))
		ms := impl.GetMasterBrokerAddrs()
		sort.Strings(ms)
		assert.Equal(t, []string{"master", "master2"}, ms)

		r, err := impl.FindBrokerAddr("b1", 0, false)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "master", r.Addr)
		assert.False(t, r.IsSlave)

		_, err = impl.FindBrokerAddr("b1", 3, false)
		assert.Nil(t, err)
		_, err = impl.FindBrokerAddr("b1", 3, true)
		assert.NotNil(t, err)
		_, err = impl.FindBrokerAddr("b0", 3, true)
		assert.NotNil(t, err)
	})

	t.Run("update topic router from namesrv", func(t *testing.T) {
		impl := client.(*mqClient)
		impl.rpc = &mockRPC{}

		updated, err := impl.updateTopicRouterInfoFromNamesrv("t")
		println("===")
		assert.False(t, updated)
		assert.NotNil(t, err)

		updated, err = impl.updateTopicRouterInfoFromNamesrv("t")
		assert.Nil(t, err)
		assert.True(t, updated)
		assert.Equal(t, []string{"t"}, impl.routersOfTopic.Topics())

		updated, _ = impl.updateTopicRouterInfoFromNamesrv("t")
		assert.True(t, updated)
		updated, _ = impl.updateTopicRouterInfoFromNamesrv("t")
		assert.False(t, updated)
	})
}

type mockRPC struct {
	topicRouterInfoCallCount int
}

func (r *mockRPC) GetTopicRouteInfo(addr string, topic string, to time.Duration) (
	*route.TopicRouter, error,
) {
	c := r.topicRouterInfoCallCount
	r.topicRouterInfoCallCount++

	switch c {
	case 0:
		return nil, errors.New("error")
	case 1:
		return &route.TopicRouter{}, nil
	case 2:
		fallthrough
	case 3:
		return &route.TopicRouter{
			OrderTopicConf: "ignore",
			Queues:         []*route.TopicQueue{&route.TopicQueue{}},
			Brokers:        []*route.Broker{&route.Broker{}},
		}, nil
	default:
		return nil, nil
	}
}

func (r *mockRPC) UnregisterClient(addr, clientID, pGroup, cGroup string, to time.Duration) error {
	return nil
}
