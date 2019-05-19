package client

import (
	"errors"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/remote"

	"github.com/stretchr/testify/assert"
)

func TestUnion(t *testing.T) {
	assert.Equal(t, []string{"1", "2"}, union([]string{"1"}, []string{"2"}))
	assert.Equal(t, []string{"1", "2"}, union([]string{"1", "2"}, []string{"2"}))
	assert.Equal(t, []string{"1", "2"}, union([]string{"1", "2"}, nil))
	assert.Equal(t, []string{"1", "2"}, union(nil, []string{"1", "2"}))
}

func TestMQClient(t *testing.T) {
	_, err := New(&Config{}, "", nil)
	assert.NotNil(t, err)
	_, err = New(&Config{}, "clientid", nil)
	assert.NotNil(t, err)

	logger := log.Std
	client, err := New(&Config{NameServerAddrs: []string{"addr"}}, "clientid", logger)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(mqClients.eles))

	client1, err := New(&Config{NameServerAddrs: []string{"addr"}}, "clientid", logger)
	assert.Equal(t, client, client1)

	client.Start()
	defer client.Shutdown()

	t.Run("[un]register consumer", func(t *testing.T) {
		assert.NotNil(t, client.RegisterConsumer(&fakeConsumer{}))
		assert.Nil(t, client.RegisterConsumer(&fakeConsumer{group: "group"}))
		assert.NotNil(t, client.RegisterConsumer(&fakeConsumer{group: "group"}))
		assert.Equal(t, 1, client.ConsumerCount())
		assert.Nil(t, client.RegisterConsumer(&fakeConsumer{group: "1group"}))
		assert.Equal(t, 2, client.ConsumerCount())

		client.UnregisterConsumer("group")
		assert.Equal(t, 1, client.ConsumerCount())
		client.UnregisterConsumer("1group")
		assert.Equal(t, 0, client.ConsumerCount())
	})

	t.Run("[un]register producer", func(t *testing.T) {
		assert.NotNil(t, client.RegisterProducer(&fakeProducer{}))
		assert.Nil(t, client.RegisterProducer(&fakeProducer{"group"}))
		assert.NotNil(t, client.RegisterProducer(&fakeProducer{"group"}))
		assert.Equal(t, 1, client.ProducerCount())
		assert.Nil(t, client.RegisterProducer(&fakeProducer{"1group"}))
		assert.Equal(t, 2, client.ProducerCount())

		client.UnregisterProducer("group")
		assert.Equal(t, 1, client.ProducerCount())
		client.UnregisterProducer("1group")
	})

	t.Run("[un]register admin", func(t *testing.T) {
		assert.NotNil(t, client.RegisterAdmin(&fakeAdmin{}))
		assert.Nil(t, client.RegisterAdmin(&fakeAdmin{"group"}))
		assert.Equal(t, 1, client.AdminCount())
		assert.Nil(t, client.RegisterAdmin(&fakeAdmin{"1group"}))
		assert.Equal(t, 2, client.AdminCount())

		client.UnregisterAdmin("group")
		assert.Equal(t, 1, client.AdminCount())
		client.UnregisterAdmin("1group")
	})

	t.Run("prepare heartbeat data", func(t *testing.T) {
		err := client1.RegisterProducer(&fakeProducer{"p0"})
		if err != nil {
			t.Fatal(err)
		}
		err = client1.RegisterProducer(&fakeProducer{"p1"})
		mc := &fakeConsumer{group: "c0"}
		client1.RegisterConsumer(mc)

		hd := client1.prepareHeartbeatData()
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
		assert.Equal(t, toRPCSubscriptionDatas(mc.Subscriptions()), c.Subscription)
		assert.Equal(t, mc.ConsumeFromWhere(), c.FromWhere)
		assert.Equal(t, mc.Model(), c.Model)
		assert.Equal(t, mc.Type(), c.Type)
		assert.Equal(t, mc.UnitMode(), c.UnitMode)

		client1.UnregisterConsumer("c0")
		client1.UnregisterProducer("p0")
		client1.UnregisterProducer("p1")
	})

	t.Run("broker addr", func(t *testing.T) {
		client.brokerAddrs.put("b1", map[int32]string{0: "master", 2: "slave"})
		client.brokerAddrs.put("b2", map[int32]string{0: "master2", 2: "slave"})
		assert.Equal(t, "master", client.GetMasterBrokerAddr("b1"))
		ms := client.GetMasterBrokerAddrs()
		sort.Strings(ms)
		assert.Equal(t, []string{"master", "master2"}, ms)

		r, err := client.FindBrokerAddr("b1", 0, false)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "master", r.Addr)
		assert.False(t, r.IsSlave)

		_, err = client.FindBrokerAddr("b1", 3, false)
		assert.Nil(t, err)
		_, err = client.FindBrokerAddr("b1", 3, true)
		assert.NotNil(t, err)
		_, err = client.FindBrokerAddr("b0", 3, true)
		assert.NotNil(t, err)
	})

	t.Run("update topic router from namesrv", func(t *testing.T) {
		remoteClient := &fakeRemoteClient{}
		client.Client = remoteClient

		// bad request
		remoteClient.requestSyncErr = errors.New("bad request")
		updated, err := client.updateTopicRouterInfoFromNamesrv("t")
		assert.False(t, updated)
		assert.NotNil(t, err)
		remoteClient.requestSyncErr = nil

		// add
		remoteClient.command.Body = []byte(`{}`)
		updated, err = client.updateTopicRouterInfoFromNamesrv("t")
		assert.Nil(t, err)
		assert.True(t, updated)
		assert.Equal(t, []string{"t"}, client.routersOfTopic.Topics())

		// no updated
		updated, _ = client.updateTopicRouterInfoFromNamesrv("t")
		assert.False(t, updated)

		// change
		remoteClient.command.Body = []byte(`{"orderTopicConf":"new"}`)
		updated, _ = client.updateTopicRouterInfoFromNamesrv("t")
		assert.True(t, updated)
	})
}

func TestResetOffset(t *testing.T) {
	client, err := New(&Config{NameServerAddrs: []string{"addr"}}, "clientid", log.Std)
	if err != nil {
		t.Fatal(err)
	}

	fc := &fakeConsumer{group: "TestResetOffset"}
	err = client.RegisterConsumer(fc)
	assert.Nil(t, err)

	conn, _ := net.Pipe()
	defer conn.Close()
	ctx := &remote.ChannelContext{Conn: conn}

	// no body
	client.resetOffset(ctx, &remote.Command{})
	assert.False(t, fc.runResetOffset)

	// bad offsets
	client.resetOffset(ctx, &remote.Command{Body: []byte("}")})
	assert.False(t, fc.runResetOffset)

	// no consumer
	fc.resetOffsetErr = errors.New("reset failed")
	client.resetOffset(ctx, &remote.Command{Body: []byte("{}")})
	assert.False(t, fc.runResetOffset)

	// OK
	client.resetOffset(ctx, &remote.Command{
		Body:      []byte("{}"),
		ExtFields: map[string]string{"group": fc.group, "topic": "TestResetOffsetTopic"},
	})
	assert.True(t, fc.runResetOffset)
	assert.Equal(t, "TestResetOffsetTopic", fc.resetTopicOfOffset)
}

func TestConsumeMessageDirectly(t *testing.T) {
	client, err := New(&Config{NameServerAddrs: []string{"addr"}}, "clientid", log.Std)
	if err != nil {
		t.Fatal(err)
	}

	fc := &fakeConsumer{group: "TestConsumeMessageDirectly"}
	err = client.RegisterConsumer(fc)
	assert.Nil(t, err)

	conn, _ := net.Pipe()
	ctx := &remote.ChannelContext{Conn: conn}

	// decode error
	resp, err := client.consumeMessageDirectly(ctx, remote.NewCommandWithBody(0, nil, []byte{'1'}))
	assert.NotNil(t, err)

	// no message
	resp, err = client.consumeMessageDirectly(ctx, remote.NewCommandWithBody(0, nil, nil))
	assert.Equal(t, 0, strings.Index(err.Error(), "bad message count"))

	//
	// data is the result of encoding message following it
	//
	//
	// message.Ext{
	//  	Message:   message.Message{Topic: "TestConsumeDirectly"},
	//  	MsgID:     "id",
	//  	BornHost:  message.Addr{Port: 9090},
	//  	StoreHost: message.Addr{Port: 9090},
	// }

	data := []byte{
		0, 0, 0, 110, 218, 163, 32, 167, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 35, 130, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 35, 130, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 84, 101, 115, 116, 67, 111, 110, 115, 117, 109, 101, 68, 105, 114, 101, 99, 116, 108, 121, 0, 0,
	}
	// no consumer
	resp, err = client.consumeMessageDirectly(ctx, remote.NewCommandWithBody(0, nil, data))
	assert.Nil(t, err)
	assert.Equal(t, int(rpc.SystemError), int(resp.Code))
	assert.Equal(t, 0, strings.Index(resp.Remark, "The Consumer Group"))

	cmd := remote.NewCommandWithBody(0, nil, data)
	cmd.ExtFields = map[string]string{"consumerGroup": "TestConsumeMessageDirectly"}
}
