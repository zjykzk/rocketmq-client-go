package producer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/remote/rpc"
	"github.com/zjykzk/rocketmq-client-go/route"
)

func TestSendHeader(t *testing.T) {
	p := NewProducer("sendHeader", []string{"abc"}, &log.MockLogger{})
	p.CreateTopicKey, p.DefaultTopicQueueNums = "CreateTopicKey", 100
	m := &message.Message{Topic: "sendHeader", Flag: 111, Properties: map[string]string{
		message.PropertyReconsumeTime:     "100",
		message.PropertyMaxReconsumeTimes: "120",
	}}
	q := &message.Queue{Topic: "sendHeader", BrokerName: "b", QueueID: 12}
	header := p.buildSendHeader(m, q, 123)

	assert.Equal(t, header.Group, p.Group())
	assert.Equal(t, header.Topic, m.Topic)
	assert.Equal(t, header.DefaultTopic, p.CreateTopicKey)
	assert.Equal(t, header.DefaultTopicQueueNums, p.DefaultTopicQueueNums)
	assert.Equal(t, header.QueueID, q.QueueID)
	assert.Equal(t, header.SysFlag, int32(123))
	assert.True(t, header.BornTimestamp > 0)
	assert.Equal(t, header.Flag, m.Flag)
	assert.True(t, header.Properties != "")
	assert.Equal(t, header.UnitMode, p.IsUnitMode)
	assert.Equal(t, header.Batch, false)
	assert.Equal(t, header.ReconsumeTimes, int32(0))
	assert.Equal(t, header.MaxReconsumeTimes, int32(0))
	assert.Equal(t, "100", m.Properties[message.PropertyReconsumeTime])
	assert.Equal(t, "120", m.Properties[message.PropertyMaxReconsumeTimes])

	m.Topic = rocketmq.RetryGroupTopicPrefix + p.GroupName
	header = p.buildSendHeader(m, q, 123)
	assert.Equal(t, header.ReconsumeTimes, int32(100))
	assert.Equal(t, header.MaxReconsumeTimes, int32(120))
	assert.Equal(t, "", m.Properties[message.PropertyReconsumeTime])
	assert.Equal(t, "", m.Properties[message.PropertyMaxReconsumeTimes])
}

type mockRemoteClient struct {
	*remote.MockClient

	requestSyncErr error
	command        remote.Command
}

func (m *mockRemoteClient) RequestSync(
	addr string, cmd *remote.Command, timeout time.Duration,
) (
	*remote.Command, error,
) {
	return &m.command, m.requestSyncErr
}

type mockMQClient struct {
	*client.EmptyMQClient
	mqClient mockRemoteClient

	brokerAddr       map[string]string
	p                *Producer
	updateTopicCount int

	updateTopicRouterInfoFromNamesrvErr error
	topicRouter                         *route.TopicRouter
}

func (c *mockMQClient) RemotingClient() remote.Client { return &c.mqClient }

func (c *mockMQClient) GetMasterBrokerAddr(broker string) string {
	return c.brokerAddr[broker]
}

func (c *mockMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	return c.updateTopicRouterInfoFromNamesrvErr
}

func TestSendSync0(t *testing.T) {
	p := NewProducer("sendHeader", []string{"abc"}, &log.MockLogger{})
	mockMQClient := &mockMQClient{brokerAddr: map[string]string{"ok": "ok"}}
	mockRemoteClient := &mockMQClient.mqClient
	p.client = mockMQClient

	// no broker
	sr, err := p.sendSync(&message.Message{}, &message.Queue{BrokerName: "not exist"}, 123)
	assert.NotNil(t, err)
	assert.Equal(t, "cannot find broker", err.Error())

	// bad send
	mockRemoteClient.requestSyncErr = errors.New("bad request")
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.NotNil(t, err)
	assert.Equal(t, remote.RequestError(mockRemoteClient.requestSyncErr), err)
	mockRemoteClient.requestSyncErr = nil

	// ok
	q := &message.Queue{BrokerName: "ok"}
	mockRemoteClient.command.ExtFields = map[string]string{
		"msgId":       "1",
		"queueOffset": "111",
		"MSG_REGION":  "RegionID",
		"TRACE_ON":    "true",
		"queueId":     "3",
	}
	sr, err = p.sendSync(&message.Message{}, q, 123)
	assert.Nil(t, err)
	assert.Equal(t, OK, sr.Status)
	assert.Equal(t, "1", sr.OffsetID)
	assert.Equal(t, int64(111), sr.QueueOffset)
	assert.Equal(t, *q, *sr.Queue)
	assert.Equal(t, "RegionID", sr.RegionID)
	assert.True(t, sr.TraceOn)

	// disk timeout
	mockRemoteClient.command.Code = rpc.FlushDiskTimeout
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, FlushDiskTimeout, sr.Status)

	// slave timeout
	mockRemoteClient.command.Code = rpc.FlushSlaveTimeout
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, FlushSlaveTimeout, sr.Status)

	// slave not available
	mockRemoteClient.command.Code = rpc.SlaveNotAvailable
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, SlaveNotAvailable, sr.Status)
}

func TestSendSync(t *testing.T) {
	p := NewProducer("sendSync", []string{"abc"}, &log.MockLogger{})
	p.Start()
	mc := &mockMQClient{brokerAddr: map[string]string{"b1": "ok", "b2": "b2"}, p: p}
	p.client = mc

	defer func() {
		p.Shutdown()
	}()

	// empty message
	sr, err := p.SendSync(nil)
	assert.Equal(t, errEmptyMessage, err)

	m := &message.Message{}

	// empty body
	sr, err = p.SendSync(m)
	assert.Equal(t, errEmptyBody, err)
	m.Body = []byte("test send sync topic body")

	// empty topic
	sr, err = p.SendSync(m)
	assert.Equal(t, errEmptyTopic, err)
	m.Topic = "test send sync topic"

	// update topic router error
	mc.updateTopicRouterInfoFromNamesrvErr = errors.New("bad update topic router")
	sr, err = p.SendSync(m)
	assert.Equal(t, mc.updateTopicRouterInfoFromNamesrvErr, err)
	mc.updateTopicRouterInfoFromNamesrvErr = nil

	// no routers
	sr, err = p.SendSync(m)
	assert.Equal(t, errNoRouters, err)

	// ok
	mc.p.UpdateTopicPublish(m.Topic, &route.TopicRouter{
		Queues: []*route.TopicQueue{
			&route.TopicQueue{
				BrokerName: "b",
				ReadCount:  2,
				WriteCount: 2,
				Perm:       route.PermWrite | route.PermWrite,
				SyncFlag:   2,
			},
			&route.TopicQueue{
				BrokerName: "b1",
				ReadCount:  2,
				WriteCount: 2,
				Perm:       route.PermWrite | route.PermWrite,
				SyncFlag:   2,
			},
		},
		Brokers: []*route.Broker{
			&route.Broker{
				Cluster:   "c",
				Name:      "b",
				Addresses: map[int32]string{0: "a"},
			},
			&route.Broker{
				Cluster:   "c",
				Name:      "b1",
				Addresses: map[int32]string{0: "a"},
			},
		},
	})
	mc.mqClient.command.ExtFields = map[string]string{
		"msgId":       "1",
		"queueOffset": "111",
		"MSG_REGION":  "RegionID",
		"TRACE_ON":    "true",
		"queueId":     "3",
	}
	sr, err = p.SendSync(m)
	assert.Nil(t, err)
	assert.Equal(t, OK, sr.Status)
	assert.False(t, p.mqFaultStrategy.Available("b"))
	assert.True(t, p.mqFaultStrategy.Available("b1"))
}

func TestUpdateTopicRouter(t *testing.T) {
	p := NewProducer("sendSync", []string{"abc"}, &log.MockLogger{})
	topic := "update topic router"
	p.topicPublishInfos.table = make(map[string]*topicPublishInfo)
	p.UpdateTopicPublish(topic, &route.TopicRouter{
		Queues: []*route.TopicQueue{
			&route.TopicQueue{
				BrokerName: "b1",
				ReadCount:  1,
				WriteCount: 1,
				Perm:       route.PermRead,
			},
			&route.TopicQueue{
				BrokerName: "b2",
				ReadCount:  2,
				WriteCount: 2,
				Perm:       route.PermWrite,
			},
			&route.TopicQueue{
				BrokerName: "b3",
				ReadCount:  3,
				WriteCount: 3,
				Perm:       route.PermWrite,
			},
		},
		Brokers: []*route.Broker{
			&route.Broker{
				Cluster: "c",
				Name:    "b1",
			},
			&route.Broker{
				Cluster: "c",
				Name:    "b2",
			},
			&route.Broker{
				Cluster:   "c",
				Name:      "b3",
				Addresses: map[int32]string{0: "b3"},
			},
		},
	})

	pi := p.topicPublishInfos.get(topic)
	queues := pi.queues
	assert.Equal(t, 3, len(queues))
	for i, q := range queues {
		assert.Equal(t, topic, q.Topic)
		assert.Equal(t, "b3", q.BrokerName)
		assert.Equal(t, uint8(i), q.QueueID)
	}
}
