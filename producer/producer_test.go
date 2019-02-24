package producer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
	"qiniu.com/dora-cloud/boots/broker/utils"
)

func TestSendHeader(t *testing.T) {
	p := NewProducer("sendHeader", []string{"abc"}, utils.CreateDefaultLogger())
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
	assert.Equal(t, header.SysFlag, 123)
	assert.True(t, header.BornTimestamp > 0)
	assert.Equal(t, header.Flag, m.Flag)
	assert.True(t, header.Properties != "")
	assert.Equal(t, header.UnitMode, p.IsUnitMode)
	assert.Equal(t, header.Batch, false)
	assert.Equal(t, header.ReconsumeTimes, 0)
	assert.Equal(t, header.MaxReconsumeTimes, 0)
	assert.Equal(t, "100", m.Properties[message.PropertyReconsumeTime])
	assert.Equal(t, "120", m.Properties[message.PropertyMaxReconsumeTimes])

	m.Topic = rocketmq.RetryGroupTopicPrefix + p.GroupName
	header = p.buildSendHeader(m, q, 123)
	assert.Equal(t, header.ReconsumeTimes, 100)
	assert.Equal(t, header.MaxReconsumeTimes, 120)
	assert.Equal(t, "", m.Properties[message.PropertyReconsumeTime])
	assert.Equal(t, "", m.Properties[message.PropertyMaxReconsumeTimes])
}

type mockMQClient struct {
	*client.EmptyMQClient
	mqClient         client.MQClient
	brokerAddr       map[string]string
	p                *Producer
	updateTopicCount int
}

func (c *mockMQClient) GetMasterBrokerAddr(broker string) string {
	return c.brokerAddr[broker]
}

func (c *mockMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	switch c.updateTopicCount++; c.updateTopicCount {
	case 1:
		return errors.New("mock update topic error")
	case 2:
		return nil
	default:
		c.p.UpdateTopicPublish(topic, &route.TopicRouter{
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
				&route.TopicQueue{
					BrokerName: "b2",
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
				&route.Broker{
					Cluster:   "c",
					Name:      "b2",
					Addresses: map[int32]string{0: "a"},
				},
			},
		})
		return nil
	}
}

type mockRPC struct {
	sendCount int
}

func (r *mockRPC) SendMessageSync(
	addr string, body []byte, h *remote.SendHeader, to time.Duration,
) (
	*remote.SendResponse, error,
) {
	resp := &remote.SendResponse{
		Response:      remote.Response{},
		MsgID:         "msgID",
		QueueOffset:   111,
		QueueID:       222,
		RegionID:      "RegionID",
		TraceOn:       true,
		TransactionID: "TransactionID",
	}
	switch r.sendCount++; r.sendCount {
	case 1:
		return nil, errors.New("mock send error")
	case 2:
		resp.Code = remote.Success
	case 3:
		resp.Code = remote.FlushDiskTimeout
	case 4:
		resp.Code = remote.FlushSlaveTimeout
	case 5:
		resp.Code = remote.SlaveNotAvailable
	default:
		resp.Code = remote.SystemError
		resp.Message = "mock system error"
	}
	return resp, nil
}

func TestSendSync0(t *testing.T) {
	p := NewProducer("sendHeader", []string{"abc"}, utils.CreateDefaultLogger())
	p.client = &mockMQClient{brokerAddr: map[string]string{"ok": "ok"}}
	p.rpc = &mockRPC{}

	sr, err := p.sendSync(&message.Message{}, &message.Queue{BrokerName: "not exist"}, 123)
	assert.NotNil(t, err)
	assert.Equal(t, "cannot find broker", err.Error())

	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.NotNil(t, err)
	assert.Equal(t, "mock send error", err.Error())
	q := &message.Queue{BrokerName: "ok"}
	sr, err = p.sendSync(&message.Message{}, q, 123)
	assert.Nil(t, err)
	assert.Equal(t, OK, sr.Status)
	assert.Equal(t, "msgID", sr.OffsetID)
	assert.Equal(t, int64(111), sr.QueueOffset)
	assert.Equal(t, *q, *sr.Queue)
	assert.Equal(t, "RegionID", sr.RegionID)
	assert.Equal(t, "TransactionID", sr.TransactionID)
	assert.True(t, sr.TraceOn)

	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, FlushDiskTimeout, sr.Status)
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, FlushSlaveTimeout, sr.Status)
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, SlaveNotAvailable, sr.Status)
	sr, err = p.sendSync(&message.Message{}, &message.Queue{BrokerName: "ok"}, 123)
	assert.NotNil(t, err)
}

func TestSendSync(t *testing.T) {
	p := NewProducer("sendSync", []string{"abc"}, utils.CreateDefaultLogger())
	p.Start()
	mc := &mockMQClient{brokerAddr: map[string]string{"b1": "ok", "b2": "b2"}, mqClient: p.client, p: p}
	p.client = mc
	p.rpc = &mockRPC{}

	defer func() {
		p.Shutdown()
		mc.mqClient.UnregisterProducer(p.GroupName)
		mc.mqClient.Shutdown()
	}()

	m := &message.Message{
		Topic: "test send sync topic",
		Body:  []byte("test send sync topic body"),
	}

	sr, err := p.SendSync(m)
	assert.Equal(t, "mock update topic error", err.Error())
	sr, err = p.SendSync(m)
	assert.Equal(t, "no routers", err.Error())
	sr, err = p.SendSync(m)
	assert.NotNil(t, err)

	sr, err = p.SendSync(m)
	assert.Equal(t, OK, sr.Status)
	assert.False(t, p.mqFaultStrategy.Available("b"))
	assert.False(t, p.mqFaultStrategy.Available("b1"))
}

func TestUpdateTopicRouter(t *testing.T) {
	p := NewProducer("sendSync", []string{"abc"}, utils.CreateDefaultLogger())
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
