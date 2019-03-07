package producer

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

type fakeMQClient struct {
	sendMessageSyncErr error
	sendResponse       rpc.SendResponse
}

func (f *fakeMQClient) Start() error {
	return nil
}
func (f *fakeMQClient) Shutdown() {

}
func (f *fakeMQClient) RegisterProducer(p client.Producer) error {
	return nil
}
func (f *fakeMQClient) UnregisterProducer(group string) {

}
func (f *fakeMQClient) SendMessageSync(
	broker string, body []byte, h *rpc.SendHeader, timeout time.Duration,
) (
	*rpc.SendResponse, error,
) {
	return &f.sendResponse, f.sendMessageSyncErr
}

func (f *fakeMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	return nil
}

func newTestProducer() *Producer {
	p := New("sendHeader", []string{"abc"}, log.Std)
	p.State = rocketmq.StateRunning
	p.client = &fakeMQClient{}
	return p
}

func TestSendHeader(t *testing.T) {
	p := newTestProducer()
	p.CreateTopicKey, p.DefaultTopicQueueNums = "CreateTopicKey", 100
	m := &messageWrap{Message: &message.Message{Topic: "sendHeader", Flag: 111, Properties: map[string]string{
		message.PropertyReconsumeTime:     "100",
		message.PropertyMaxReconsumeTimes: "120",
	}}}
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

func TestSendSync0(t *testing.T) {
	p := newTestProducer()
	mqClient := p.client.(*fakeMQClient)

	m := &messageWrap{Message: &message.Message{}}
	// send message sync failed
	mqClient.sendMessageSyncErr = errors.New("bad send")
	sr, err := p.sendMessageWithQueueSync(m, &message.Queue{BrokerName: "not exist"}, 123)
	assert.NotNil(t, err)
	mqClient.sendMessageSyncErr = nil

	// disk timeout
	mqClient.sendResponse.Code = rpc.FlushDiskTimeout
	sr, err = p.sendMessageWithQueueSync(m, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, FlushDiskTimeout, sr.Status)

	// slave timeout
	mqClient.sendResponse.Code = rpc.FlushSlaveTimeout
	sr, err = p.sendMessageWithQueueSync(m, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, FlushSlaveTimeout, sr.Status)

	// slave not available
	mqClient.sendResponse.Code = rpc.SlaveNotAvailable
	sr, err = p.sendMessageWithQueueSync(m, &message.Queue{BrokerName: "ok"}, 123)
	assert.Nil(t, err)
	assert.Equal(t, SlaveNotAvailable, sr.Status)
}

func TestSendSync(t *testing.T) {
	p := newTestProducer()
	p.mqFaultStrategy = NewMQFaultStrategy(true)

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
	assert.NotNil(t, err)
	m.Topic = "test"

	// large body
	m.Body = make([]byte, 1<<23)
	sr, err = p.SendSync(m)
	assert.Equal(t, errLargeBody, err)
	m.Body = make([]byte, 1)

	// ok
	p.topicPublishInfos.table = map[string]*topicPublishInfo{m.Topic: &topicPublishInfo{queues: []*message.Queue{
		{BrokerName: "b"},
	}}}

	sr, err = p.SendSync(m)
	assert.Nil(t, err)
	assert.Equal(t, OK, sr.Status)
	assert.True(t, p.mqFaultStrategy.Available("b"))
}

func TestUpdateTopicRouter(t *testing.T) {
	p := newTestProducer()
	topic := "TestUpdateTopicRouter"
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

func TestSendMessageWithLatency(t *testing.T) {
	p := newTestProducer()
	topic, mqClient := "TestSendMessageWithLatency", p.client.(*fakeMQClient)
	p.mqFaultStrategy = NewMQFaultStrategy(true)

	router := &topicPublishInfo{
		queues: []*message.Queue{
			{Topic: topic, BrokerName: "b"},
			{Topic: topic, BrokerName: "b1"},
		},
	}

	// send failed
	mqClient.sendMessageSyncErr = errors.New("bad send")
	_, err := p.sendMessageWithLatency(router, &messageWrap{Message: &message.Message{}}, 0)
	assert.NotNil(t, err)
	assert.False(t, p.mqFaultStrategy.Available("b"))
	assert.False(t, p.mqFaultStrategy.Available("b1"))
}

func TestCompress(t *testing.T) {
	p := newTestProducer()

	// under threshold
	ok, err := p.tryToCompress(&message.Message{})
	assert.False(t, ok)
	assert.Nil(t, err)

	p.CompressSizeThreshod = 1
	p.CompressLevel = 100
	// bad level
	ok, err = p.tryToCompress(&message.Message{Body: []byte{1, 2}})
	assert.NotNil(t, err)
	p.CompressLevel = 5

	// OK
	ok, err = p.tryToCompress(&message.Message{Body: []byte{1, 2}})
	assert.Nil(t, err)
	assert.True(t, ok)
}

func TestSendBatch(t *testing.T) {
	p := newTestProducer()
	// bad topic
	_, err := p.SendBatchSync(&message.Batch{})
	assert.NotNil(t, err)
}
