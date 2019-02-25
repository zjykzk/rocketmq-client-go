package consumer

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
	"github.com/zjykzk/rocketmq-client-go/route"
)

type mockConsumerService struct {
	queues    []message.Queue
	runInsert bool

	insertRet bool
	pt        *processQueue

	removeRet bool
}

func (m *mockConsumerService) messageQueues() []message.Queue {
	return m.queues
}

func (m *mockConsumerService) removeOldMessageQueue(mq *message.Queue) bool {
	nqs := make([]message.Queue, 0, len(m.queues))
	for _, q := range m.queues {
		if q != *mq {
			nqs = append(nqs, q)
		}
	}
	m.queues = nqs
	return m.removeRet
}

func (m *mockConsumerService) insertNewMessageQueue(mq *message.Queue) (*processQueue, bool) {
	m.runInsert = true
	m.queues = append(m.queues, *mq)
	return m.pt, m.insertRet
}

func newTestConcurrentConsumer() *PushConsumer {
	pc, err := NewConcurrentConsumer(
		"test push consumer", []string{"dummy"}, &mockConcurrentlyConsumer{}, &log.MockLogger{},
	)
	if err != nil {
		panic(err)
	}

	return pc
}

func TestNewPushConsumer(t *testing.T) {
	pc := newPushConsumer("group", []string{}, &log.MockLogger{})
	assert.NotNil(t, pc)
	assert.Equal(t, pc.MaxReconsumeTimes, defaultPushMaxReconsumeTimes)
	assert.Equal(t, pc.LastestConsumeTimestamp, defaultLastestConsumeTimestamp)
	assert.Equal(t, pc.ConsumeTimeout, defaultConsumeTimeout)
	assert.Equal(t, pc.MaxCountForQueue, defaultMaxCountForQueue)
	assert.Equal(t, pc.MaxSizeForQueue, defaultMaxSizeForQueue)
	assert.Equal(t, pc.MaxCountForTopic, defaultMaxCountForTopic)
	assert.Equal(t, pc.MaxSizeForTopic, defaultMaxSizeForTopic)
	assert.Equal(t, pc.PullInterval, defaultPullInterval)
	assert.Equal(t, pc.BatchSize, defaultBatchSize)
	assert.Equal(t, pc.PostSubscriptionWhenPull, defaultPostSubscriptionWhenPull)
	assert.Equal(t, pc.ConsumeMessageBatchMaxSize, defaultConsumeMessageBatchMaxSize)
}

func TestUpdateProcessTable(t *testing.T) {
	pc := newTestConcurrentConsumer()
	mockOffseter, mockConsumerService := &mockOffseter{}, &mockConsumerService{}
	pc.offseter, pc.consumerService = mockOffseter, mockConsumerService
	pc.rpc = &mockConsumerRPC{}

	mmp := &mockMessagePuller{}
	pc.pullService, _ = newPullService(pullServiceConfig{
		messagePuller: mmp,
		logger:        pc.Logger,
	})

	topic := "TestUpdateProcessTable"

	test := func(newMQs, expected []*message.Queue, expectedChanged bool) {
		changed := pc.updateProcessTable(topic, newMQs)
		assert.Equal(t, expectedChanged, changed)
		mqs := pc.consumerService.messageQueues()
		assertMQs(t, expected, mqs)
	}
	newMQs := []*message.Queue{{}, {QueueID: 1}}
	// insert all
	t.Log("add all")
	mockConsumerService.insertRet = true
	test(newMQs, newMQs, true)

	// insert compute pull offset failed
	t.Log("insert, but donot insert")
	mockOffseter.readOffsetErr = errors.New("bad readoffset")
	expected := newMQs
	newMQs = append(newMQs, &message.Queue{QueueID: 3})
	test(newMQs, expected, false)
	mockOffseter.readOffsetErr = nil

	// do nothing
	t.Log("do nothing")
	newMQs = expected
	test(newMQs, expected, false)

	// remove
	t.Log("remove")
	newMQs = []*message.Queue{{}}
	mockConsumerService.removeRet = true
	test(newMQs, newMQs, true)
	test([]*message.Queue{}, []*message.Queue{}, true)

	// insert and remove
	t.Log("add and remove")
	newMQs = []*message.Queue{{QueueID: 2}}
	test(newMQs, newMQs, true)
}

func assertMQs(t *testing.T, mqs1 []*message.Queue, mqs2 []message.Queue) {
	assert.Equal(t, len(mqs1), len(mqs2))
	for _, mq1 := range mqs1 {
		found := false
		for _, mq2 := range mqs2 {
			if *mq1 == mq2 {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func TestUpdateThresholdOfQueue(t *testing.T) {
	pc := newTestConcurrentConsumer()
	consumerService := &mockConsumerService{}
	pc.consumerService = consumerService

	consumerService.queues = []message.Queue{{}, {QueueID: 1}}

	// divided by queues
	pc.MaxSizeForTopic, pc.MaxCountForTopic = 10, 20
	pc.updateThresholdOfQueue()
	assert.Equal(t, 5, pc.MaxSizeForQueue)
	assert.Equal(t, 10, pc.MaxCountForQueue)

	// less than 1
	pc.MaxSizeForTopic, pc.MaxCountForTopic = 1, 1
	pc.updateThresholdOfQueue()
	assert.Equal(t, 1, pc.MaxSizeForQueue)
	assert.Equal(t, 1, pc.MaxCountForQueue)

	// do nothing
	pc.MaxSizeForTopic, pc.MaxCountForTopic = -1, -1
	pc.MaxSizeForQueue, pc.MaxCountForQueue = 12, 34
	pc.updateThresholdOfQueue()
	assert.Equal(t, 12, pc.MaxSizeForQueue)
	assert.Equal(t, 34, pc.MaxCountForQueue)
}

func TestUpdateSubscribeVersion(t *testing.T) {
	pc := newTestConcurrentConsumer()
	pc.subscribeData = client.NewDataTable()
	pc.client = &mockMQClient{}
	pc.Subscribe("TestUpdateSubscribeVersion")

	t1 := time.Now().UnixNano() / int64(time.Millisecond)
	pc.updateSubscribeVersion("TestUpdateSubscribeVersion")
	t2 := time.Now().UnixNano() / int64(time.Millisecond)
	assert.True(t, t1 <= pc.subscribeData.Get("TestUpdateSubscribeVersion").Version)
	assert.True(t, t2 <= pc.subscribeData.Get("TestUpdateSubscribeVersion").Version)
}

func TestReblance(t *testing.T) {
	pc := newTestConcurrentConsumer()
	mockConsumerService := &mockConsumerService{}
	pc.offseter = &mockOffseter{}
	pc.client = &mockMQClient{}
	pc.consumerService = mockConsumerService

	pc.topicRouters = route.NewTopicRouterTable()
	pc.subscribeQueues = client.NewQueueTable()

	clientID := "a"
	pc.ClientID = clientID
	pc.rpc = &mockConsumerRPC{clientIDs: []string{clientID}}

	// no queue
	pc.reblance("TestReblance")
	assert.False(t, mockConsumerService.runInsert)

	// new queues
	pc.subscribeQueues.Put("TestReblance", []*message.Queue{{}})
	pc.topicRouters.Put("TestReblance", &route.TopicRouter{
		Brokers: []*route.Broker{{Addresses: map[int32]string{0: "addr"}}},
	})
	pc.reblance("TestReblance")
	assert.True(t, mockConsumerService.runInsert)
}

func TestComputeFromLastOffset(t *testing.T) {
	pc := newTestConcurrentConsumer()

	mockOffseter, mockRPC, mockMQClient := &mockOffseter{}, &mockConsumerRPC{}, &mockMQClient{}
	pc.offseter, pc.rpc, pc.client = mockOffseter, mockRPC, mockMQClient

	pc.FromWhere = consumeFromLastOffset
	mockMQClient.brokderAddr = "mock"

	q := &message.Queue{}
	// from offseter
	mockOffseter.offset = 2
	offset, err := pc.computeFromLastOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), offset)

	// from remote
	mockOffseter.readOffsetErr = errOffsetNotExist
	mockRPC.maxOffset = 22
	offset, err = pc.computeWhereToPull(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(22), offset)

	// bad readoffset
	mockOffseter.readOffsetErr = errors.New("bad readoffset")
	offset, err = pc.computeWhereToPull(q)
	assert.Equal(t, mockOffseter.readOffsetErr, err)

	// retry topic
	q.Topic = rocketmq.RetryGroupTopicPrefix + "t"
	mockOffseter.readOffsetErr = errOffsetNotExist
	offset, err = pc.computeWhereToPull(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)

	// bad maxoffset offset
	q.Topic = ""
	mockRPC.maxOffsetErr = &remote.RPCError{}
	_, err = pc.computeWhereToPull(q)
	assert.Equal(t, mockRPC.maxOffsetErr, err)
	mockRPC.maxOffsetErr = nil

	// retry topic
	q.Topic = rocketmq.RetryGroupTopicPrefix + "t"
	offset, err = pc.computeFromLastOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)
}

func TestComputeFromFirstOffset(t *testing.T) {
	pc := newTestConcurrentConsumer()

	mockOffseter, mockRPC, mockMQClient := &mockOffseter{}, &mockConsumerRPC{}, &mockMQClient{}
	pc.offseter, pc.rpc, pc.client = mockOffseter, mockRPC, mockMQClient

	pc.FromWhere = consumeFromFirstOffset
	mockMQClient.brokderAddr = "mock"

	q := &message.Queue{}
	// from offseter
	mockOffseter.offset = 2
	offset, err := pc.computeFromFirstOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), offset)

	// not exist
	mockOffseter.readOffsetErr = errOffsetNotExist
	offset, err = pc.computeFromFirstOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)

	// bad readoffset
	mockOffseter.readOffsetErr = errors.New("bad readoffset")
	offset, err = pc.computeFromFirstOffset(q)
	assert.NotNil(t, err)
}

func TestComputeFromTimestamp(t *testing.T) {
	pc := newTestConcurrentConsumer()

	mockOffseter, mockRPC, mockMQClient := &mockOffseter{}, &mockConsumerRPC{}, &mockMQClient{}
	pc.offseter, pc.rpc, pc.client = mockOffseter, mockRPC, mockMQClient

	pc.FromWhere = consumeFromTimestamp
	mockMQClient.brokderAddr = "mock"

	q := &message.Queue{}
	// from offseter
	mockOffseter.offset = 2
	offset, err := pc.computeFromTimestamp(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), offset)

	// offseter search error
	mockOffseter.readOffsetErr = errors.New("bad read offset")
	_, err = pc.computeFromTimestamp(q)
	assert.Equal(t, mockOffseter.readOffsetErr, err)
	mockOffseter.readOffsetErr = nil

	// not exist and retry topic
	mockRPC.maxOffset = 100
	q.Topic = rocketmq.RetryGroupTopicPrefix
	mockOffseter.readOffsetErr = errOffsetNotExist
	offset, err = pc.computeFromTimestamp(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(100), offset)

	// not exist, search remote suc
	q.Topic = ""
	mockRPC.searchOffsetByTimestampRet = 200
	offset, err = pc.computeFromTimestamp(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(200), offset)

	// not exist search failed
	mockRPC.searchOffsetByTimestampErr = &remote.RPCError{}
	offset, err = pc.computeFromTimestamp(q)
	assert.Equal(t, mockRPC.searchOffsetByTimestampErr, err)
	mockRPC.searchOffsetByTimestampErr = nil
}
