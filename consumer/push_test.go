package consumer

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

type fakePullRequestDispatcher struct {
	runSubmitImmediately bool
	runSubmitLater       bool
	delay                time.Duration
}

func (d *fakePullRequestDispatcher) submitRequestImmediately(r *pullRequest) {
	d.runSubmitImmediately = true
}

func (d *fakePullRequestDispatcher) submitRequestLater(r *pullRequest, delay time.Duration) {
	d.runSubmitLater = true
	d.delay = delay
}

func newTestConcurrentConsumer() *PushConsumer {
	pc, err := NewConcurrentlyConsumer(
		"test-push-consumer", []string{"dummy"}, &fakeConcurrentlyConsumer{}, log.Std,
	)
	pc.client = &fakeMQClient{}
	pc.pullService = &fakePullRequestDispatcher{}
	pc.offsetStorer = &fakeOffsetStorer{}
	if err != nil {
		panic(err)
	}

	return pc
}

func TestNewPushConsumer(t *testing.T) {
	pc := newPushConsumer("group", []string{}, log.Std)
	assert.NotNil(t, pc)
	assert.Equal(t, pc.maxReconsumeTimes, defaultPushMaxReconsumeTimes)
	assert.Equal(t, pc.lastestConsumeTimestamp, defaultLastestConsumeTimestamp)
	assert.Equal(t, pc.consumeTimeout, defaultConsumeTimeout)
	assert.Equal(t, pc.thresholdCountOfQueue, defaultThresholdCountOfQueue)
	assert.Equal(t, pc.thresholdSizeOfQueue, defaultThresholdSizeOfQueue)
	assert.Equal(t, pc.thresholdCountOfTopic, defaultThresholdCountOfTopic)
	assert.Equal(t, pc.thresholdSizeOfTopic, defaultThresholdSizeOfTopic)
	assert.Equal(t, pc.pullInterval, defaultPullInterval)
	assert.Equal(t, pc.consumeBatchSize, defaultConsumeBatchSize)
	assert.Equal(t, pc.pullBatchSize, defaultPullBatchSize)
	assert.Equal(t, pc.postSubscriptionWhenPull, defaultPostSubscriptionWhenPull)
	assert.Equal(t, pc.consumeMessageBatchMaxSize, defaultConsumeMessageBatchMaxSize)
}

func TestUpdateProcessTable(t *testing.T) {
	pc := newTestConcurrentConsumer()
	offseter, consumerService := &fakeOffsetStorer{}, &fakeConsumerService{}
	pc.offsetStorer, pc.consumeService = offseter, consumerService

	mmp := &fakeMessagePuller{}
	pc.pullService, _ = newPullService(pullServiceConfig{
		messagePuller: mmp,
		logger:        pc.logger,
	})

	topic := "TestUpdateProcessTable"

	test := func(newMQs, expected []*message.Queue, expectedChanged bool) {
		changed := pc.updateProcessTableAndDispatchPullRequest(topic, newMQs)
		assert.Equal(t, expectedChanged, changed)
		mqs := pc.consumeService.messageQueuesOfTopic("")
		assertMQs(t, expected, mqs)
	}
	newMQs := []*message.Queue{{}, {QueueID: 1}}
	// insert all
	t.Log("add all")
	consumerService.insertRet = true
	test(newMQs, newMQs, true)

	// insert compute pull offset failed
	t.Log("insert, but donot insert")
	offseter.readOffsetErr = errors.New("bad readoffset")
	expected := newMQs
	newMQs = append(newMQs, &message.Queue{QueueID: 3})
	test(newMQs, expected, false)
	offseter.readOffsetErr = nil

	// do nothing
	t.Log("do nothing")
	newMQs = expected
	test(newMQs, expected, false)

	// remove
	t.Log("remove")
	newMQs = []*message.Queue{{}}
	consumerService.removeRet = true
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
	consumerService := &fakeConsumerService{}
	pc.consumeService = consumerService

	consumerService.queues = []message.Queue{{}, {QueueID: 1}}

	// divided by queues
	pc.thresholdSizeOfTopic, pc.thresholdCountOfTopic = 10, 20
	pc.updateThresholdOfQueue("")
	assert.Equal(t, 5, pc.thresholdSizeOfQueue)
	assert.Equal(t, 10, pc.thresholdCountOfQueue)

	// less than 1
	pc.thresholdSizeOfTopic, pc.thresholdCountOfTopic = 1, 1
	pc.updateThresholdOfQueue("")
	assert.Equal(t, 1, pc.thresholdSizeOfQueue)
	assert.Equal(t, 1, pc.thresholdCountOfQueue)

	// do nothing
	pc.thresholdSizeOfTopic, pc.thresholdCountOfTopic = -1, -1
	pc.thresholdSizeOfQueue, pc.thresholdCountOfQueue = 12, 34
	pc.updateThresholdOfQueue("")
	assert.Equal(t, 12, pc.thresholdSizeOfQueue)
	assert.Equal(t, 34, pc.thresholdCountOfQueue)
}

func TestUpdateSubscribeVersion(t *testing.T) {
	pc := newTestConcurrentConsumer()
	pc.subscribeData = client.NewSubcribeTable()
	pc.client = &fakeMQClient{}
	pc.Subscribe("TestUpdateSubscribeVersion", exprAll)

	t1 := time.Now().UnixNano() / int64(time.Millisecond)
	pc.updateSubscribeVersion("TestUpdateSubscribeVersion")
	assert.True(t, t1 <= pc.subscribeData.Get("TestUpdateSubscribeVersion").Version)
}

func TestReblance(t *testing.T) {
	pc := newTestConcurrentConsumer()
	consumerService := &fakeConsumerService{}
	pc.offsetStorer = &fakeOffsetStorer{}
	pc.client = &fakeMQClient{}
	pc.consumeService = consumerService

	pc.topicRouters = route.NewTopicRouterTable()
	pc.subscribeQueues = client.NewSubscribeQueueTable()

	clientID := "a"
	pc.ClientID = clientID
	pc.client = &fakeMQClient{clientIDs: []string{clientID}}

	// no queue
	pc.reblance("TestReblance")
	assert.False(t, consumerService.runInsert)

	// new queues
	pc.subscribeQueues.Put("TestReblance", []*message.Queue{{}})
	pc.topicRouters.Put("TestReblance", &route.TopicRouter{
		Brokers: []*route.Broker{{Addresses: map[int32]string{0: "addr"}}},
	})
	pc.reblance("TestReblance")
	assert.True(t, consumerService.runInsert)
}

func TestComputeFromLastOffset(t *testing.T) {
	pc := newTestConcurrentConsumer()

	offseter, mqClient := &fakeOffsetStorer{}, &fakeMQClient{}
	pc.offsetStorer, pc.client = offseter, mqClient

	pc.FromWhere = ConsumeFromLastOffset
	mqClient.brokderAddr = "fake"

	q := &message.Queue{}
	// from offseter
	offseter.offset = 2
	offset, err := pc.computeFromLastOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), offset)

	// from remote
	offseter.readOffsetErr = errOffsetNotExist
	mqClient.maxOffset = 22
	offset, err = pc.computeWhereToPull(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(22), offset)

	// bad readoffset
	offseter.readOffsetErr = errors.New("bad readoffset")
	offset, err = pc.computeWhereToPull(q)
	assert.Equal(t, offseter.readOffsetErr, err)

	// retry topic
	q.Topic = rocketmq.RetryGroupTopicPrefix + "t"
	offseter.readOffsetErr = errOffsetNotExist
	offset, err = pc.computeWhereToPull(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)

	// bad maxoffset offset
	q.Topic = ""
	mqClient.maxOffsetErr = &rpc.Error{}
	_, err = pc.computeWhereToPull(q)
	assert.Equal(t, mqClient.maxOffsetErr, err)
	mqClient.maxOffsetErr = nil

	// retry topic
	q.Topic = rocketmq.RetryGroupTopicPrefix + "t"
	offset, err = pc.computeWhereToPull(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)
}

func TestComputeFromFirstOffset(t *testing.T) {
	pc := newTestConcurrentConsumer()

	offseter, mqClient := &fakeOffsetStorer{}, &fakeMQClient{}
	pc.offsetStorer, pc.client = offseter, mqClient

	pc.FromWhere = ConsumeFromFirstOffset
	mqClient.brokderAddr = "fake"

	q := &message.Queue{}
	// from offseter
	offseter.offset = 2
	offset, err := pc.computeFromFirstOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), offset)

	// not exist
	offseter.readOffsetErr = errOffsetNotExist
	offset, err = pc.computeFromFirstOffset(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)

	// bad readoffset
	offseter.readOffsetErr = errors.New("bad readoffset")
	offset, err = pc.computeFromFirstOffset(q)
	assert.NotNil(t, err)
}

func TestComputeFromTimestamp(t *testing.T) {
	pc := newTestConcurrentConsumer()

	offseter, mqClient := &fakeOffsetStorer{}, &fakeMQClient{}
	pc.offsetStorer, pc.client = offseter, mqClient

	pc.FromWhere = ConsumeFromTimestamp
	mqClient.brokderAddr = "fake"

	q := &message.Queue{}
	// from offseter
	offseter.offset = 2
	offset, err := pc.computeFromTimestamp(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), offset)

	// offseter search error
	offseter.readOffsetErr = errors.New("bad read offset")
	_, err = pc.computeFromTimestamp(q)
	assert.Equal(t, offseter.readOffsetErr, err)
	offseter.readOffsetErr = nil

	// not exist and retry topic
	mqClient.maxOffset = 100
	q.Topic = rocketmq.RetryGroupTopicPrefix
	offseter.readOffsetErr = errOffsetNotExist
	offset, err = pc.computeFromTimestamp(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(100), offset)

	// not exist, search remote suc
	q.Topic = ""
	mqClient.searchOffsetByTimestampRet = 200
	offset, err = pc.computeFromTimestamp(q)
	assert.Nil(t, err)
	assert.Equal(t, int64(200), offset)

	// not exist search failed
	mqClient.searchOffsetByTimestampErr = &rpc.Error{}
	offset, err = pc.computeFromTimestamp(q)
	assert.Equal(t, mqClient.searchOffsetByTimestampErr, err)
	mqClient.searchOffsetByTimestampErr = nil
}

func TestPushPull(t *testing.T) {
	c := newTestConcurrentConsumer()
	pr := &pullRequest{
		group:        "g",
		messageQueue: &message.Queue{},
		processQueue: &processQueue{},
		nextOffset:   0,
	}

	// dropped
	pq := pr.processQueue
	pq.drop()
	c.pull(pr)
	assert.Equal(t, int64(0), pq.lastPullTime)
	pq.dropped = processQueueStateNormal

	// bad state
	pullService := c.pullService.(*fakePullRequestDispatcher)
	c.State = rocketmq.StateCreating
	c.pull(pr)
	assert.False(t, pq.lastPullTime == 0, pq.lastPullTime)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
	pullService.runSubmitLater = false
	c.State = rocketmq.StateRunning

	// pause
	c.Pause()
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenPause, pullService.delay)
	c.Resume()
	pullService.runSubmitLater = false

	// over count threshold
	pq.msgCount = int32(c.thresholdCountOfQueue + 1)
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenFlowControl, pullService.delay)
	pullService.runSubmitLater = false
	pq.msgCount = 0

	// over size threshold
	pq.msgSize = int64(c.thresholdSizeOfQueue + 1)
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenFlowControl, pullService.delay)
	pullService.runSubmitLater = false
	pq.msgSize = 0

	// consumer flow controll
	cs := &fakeConsumerService{}
	c.consumeService = cs
	cs.flowControllRet = true
	bk := c.queueControlFlowTotal
	c.pull(pr)
	assert.Equal(t, 1+bk, c.queueControlFlowTotal)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenFlowControl, pullService.delay)
	pullService.runSubmitLater = false
	cs.flowControllRet = false

	// consume service check failed
	cs.checkRet = errors.New("check failed")
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
	cs.checkRet = nil
	pullService.runSubmitLater = false
	pullService.delay = 0

	// no subscription
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
	pullService.runSubmitLater = false
	pullService.delay = 0

	c.subscribeData.Put(pr.messageQueue.Topic, &client.SubscribeData{})

	offsetStorer := c.offsetStorer.(*fakeOffsetStorer)
	c.MessageModel = Clustering

	// commit offset
	c.pull(pr)
	assert.True(t, offsetStorer.runReadOffset)
	assert.Equal(t, ReadOffsetFromMemory, offsetStorer.readType)
	offsetStorer.runReadOffset = false

	// find broker error
	mqClient := c.client.(*fakeMQClient)
	mqClient.findBrokerAddrErr = errors.New("find broker failed")
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
	pullService.runSubmitLater = false
	pullService.delay = 0
	mqClient.findBrokerAddrErr = nil

	// bad version
	c.subscribeData.Put(pr.messageQueue.Topic, &client.SubscribeData{Type: "a>1"})
	mqClient.findBrokerAddrRet.Version = 0
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
	pullService.runSubmitLater = false
	pullService.delay = 0
	mqClient.findBrokerAddrErr = nil
	c.subscribeData.Put(pr.messageQueue.Topic, &client.SubscribeData{})

	// check header
	c.subscribeData.Put(pr.messageQueue.Topic, &client.SubscribeData{
		Type:    ExprTypeSQL92.String(),
		Version: 10000,
		Expr:    "a>1",
	})
	offsetStorer.offset = 123
	mqClient.findBrokerAddrRet.Version = 10000
	c.pull(pr)
	header := mqClient.pullHeader
	assert.Equal(t, int32(c.pullBatchSize), header.MaxCount)
	assert.Equal(t, int64(123), header.CommitOffset)
	assert.Equal(t, "a>1", header.Subscription)
	assert.Equal(t, ExprTypeSQL92.String(), header.ExpressionType)
	assert.Equal(t, int64(10000), header.SubVersion)
	assert.Equal(t, buildPullFlag(true, true, false, false), header.SysFlag)

	// post subscription when pull
	c.postSubscriptionWhenPull = true
	c.pull(pr)
	header = mqClient.pullHeader
	assert.Equal(t, buildPullFlag(true, true, true, false), header.SysFlag)

	// send failed
	mqClient.pullAsyncErr = errors.New("pull failed")
	c.pull(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
}

func TestStartAndShutdown(t *testing.T) {
	c := newTestConcurrentConsumer()

	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	c.Shutdown()
}
