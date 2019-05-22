package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeMessageQueueLocker struct {
	lockRet []message.Queue
	lockErr error

	runUnlock bool
	unlockErr error

	waitRunChan chan struct{}
}

func (l *fakeMessageQueueLocker) Lock(broker string, mqs []message.Queue) ([]message.Queue, error) {
	l.waitRunChan <- struct{}{}
	return l.lockRet, l.lockErr
}

func (l *fakeMessageQueueLocker) ignoreLockWait() {
	select {
	case <-l.waitRunChan:
	default:
	}
}

func (l *fakeMessageQueueLocker) Unlock(mq message.Queue) error {
	l.runUnlock = true
	return l.unlockErr
}

type fakeOrderlyConsumer struct {
	afterConsumeCallback func()

	runConsumeCount int
	status          ConsumeOrderlyStatus
}

func (c *fakeOrderlyConsumer) Consume(messages []*message.Ext, ctx *OrderlyContext) ConsumeOrderlyStatus {
	c.runConsumeCount++

	if c.afterConsumeCallback != nil {
		c.afterConsumeCallback()
	}

	return c.status
}

func TestNewConsumeOrderlyService(t *testing.T) {
	// empty mq locker
	_, err := newConsumeOrderlyService(orderlyServiceConfig{})
	assert.Equal(t, errors.New("new consume orderly service error:empty message queue locker"), err)

	// base consume error
	_, err = newConsumeOrderlyService(orderlyServiceConfig{
		mqLocker: &fakeMessageQueueLocker{},
		consumer: &fakeOrderlyConsumer{},
	})

	baseConf := consumeServiceConfig{
		group:           "g",
		logger:          log.Std,
		messageSendBack: &fakeSendback{},
		offseter:        &fakeOffsetStorer{},
	}

	// no consumer
	_, err = newConsumeOrderlyService(orderlyServiceConfig{
		consumeServiceConfig: baseConf,
		mqLocker:             &fakeMessageQueueLocker{},
	})
	assert.NotNil(t, err)

	// no consumer
	c, err := newConsumeOrderlyService(orderlyServiceConfig{
		consumeServiceConfig: baseConf,
		mqLocker:             &fakeMessageQueueLocker{},
		consumer:             &fakeOrderlyConsumer{},
	})
	assert.Nil(t, err)
	assert.NotNil(t, c)

	assert.Equal(t, defaultLockedTimeInBroker, c.lockedTimeInBroker)
	assert.Equal(t, defaultCheckLockInterval, c.checkLockInterval)
}

func newTestConsumeOrderlyService(t *testing.T) *consumeOrderlyService {
	cs, err := newConsumeOrderlyService(orderlyServiceConfig{
		consumeServiceConfig: consumeServiceConfig{
			group:           "test orderly consume",
			logger:          log.Std,
			messageSendBack: &fakeSendback{},
			offseter:        &fakeOffsetStorer{},
		},
		messageModel: Clustering,
		mqLocker:     &fakeMessageQueueLocker{waitRunChan: make(chan struct{}, 1)},
		consumer:     &fakeOrderlyConsumer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	cs.pullExpiredInterval = time.Millisecond
	cs.checkLockInterval = time.Millisecond
	return cs
}

func TestLock(t *testing.T) {
	c, oq := newTestConsumeOrderlyService(t), newOrderProcessQueue()
	locker := c.mqLocker.(*fakeMessageQueueLocker)
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b", Topic: "t"}, oq)

	assert.Equal(t, locker.lockErr, c.lockQueues())
}

func TestGetMQsGroupByBroker(t *testing.T) {
	c, oq := newTestConsumeOrderlyService(t), newOrderProcessQueue()
	assert.Equal(t, 0, len(c.getMQsGroupByBroker()))

	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b", Topic: "t"}, oq)
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b1", Topic: "t1"}, oq)
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b1", Topic: "t2"}, oq)

	assert.True(t, isEqualOfMQ([]mqsOfBroker{
		{broker: "b", mqs: []message.Queue{{BrokerName: "b", Topic: "t"}}},
		{broker: "b1", mqs: []message.Queue{
			{BrokerName: "b1", Topic: "t1"},
			{BrokerName: "b1", Topic: "t2"},
		}},
	}, c.getMQsGroupByBroker()))
}

func isEqualOfMQ(mq1, mq2 []mqsOfBroker) bool {
	if len(mq1) != len(mq2) {
		return false
	}

NEXT_MQ1:
	for _, q1 := range mq1 {
		for _, q2 := range mq2 {
			if q1.broker == q2.broker && isEqualOfMQ0(q1.mqs, q2.mqs) {
				continue NEXT_MQ1
			}
		}
		return false
	}

NEXT_MQ2:
	for _, q2 := range mq2 {
		for _, q1 := range mq1 {
			if q2.broker == q1.broker && isEqualOfMQ0(q2.mqs, q1.mqs) {
				continue NEXT_MQ2
			}
		}
		return false
	}
	return true
}

func isEqualOfMQ0(mq1, mq2 []message.Queue) bool {
	if len(mq1) != len(mq2) {
		return false
	}

NEXT_MQ1:
	for _, q1 := range mq1 {
		for _, q2 := range mq2 {
			if q1 == q2 {
				continue NEXT_MQ1
			}
		}
		return false
	}

NEXT_MQ2:
	for _, q2 := range mq2 {
		for _, q1 := range mq1 {
			if q2 == q1 {
				continue NEXT_MQ2
			}
		}
		return false
	}

	return true
}

func TestConsumeOrderlyStart(t *testing.T) {
	c, oq := newTestConsumeOrderlyService(t), newOrderProcessQueue()
	c.start()
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b", Topic: "t"}, oq)

	<-c.mqLocker.(*fakeMessageQueueLocker).waitRunChan
}

func TestInsertNewMessageQueueOrderly(t *testing.T) {
	c := newTestConsumeOrderlyService(t)
	mqLocker := c.mqLocker.(*fakeMessageQueueLocker)
	// lock failed
	mqLocker.lockErr = errors.New("bad lock")
	pq, ok := c.insertNewMessageQueue(&message.Queue{BrokerName: "insert"})
	assert.False(t, ok)
	mqLocker.lockErr = nil
	mqLocker.ignoreLockWait()

	// locked returned empty mq
	pq, ok = c.insertNewMessageQueue(&message.Queue{BrokerName: "insert"})
	assert.False(t, ok)
	mqLocker.ignoreLockWait()

	// lock ok
	mqLocker.lockRet = []message.Queue{{}}
	pq, ok = c.insertNewMessageQueue(&message.Queue{BrokerName: "insert"})
	assert.True(t, ok)
	assert.NotNil(t, pq)
	mqLocker.ignoreLockWait()

	// insert duplicated
	pq, ok = c.insertNewMessageQueue(&message.Queue{BrokerName: "insert"})
	assert.False(t, ok)
	assert.Nil(t, pq)
}

func TestFlowControlOrderly(t *testing.T) {
	c := newTestConsumeOrderlyService(t)
	assert.False(t, c.flowControl(nil))
}

func TestCheckOrderly(t *testing.T) {
	c := newTestConsumeOrderlyService(t)

	q := &orderProcessQueue{}
	assert.NotNil(t, c.check(&q.processQueue))
	q.lockedInBroker = lockedInBroker
	assert.Nil(t, c.check(&q.processQueue))
}

type fakeProcessQueue struct {
	processQueue

	other int
}

func testDropExpiredProcessQueue(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	cs.processQueues.Store(message.Queue{}, &fakeProcessQueue{})
	cs.processQueues.Store(message.Queue{QueueID: 1}, &fakeProcessQueue{
		processQueue{lastPullTime: time.Now().Add(time.Second).UnixNano()},
		1,
	})

	count := 0
	counter := func() {
		count = 0
		cs.processQueues.Range(func(_, _ interface{}) bool { count++; return true })
	}

	counter()
	assert.Equal(t, 2, count)

	cs.dropPullExpiredProcessQueues()

	counter()
	assert.Equal(t, 1, count)

	pq, ok := cs.processQueues.Load(message.Queue{})
	assert.True(t, ok)
	assert.True(t, pq.(*fakeProcessQueue).isDropped())

	pq, ok = cs.processQueues.Load(message.Queue{QueueID: 1})
	assert.True(t, ok)
	assert.False(t, pq.(*fakeProcessQueue).isDropped())
}

func TestOrderProcessQueue(t *testing.T) {
	pq := newOrderProcessQueue()

	// pull expired
	pq.updatePullTime(time.Now())
	assert.False(t, pq.isPullExpired(time.Second))
	assert.True(t, pq.isPullExpired(time.Nanosecond))

	// take message
	assert.Equal(t, 0, len(pq.takeMessage(1)))
	pq.putMessages([]*message.Ext{{QueueOffset: 0, Message: message.Message{Body: []byte{0}}}, {QueueOffset: 1}})
	assert.Equal(t, 1, len(pq.takeMessage(1)))
	assert.Equal(t, 1, len(pq.consumingMessages))
	assert.Equal(t, 1, pq.messages.Size())
	assert.Equal(t, 1, len(pq.takeMessage(2)))
	assert.Equal(t, 2, len(pq.consumingMessages))
	assert.Equal(t, 0, pq.messages.Size())

	// reconsume
	pq.reconsume()
	assert.Equal(t, 0, len(pq.consumingMessages))
	assert.Equal(t, 2, pq.messages.Size())

	// commit
	assert.Equal(t, int32(2), pq.messageCount())
	assert.Equal(t, int64(1), pq.messageSize())
	assert.Equal(t, 2, len(pq.takeMessage(2)))
	assert.Equal(t, int64(1), pq.commit())
	assert.Equal(t, int32(0), pq.messageCount())
	assert.Equal(t, int64(0), pq.messageSize())
}

func TestOrderlyDropAndRemoveProcessQueue(t *testing.T) {
	cs, oq := newTestConsumeOrderlyService(t), newOrderProcessQueue()
	cs.processQueues.Store(message.Queue{}, oq)

	counter := func() int {
		c := 0
		cs.processQueues.Range(func(_, _ interface{}) bool { c++; return true })
		return c
	}
	assert.Equal(t, 1, counter())

	// BroadCasting
	cs.messageModel = BroadCasting
	assert.True(t, cs.dropAndRemoveProcessQueue(&message.Queue{}))
	assert.Equal(t, 0, counter())
	cs.processQueues.Store(message.Queue{}, oq)

	// cluster
	cs.messageModel = Clustering

	// no message queue
	assert.False(t, cs.dropAndRemoveProcessQueue(&message.Queue{QueueID: 1}))

	// lock failed
	oq.tryLockConsume(time.Second)
	assert.False(t, cs.dropAndRemoveProcessQueue(&message.Queue{}))

	// ok
	oq.unLockConsume()
	assert.True(t, cs.dropAndRemoveProcessQueue(&message.Queue{}))
}

func TestUnlockProcessQueueInBroker(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)

	t.Run("locked failed", func(t *testing.T) {
		oq, mq := newOrderProcessQueue(), message.Queue{}
		assert.True(t, oq.tryLockConsume(time.Millisecond))

		cs.processQueues.Store(mq, oq)
		assert.False(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		cs.processQueues.Delete(mq)
	})

	t.Run("locked success", func(t *testing.T) {
		oq, mq := newOrderProcessQueue(), message.Queue{}
		cs.processQueues.Store(mq, oq)
		assert.True(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		cs.processQueues.Delete(mq)
	})

	t.Run("delay unlock", func(t *testing.T) {
		cs.unlockDelay = time.Millisecond
		oq, mq := newOrderProcessQueue(), message.Queue{}
		cs.processQueues.Store(mq, oq)
		oq.putMessages([]*message.Ext{{}})
		assert.True(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		qlocker := cs.mqLocker.(*fakeMessageQueueLocker)
		for !qlocker.runUnlock {
		}

		qlocker.runUnlock = false
		cs.processQueues.Delete(mq)
	})

	t.Run("unlock", func(t *testing.T) {
		oq, mq := newOrderProcessQueue(), message.Queue{}
		cs.processQueues.Store(mq, oq)
		assert.True(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		cs.processQueues.Delete(mq)

		qlocker := cs.mqLocker.(*fakeMessageQueueLocker)
		assert.True(t, qlocker.runUnlock)
	})
}

func TestDropPullExpiredProcessQueue(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	cs.unlockDelay = time.Millisecond

	expiredOQ, mq := newOrderProcessQueue(), message.Queue{}
	cs.processQueues.Store(mq, expiredOQ)

	normalOQ := newOrderProcessQueue()
	normalOQ.lastPullTime = time.Now().UnixNano()
	cs.processQueues.Store(message.Queue{QueueID: 1}, normalOQ)

	cs.dropPullExpiredProcessQueues()

	v, ok := cs.processQueues.Load(message.Queue{QueueID: 1})
	assert.True(t, ok)
	assert.Equal(t, normalOQ, v.(*orderProcessQueue))
}

func TestOrderlyConsume(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	consumer := cs.consumer.(*fakeOrderlyConsumer)

	t.Run("normal", func(t *testing.T) {
		mq, oq := &message.Queue{}, newOrderProcessQueue()
		r := &consumeOrderlyRequest{messageQueue: mq, processQueue: oq}
		// dropped
		oq.drop()
		cs.consume(r)
		oq.dropped = 0

		// unlocked
		oq.lockedInBroker = unlockedInBroker
		cs.consume(r)

		// locked and expired
		oq.lockedInBroker = lockedInBroker
		cs.consume(r)

		// consume no message
		oq.lastLockTime = time.Now().UnixNano()
		cs.consume(r)
		assert.Equal(t, 0, consumer.runConsumeCount)

		// with message
		oq.putMessages([]*message.Ext{{}})
		cs.consume(r)
		assert.Equal(t, 1, consumer.runConsumeCount)
	})

	// oq is dropped
	t.Run("dropped after consume", func(t *testing.T) {
		mq, oq := &message.Queue{}, newOrderProcessQueue()
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		oq.lockInBroker(time.Now().UnixNano())
		r := &consumeOrderlyRequest{messageQueue: mq, processQueue: oq}

		consumer.afterConsumeCallback = func() { oq.drop() }
		consumer.runConsumeCount = 0
		cs.consume(r)
		assert.Equal(t, int32(1), oq.messageCount())
		assert.Equal(t, 1, consumer.runConsumeCount)
		consumer.runConsumeCount = 0
		oq.dropped = processQueueStateNormal
	})

	// droped after get the consume-locker
	t.Run("dropped after get consume locker", func(t *testing.T) {
		mq, oq := &message.Queue{}, newOrderProcessQueue()
		oq.lockInBroker(time.Now().UnixNano())
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		r := &consumeOrderlyRequest{messageQueue: mq, processQueue: oq}

		oq.lockConsume()
		oq.putMessages([]*message.Ext{{}})
		go func() {
			for len(oq.timeoutLocker.sem) <= 0 {
			}
			oq.drop()
			oq.unLockConsume()
		}()
		cs.consume(r)
		assert.Equal(t, int32(2), oq.messageCount())
		oq.dropped = processQueueStateNormal
		assert.Equal(t, 0, consumer.runConsumeCount)
	})

	mqLocker := cs.mqLocker.(*fakeMessageQueueLocker)
	t.Run("unlocked after consuming", func(t *testing.T) {
		mq, oq := &message.Queue{BrokerName: "unlocked after consuming"}, newOrderProcessQueue()
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		oq.lockInBroker(time.Now().UnixNano())
		r := &consumeOrderlyRequest{messageQueue: mq, processQueue: oq}

		consumer.afterConsumeCallback = func() { oq.lockedInBroker = unlockedInBroker }
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		assert.Equal(t, int32(2), oq.messageCount())
		cs.consume(r)
		assert.Equal(t, int32(1), oq.messageCount())
		mqLocker.lockRet = []message.Queue{{}}
		<-mqLocker.waitRunChan
		oq.lockedInBroker = lockedInBroker
		assert.True(t, 1 <= consumer.runConsumeCount)
		for ; consumer.runConsumeCount != 2; mqLocker.ignoreLockWait() {
		}
		for ; oq.messageCount() > 0; mqLocker.ignoreLockWait() { // wait for committed
		}
		consumer.runConsumeCount = 0
		oq.lockedInBroker = lockedInBroker
	})

	t.Run("locked expired and delay", func(t *testing.T) {
		mq, oq := &message.Queue{BrokerName: "locked expired and delay"}, newOrderProcessQueue()
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		oq.lockInBroker(time.Now().UnixNano())
		r := &consumeOrderlyRequest{messageQueue: mq, processQueue: oq}

		assert.Equal(t, int32(2), oq.messageCount())
		consumer.afterConsumeCallback = func() { oq.lastLockTime = 0 }
		cs.consume(r)
		<-mqLocker.waitRunChan
		assert.True(t, int32(1) >= oq.messageCount())
		assert.True(t, 1 <= consumer.runConsumeCount)

		oq.lastLockTime = time.Now().UnixNano()
		for ; consumer.runConsumeCount != 2; mqLocker.ignoreLockWait() {
		}
		for oq.messageCount() > 0 { // wait for committed
		}
		oq.lastLockTime = time.Now().UnixNano()
	})

	t.Run("consume too long", func(t *testing.T) {
		mq, oq := &message.Queue{BrokerName: "consume too long"}, newOrderProcessQueue()
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		oq.lockInBroker(time.Now().UnixNano())
		r := &consumeOrderlyRequest{messageQueue: mq, processQueue: oq}

		consumer.afterConsumeCallback = nil
		cs.maxConsumeContinuouslyTime = 0
		oq.putMessages([]*message.Ext{{}, {QueueOffset: 1}})
		cs.consume(r)
		<-mqLocker.waitRunChan
		for ; oq.messageCount() > 0; mqLocker.ignoreLockWait() { // wait for committed
		}
	})

}

func TestSubmitOrderlyRequest(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)

	mq := &message.Queue{}
	cs.submitRequest(&consumeOrderlyRequest{messageQueue: mq, processQueue: newOrderProcessQueue()})

	requests, ok := cs.requestsOfQueue.Load(*mq)
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests.(chan *consumeOrderlyRequest)))
}

func TestLockAndConsumeLater(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	mqLocker := cs.mqLocker.(*fakeMessageQueueLocker)
	mq, oq := &message.Queue{}, newOrderProcessQueue()
	mqLocker.lockRet = []message.Queue{*mq}
	cs.lockAndConsumeLater(
		&consumeOrderlyRequest{messageQueue: mq, processQueue: oq}, time.Millisecond,
	)

	for _, ok := cs.requestsOfQueue.Load(*mq); !ok; _, ok = cs.requestsOfQueue.Load(*mq) {
	}
}

func TestProcessOrderlyConsumeResult(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	oq, mq := newOrderProcessQueue(), message.Queue{}
	cs.processQueues.Store(mq, oq)

	msgs := []*message.Ext{
		{QueueOffset: 1, Message: message.Message{Body: []byte{0}}}, {QueueOffset: 2},
	}
	oq.putMessages(msgs)
	consumingMsgs := oq.takeMessage(1)
	assert.Equal(t, 1, len(consumingMsgs))

	ctx := &OrderlyContext{}
	req := &consumeOrderlyRequest{processQueue: oq, messageQueue: &message.Queue{}}
	// sucess no auto commit
	assert.True(t, cs.processConsumeResult(consumingMsgs, OrderlySuccess, ctx, req))
	assert.Equal(t, 1, len(oq.consumingMessages))
	assert.Equal(t, int32(2), oq.messageCount())

	// success auto commit
	ctx.autoCommit = true
	assert.True(t, cs.processConsumeResult(consumingMsgs, OrderlySuccess, ctx, req))
	assert.Equal(t, 0, len(oq.consumingMessages))
	assert.Equal(t, int32(1), oq.messageCount())
	ctx.autoCommit = false

	// suspend no auto commit, not exceed the max reconsume time
	cs.maxReconsumeTimes = 1
	consumingMsgs = oq.takeMessage(1)
	assert.Equal(t, 1, len(consumingMsgs))
	assert.Equal(t, int32(1), oq.messageCount())
	assert.False(t, cs.processConsumeResult(consumingMsgs, SuspendCurrentQueueMoment, ctx, req))
	assert.Equal(t, 0, len(oq.consumingMessages))
	assert.Equal(t, int32(1), oq.messageCount())
	for _, m := range consumingMsgs {
		assert.Equal(t, int32(1), m.ReconsumeTimes)
	}

	// suspend no auto commit, exceed the max reconsume time, sendback failed
	cs.messageSendBack.(*fakeSendback).sendErr = errors.New("send failed")
	consumingMsgs = oq.takeMessage(1)
	assert.False(t, cs.processConsumeResult(consumingMsgs, SuspendCurrentQueueMoment, ctx, req))
	assert.Equal(t, 0, len(oq.consumingMessages))
	assert.Equal(t, int32(1), oq.messageCount())
	for _, m := range consumingMsgs {
		assert.Equal(t, int32(2), m.ReconsumeTimes)
	}
	cs.messageSendBack.(*fakeSendback).sendErr = nil

	// suspend no auto commit, exceed the max reconsume time, sendback suc
	consumingMsgs = oq.takeMessage(1)
	assert.False(t, cs.processConsumeResult(consumingMsgs, SuspendCurrentQueueMoment, ctx, req))
	assert.Equal(t, 1, len(oq.consumingMessages))
	assert.Equal(t, int32(1), oq.messageCount())
	for _, m := range consumingMsgs {
		assert.Equal(t, int32(2), m.ReconsumeTimes)
	}

	// suspend auto commit, exceed the max reconsume time, sendback suc
	ctx.autoCommit = true
	consumingMsgs = oq.takeMessage(1)
	assert.False(t, cs.processConsumeResult(consumingMsgs, SuspendCurrentQueueMoment, ctx, req))
	assert.Equal(t, 0, len(oq.consumingMessages))
	assert.Equal(t, int32(0), oq.messageCount())
	for _, m := range consumingMsgs {
		assert.Equal(t, int32(2), m.ReconsumeTimes)
	}

	oq.putMessages(msgs)
	// commmit offset ok queue is not droped
	consumingMsgs = oq.takeMessage(1)
	t.Log(consumingMsgs)
	assert.True(t, cs.processConsumeResult(consumingMsgs, OrderlySuccess, ctx, req))
	assert.Equal(t, int64(2), cs.offseter.(*fakeOffsetStorer).offset)

	// commmit offset ok queue is droped
	oq.drop()
	consumingMsgs = oq.takeMessage(1)
	assert.True(t, cs.processConsumeResult(consumingMsgs, OrderlySuccess, ctx, req))
	assert.Equal(t, int64(2), cs.offseter.(*fakeOffsetStorer).offset)
	assert.True(t, cs.offseter.(*fakeOffsetStorer).runUpdate)
}

func TestOrderlyConsumeDirectly(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	fakeConsumer := cs.consumer.(*fakeOrderlyConsumer)

	msg, broker := &message.Ext{}, "TestOrderlyConsumeDirectly"

	fakeConsumer.status = SuspendCurrentQueueMoment
	ret := cs.consumeMessageDirectly(msg, broker)

	assert.Equal(t, int(ReconsumeLater), int(ret.Result))
	assert.True(t, ret.TimeCost > 0)
	assert.True(t, ret.Order)
	assert.True(t, ret.AutoCommit)

	fakeConsumer.status = OrderlySuccess
	ret = cs.consumeMessageDirectly(msg, broker)

	assert.Equal(t, int(ConcurrentlySuccess), int(ret.Result))
	assert.True(t, ret.TimeCost > 0)
	assert.True(t, ret.Order)
	assert.True(t, ret.AutoCommit)
}
