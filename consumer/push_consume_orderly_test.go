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
	lockErr error

	runUnlock bool
	unlockErr error

	waitRunChan chan struct{}
}

func (l *fakeMessageQueueLocker) Lock(broker string, mqs []message.Queue) error {
	l.waitRunChan <- struct{}{}
	return l.lockErr
}

func (l *fakeMessageQueueLocker) Unlock(mq message.Queue) error {
	l.runUnlock = true
	return l.unlockErr
}

func TestNewConsumeOrderlyService(t *testing.T) {
	// empty mq locker
	_, err := newConsumeOrderlyService(orderlyServiceConfig{})
	assert.Equal(t, errors.New("new consume orderly service error:empty message queue locker"), err)

	// base consume error
	_, err = newConsumeOrderlyService(orderlyServiceConfig{
		mqLocker: &fakeMessageQueueLocker{},
	})

	assert.NotNil(t, err)
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
	})
	if err != nil {
		t.Fatal(err)
	}
	cs.pullExpiredInterval = time.Millisecond
	cs.checkLockInterval = time.Millisecond
	return cs
}

func TestLock(t *testing.T) {
	c := newTestConsumeOrderlyService(t)
	locker := c.mqLocker.(*fakeMessageQueueLocker)
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b", Topic: "t"}, true)

	assert.Equal(t, locker.lockErr, c.lockQueues())
}

func TestGetMQsGroupByBroker(t *testing.T) {
	c := newTestConsumeOrderlyService(t)
	assert.Equal(t, 0, len(c.getMQsGroupByBroker()))

	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b", Topic: "t"}, true)
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b1", Topic: "t1"}, true)
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b1", Topic: "t2"}, true)

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
	c := newTestConsumeOrderlyService(t)
	c.start()
	c.baseConsumeService.processQueues.Store(message.Queue{BrokerName: "b", Topic: "t"}, true)

	<-c.mqLocker.(*fakeMessageQueueLocker).waitRunChan
}

func TestInsertNewMessageQueueOrderly(t *testing.T) {
	c := newTestConsumeOrderlyService(t)
	pq, ok := c.insertNewMessageQueue(&message.Queue{BrokerName: "insert"})
	assert.True(t, ok)
	assert.NotNil(t, pq)
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
		processQueue{lastPullTime: time.Now().Add(time.Second)},
		1,
	})

	count := 0
	counter := func() {
		count = 0
		cs.processQueues.Range(func(_, _ interface{}) bool { count++; return true })
	}

	counter()
	assert.Equal(t, 2, count)

	cs.dropExpiredProcessQueues()

	counter()
	assert.Equal(t, 1, count)
}

func TestPullExpired(t *testing.T) {
	pq := newOrderProcessQueue()
	pq.updatePullTime(time.Now())
	assert.False(t, pq.isPullExpired(time.Second))
	assert.True(t, pq.isPullExpired(time.Nanosecond))
}

func TestUnlockProcessQueueInBroker(t *testing.T) {
	cs := newTestConsumeOrderlyService(t)
	//cs.processQueues.Store(message.Queue{}, &fakeProcessQueue{})

	//counter := func() int {
	//c := 0
	//cs.processQueues.Range(func(_, _ interface{}) bool { c++; return true })
	//return c
	//}
	//assert.Equal(t, 1, counter())

	//// BroadCasting
	//cs.messageModel = BroadCasting
	//assert.True(t, cs.dropAndRemoveProcessQueue(&message.Queue{}))
	//assert.Equal(t, 0, counter())

	//// cluster
	//cs.messageModel = Clustering

	t.Run("locked failed", func(t *testing.T) {
		oq, mq := newOrderProcessQueue(), message.Queue{}
		assert.True(t, oq.tryLockConsume(time.Millisecond))

		cs.processQueues.Store(mq, oq)
		assert.False(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		cs.processQueues.Delete(mq)
	})

	t.Run("locked success", func(t *testing.T) {
		oq, mq := newOrderProcessQueue(), message.Queue{}
		cs.processQueues.Store(message.Queue{}, oq)
		assert.True(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		cs.processQueues.Delete(mq)
	})

	t.Run("delay unlock", func(t *testing.T) {
		cs.delayUnlockDuration = time.Millisecond
		oq, mq := newOrderProcessQueue(), message.Queue{}
		cs.processQueues.Store(message.Queue{}, oq)
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
		cs.processQueues.Store(message.Queue{}, oq)
		assert.True(t, cs.unlockProcessQueueInBroker(mq, oq, time.Millisecond))
		cs.processQueues.Delete(mq)

		qlocker := cs.mqLocker.(*fakeMessageQueueLocker)
		assert.True(t, qlocker.runUnlock)
	})
}
