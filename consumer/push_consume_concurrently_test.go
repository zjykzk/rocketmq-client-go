package consumer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeConcurrentlyConsumer struct {
	consumeCount int32
	wg           sync.WaitGroup
	ret          ConsumeConcurrentlyStatus
}

func (m *fakeConcurrentlyConsumer) Consume(
	msgs []*message.Ext, ctx *ConcurrentlyContext,
) ConsumeConcurrentlyStatus {
	atomic.AddInt32(&m.consumeCount, int32(len(msgs)))
	m.wg.Done()
	return m.ret
}

type fakeSendback struct {
	runSendback bool
	sendErr     error

	msgs []*message.Ext

	sync.Mutex
}

func (ms *fakeSendback) SendBack(m *message.Ext, delayLevel int, broker string) error {
	ms.Lock()
	ms.runSendback = true
	ms.msgs = append(ms.msgs, m)
	ms.Unlock()

	return ms.sendErr
}

func (ms *fakeSendback) messageCount() int {
	ms.Lock()
	r := len(ms.msgs)
	ms.Unlock()
	return r
}

func newTestConcurrentlyService(t *testing.T) *consumeConcurrentlyService {
	cs, err := newConsumeConcurrentlyService(concurrentlyServiceConfig{
		consumeServiceConfig: consumeServiceConfig{
			group:           "test concurrent consume service",
			messageSendBack: &fakeSendback{},
			offseter:        &fakeOffsetStorer{},
			logger:          log.Std,
		},
		consumer:             &fakeConcurrentlyConsumer{},
		consumeTimeout:       time.Second * 20,
		concurrentCount:      3,
		batchSize:            3,
		cleanExpiredInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	cs.consumeLaterInterval = time.Millisecond
	return cs
}

func TestNewConcurrentlyService(t *testing.T) {
	_, err := newConsumeConcurrentlyService(concurrentlyServiceConfig{})
	assert.NotNil(t, err)

	_, err = newConsumeConcurrentlyService(concurrentlyServiceConfig{
		consumeServiceConfig: consumeServiceConfig{
			group:           "test consume service",
			messageSendBack: &fakeSendback{},
			logger:          log.Std,
		},
	})
	assert.NotNil(t, err)

	consumer := &fakeConcurrentlyConsumer{}
	_, err = newConsumeConcurrentlyService(concurrentlyServiceConfig{
		consumeServiceConfig: consumeServiceConfig{
			group:           "test consume service",
			messageSendBack: &fakeSendback{},
			logger:          log.Std,
		},
		consumer: consumer,
	})
	assert.NotNil(t, err)

	_, err = newConsumeConcurrentlyService(concurrentlyServiceConfig{
		cleanExpiredInterval: time.Millisecond,
		consumer:             consumer,
		consumeTimeout:       time.Second * 20,
		concurrentCount:      3,
		batchSize:            3,
	})
	assert.NotNil(t, err)

	newTestConcurrentlyService(t)
}

func TestStartShutdown(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	// expired message
	pq := cs.newProcessQueue(&message.Queue{})
	msgs := []*message.Ext{}
	m := &message.Ext{}
	m.SetConsumeStartTimestamp(time.Now().Add(-time.Hour).UnixNano())
	msgs = append(msgs, m)
	pq.putMessages(msgs)

	fakeConsumer := cs.consumer.(*fakeConcurrentlyConsumer)
	fakeConsumer.wg.Add(1)
	// to be consumed message
	cs.submitConsumeRequest([]*message.Ext{{QueueOffset: 20}}, pq, &message.Queue{QueueID: 1})

	cs.start()
	time.Sleep(time.Millisecond * 10)
	defer cs.shutdown()

	assert.Equal(t, 1, cs.messageSendBack.(*fakeSendback).messageCount())
	assert.Equal(t, int32(1), atomic.LoadInt32(&fakeConsumer.consumeCount))
}

func TestSubmitRequestSuccess(t *testing.T) {
	cs := newTestConcurrentlyService(t)

	cs.start()
	defer cs.shutdown()

	count := 2000
	fakeConsumer := cs.consumer.(*fakeConcurrentlyConsumer)
	fakeConsumer.wg.Add(count)

	msgs := []*message.Ext{&message.Ext{}}
	for i := 0; i < count; i++ {
		go func() { cs.submitConsumeRequest(msgs, newProcessQueue(), nil) }()
	}

	fakeConsumer.wg.Wait()
	assert.Equal(t, int32(count), fakeConsumer.consumeCount)
}

func TestConsumeConcurrentlyProcessResultConsumeLater(t *testing.T) {
	t.Run("broadcasting", func(t *testing.T) {
		cs := newTestConcurrentlyService(t)
		offsetUpdater := cs.offseter.(*fakeOffsetStorer)
		cs.messageModel = BroadCasting

		msgs := []*message.Ext{&message.Ext{}}
		pq := newProcessQueue()
		pq.putMessages(msgs)

		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: &message.Queue{}},
		)

		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)

		// two message and succcess one
		msgs = []*message.Ext{
			&message.Ext{QueueOffset: 1}, &message.Ext{QueueOffset: 2},
		}
		pq = newProcessQueue()
		pq.putMessages(msgs)

		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{AckIndex: 0},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: &message.Queue{}},
		)

		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(3), offsetUpdater.offset)
	})

	t.Run("clustering", func(t *testing.T) {
		cs := newTestConcurrentlyService(t)
		offsetUpdater := cs.offseter.(*fakeOffsetStorer)
		sendbacker := cs.messageSendBack.(*fakeSendback)
		cs.messageModel = Clustering
		mq := &message.Queue{}

		// all send back
		msgs := []*message.Ext{&message.Ext{}}
		pq := newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{AckIndex: -1, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)
		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.True(t, sendbacker.runSendback)
		sendbacker.runSendback = false

		// two message and send back ok
		msgs = []*message.Ext{
			&message.Ext{QueueOffset: 1}, &message.Ext{QueueOffset: 2},
		}
		pq = newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{AckIndex: 0, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)
		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(3), offsetUpdater.offset)
		assert.True(t, sendbacker.runSendback)

		// two message and send back failed
		sendbacker.sendErr = errors.New("sendback failed")
		pq = newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{AckIndex: 0, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)
		assert.Equal(t, 2, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(1), offsetUpdater.offset)
		assert.True(t, sendbacker.runSendback)

		// sendback ok, process dropped
		sendbacker.sendErr = nil
		offsetUpdater.offset = -1
		offsetUpdater.runUpdate = false
		pq.drop()

		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{AckIndex: 0, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)

		assert.False(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(-1), offsetUpdater.offset)
	})
}

func TestConsumeConcurrentlyProcessResultSuc(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	offsetUpdater := cs.offseter.(*fakeOffsetStorer)
	sendbacker := cs.messageSendBack.(*fakeSendback)
	t.Run("broadcasting", func(t *testing.T) {
		cs.messageModel = BroadCasting

		// two message and succcess one
		msgs := []*message.Ext{
			&message.Ext{QueueOffset: 1}, &message.Ext{QueueOffset: 2},
		}
		pq := newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ReconsumeLater,
			&ConcurrentlyContext{AckIndex: 0},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: &message.Queue{}},
		)
		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(3), offsetUpdater.offset)
	})

	t.Run("clustering", func(t *testing.T) {
		cs.messageModel = Clustering
		mq := &message.Queue{}

		// all send back
		msgs := []*message.Ext{&message.Ext{}}
		pq := newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ConcurrentlySuccess,
			&ConcurrentlyContext{AckIndex: -1, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)
		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.True(t, sendbacker.runSendback)
		sendbacker.runSendback = false

		// two message and send back ok
		msgs = []*message.Ext{
			&message.Ext{QueueOffset: 1}, &message.Ext{QueueOffset: 2},
		}
		pq = newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ConcurrentlySuccess,
			&ConcurrentlyContext{AckIndex: 0, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)
		assert.Equal(t, 0, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(3), offsetUpdater.offset)

		// two message and send back failed
		sendbacker.sendErr = errors.New("sendback failed")
		pq = newProcessQueue()
		pq.putMessages(msgs)
		cs.processConsumeResult(
			ConcurrentlySuccess,
			&ConcurrentlyContext{AckIndex: 0, MessageQueue: mq},
			&consumeConcurrentlyRequest{messages: msgs, processQueue: pq, messageQueue: mq},
		)
		assert.Equal(t, 1, pq.messages.Size())
		assert.True(t, offsetUpdater.runUpdate)
		assert.Equal(t, int64(2), offsetUpdater.offset)
		assert.True(t, sendbacker.runSendback)
	})
}

func TestConcurrentlyProcessqueue(t *testing.T) {
	cs := newTestConcurrentlyService(t)

	// not over limit
	pq := cs.newProcessQueue(&message.Queue{})
	msgs := []*message.Ext{}
	for i, t := 0, time.Now().Add(-time.Hour).UnixNano(); i < 10; i++ {
		m := &message.Ext{QueueOffset: int64(i)}
		m.SetConsumeStartTimestamp(t + int64(i))
		msgs = append(msgs, m)
	}
	for i, t := 0, time.Now().UnixNano(); i < 10; i++ {
		m := &message.Ext{QueueOffset: int64(i + 100)}
		m.SetConsumeStartTimestamp(t)
		msgs = append(msgs, m)
	}
	pq.putMessages(msgs)

	cs.clearExpiredMessage()

	sendbacker := cs.messageSendBack.(*fakeSendback)
	expiredMsgs := sendbacker.msgs
	assert.Equal(t, 10, len(expiredMsgs))
	assert.Equal(t, msgs[:10], expiredMsgs)

	// over the limit
	sendbacker.msgs = nil
	msgs = msgs[0:10]
	for i, t := 0, time.Now().Add(-time.Hour).UnixNano(); i < 10; i++ {
		m := &message.Ext{QueueOffset: int64(i + 1000)}
		m.SetConsumeStartTimestamp(t + int64(i))
		msgs = append(msgs, m)
	}
	pq = cs.newProcessQueue(&message.Queue{BrokerName: "2"})
	pq.putMessages(msgs)

	cs.clearExpiredMessage()

	expiredMsgs = sendbacker.msgs
	assert.Equal(t, 16, len(expiredMsgs))
	assert.Equal(t, msgs[:16], expiredMsgs)
}

func TestConcurrentSubmitLater(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	cs.start()

	count := 2000
	fakeConsumer := cs.consumer.(*fakeConcurrentlyConsumer)
	fakeConsumer.wg.Add(count)

	msgs := []*message.Ext{&message.Ext{}}
	for i := 0; i < count; i++ {
		go func() {
			cs.submitConsumeRequestLater(&consumeConcurrentlyRequest{
				messages:     msgs,
				processQueue: newProcessQueue(),
				messageQueue: &message.Queue{},
			})
		}()
	}

	fakeConsumer.wg.Wait()
	assert.Equal(t, int32(count), fakeConsumer.consumeCount)

	cs.shutdown()
}

func TestGetConsumingMessageQueue(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	cs.start()
	defer cs.shutdown()

	mqs := []message.Queue{message.Queue{}, message.Queue{QueueID: 1}}
	cs.newProcessQueue(&mqs[0])
	cs.newProcessQueue(&mqs[1])

	// all
	mqs1 := cs.messageQueuesOfTopic("")
	assert.Equal(t, 2, len(mqs))
	q01, q02 := mqs[0], mqs[1]
	q11, q12 := mqs1[0], mqs1[1]
	assert.True(t, (q01 == q11 || q01 == q12) && (q02 == q11 || q02 == q12))

	// empty
	mqs1 = cs.messageQueuesOfTopic("empty")
	assert.Equal(t, 0, len(mqs1))
}

func TestPutNewMessageQueue(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	mq := &message.Queue{}
	pq, ok := cs.putNewMessageQueue(mq)
	assert.True(t, ok)
	assert.NotNil(t, pq)
	pq, ok = cs.putNewMessageQueue(mq)
	assert.False(t, ok)
	assert.Nil(t, pq)
}

func TestConcurrentConsumeDirectly(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	fakeConsumer := cs.consumer.(*fakeConcurrentlyConsumer)

	msg, broker := &message.Ext{}, "TestConsumeDirectoryConcurrent"

	fakeConsumer.ret = ReconsumeLater
	fakeConsumer.wg.Add(1)
	ret := cs.consumeMessageDirectly(msg, broker)

	assert.Equal(t, int(ReconsumeLater), int(ret.Result))
	assert.True(t, ret.TimeCost > 0)
	assert.False(t, ret.Order)
	assert.True(t, ret.AutoCommit)

	fakeConsumer.ret = ConcurrentlySuccess
	fakeConsumer.wg.Add(1)
	ret = cs.consumeMessageDirectly(msg, broker)

	assert.Equal(t, int(ConcurrentlySuccess), int(ret.Result))
	assert.True(t, ret.TimeCost > 0)
	assert.False(t, ret.Order)
	assert.True(t, ret.AutoCommit)
}
