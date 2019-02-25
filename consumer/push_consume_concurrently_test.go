package consumer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type mockConcurrentlyConsumer struct {
	consumeCount int32
	ret          ConsumeConcurrentlyStatus
}

func (m *mockConcurrentlyConsumer) Consume(
	msgs []*message.MessageExt, ctx *ConcurrentlyContext,
) ConsumeConcurrentlyStatus {
	atomic.AddInt32(&m.consumeCount, int32(len(msgs)))
	return m.ret
}

type mockSendback struct {
	runSendback bool
	sendErr     error

	msgs []*message.MessageExt
}

func (ms *mockSendback) SendBack(m *message.MessageExt, delayLevel int, broker string) error {
	ms.runSendback = true
	ms.msgs = append(ms.msgs, m)
	return ms.sendErr
}

type mockOffseter struct {
	runUpdate     bool
	offset        int64
	readOffsetErr error
}

func (m *mockOffseter) persist() error {
	return nil
}

func (m *mockOffseter) updateQueues(...*message.Queue) {
	return
}

func (m *mockOffseter) updateOffsetIfGreater(_ *message.Queue, offset int64) {
	m.offset = offset
	m.runUpdate = true
}

func (m *mockOffseter) persistOne(_ *message.Queue) {
}

func (m *mockOffseter) removeOffset(_ *message.Queue) (offset int64, ok bool) {
	offset = m.offset
	return
}

func (m *mockOffseter) readOffset(_ *message.Queue, _ int) (offset int64, err error) {
	err = m.readOffsetErr
	offset = m.offset
	return
}

func newTestConcurrentlyService(t *testing.T) *consumeConcurrentlyService {
	cs, err := newConsumeConcurrentlyService(concurrentlyServiceConfig{
		consumeServiceConfig: consumeServiceConfig{
			group:           "test concurrent consume service",
			messageSendBack: &mockSendback{},
			offseter:        &mockOffseter{},
			logger:          &log.MockLogger{},
		},
		consumer:             &mockConcurrentlyConsumer{},
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
			messageSendBack: &mockSendback{},
			logger:          &log.MockLogger{},
		},
	})
	assert.NotNil(t, err)

	consumer := &mockConcurrentlyConsumer{}
	_, err = newConsumeConcurrentlyService(concurrentlyServiceConfig{
		consumeServiceConfig: consumeServiceConfig{
			group:           "test consume service",
			messageSendBack: &mockSendback{},
			logger:          &log.MockLogger{},
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
	msgs := []*message.MessageExt{}
	m := &message.MessageExt{}
	m.SetConsumeStartTimestamp(time.Now().Add(-time.Hour).UnixNano())
	msgs = append(msgs, m)
	pq.putMessages(msgs)

	// to be consumed message
	cs.submitConsumeRequest([]*message.MessageExt{{QueueOffset: 20}}, pq, &message.Queue{QueueID: 1})

	cs.start()
	time.Sleep(time.Millisecond * 10)
	defer cs.shutdown()

	assert.Equal(t, 1, len(cs.messageSendBack.(*mockSendback).msgs))
	assert.Equal(t, int32(1), cs.consumer.(*mockConcurrentlyConsumer).consumeCount)
}

func TestSubmitRequestSuccess(t *testing.T) {
	cs := newTestConcurrentlyService(t)

	cs.start()
	defer cs.shutdown()

	count := 2000
	msgs := []*message.MessageExt{&message.MessageExt{}}
	for i := 0; i < count; i++ {
		go func() { cs.submitConsumeRequest(msgs, newProcessQueue(), nil) }()
	}

	for int(atomic.LoadInt32(&cs.consumer.(*mockConcurrentlyConsumer).consumeCount)) != count {
	}
}

func TestConsumeConcurrentlyProcessResultConsumeLater(t *testing.T) {
	t.Run("broadcasting", func(t *testing.T) {
		cs := newTestConcurrentlyService(t)
		offsetUpdater := cs.offseter.(*mockOffseter)
		cs.messageModel = BroadCasting

		msgs := []*message.MessageExt{&message.MessageExt{}}
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
		msgs = []*message.MessageExt{
			&message.MessageExt{QueueOffset: 1}, &message.MessageExt{QueueOffset: 2},
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
		offsetUpdater := cs.offseter.(*mockOffseter)
		sendbacker := cs.messageSendBack.(*mockSendback)
		cs.messageModel = Clustering
		mq := &message.Queue{}

		// all send back
		msgs := []*message.MessageExt{&message.MessageExt{}}
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
		msgs = []*message.MessageExt{
			&message.MessageExt{QueueOffset: 1}, &message.MessageExt{QueueOffset: 2},
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
	offsetUpdater := cs.offseter.(*mockOffseter)
	sendbacker := cs.messageSendBack.(*mockSendback)
	t.Run("broadcasting", func(t *testing.T) {
		cs.messageModel = BroadCasting

		// two message and succcess one
		msgs := []*message.MessageExt{
			&message.MessageExt{QueueOffset: 1}, &message.MessageExt{QueueOffset: 2},
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
		msgs := []*message.MessageExt{&message.MessageExt{}}
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
		msgs = []*message.MessageExt{
			&message.MessageExt{QueueOffset: 1}, &message.MessageExt{QueueOffset: 2},
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
	msgs := []*message.MessageExt{}
	for i, t := 0, time.Now().Add(-time.Hour).UnixNano(); i < 10; i++ {
		m := &message.MessageExt{QueueOffset: int64(i)}
		m.SetConsumeStartTimestamp(t + int64(i))
		msgs = append(msgs, m)
	}
	for i, t := 0, time.Now().UnixNano(); i < 10; i++ {
		m := &message.MessageExt{QueueOffset: int64(i + 100)}
		m.SetConsumeStartTimestamp(t)
		msgs = append(msgs, m)
	}
	pq.putMessages(msgs)

	cs.clearExpiredMessage()

	sendbacker := cs.messageSendBack.(*mockSendback)
	expiredMsgs := sendbacker.msgs
	assert.Equal(t, 10, len(expiredMsgs))
	assert.Equal(t, msgs[:10], expiredMsgs)

	// over the limit
	sendbacker.msgs = nil
	msgs = msgs[0:10]
	for i, t := 0, time.Now().Add(-time.Hour).UnixNano(); i < 10; i++ {
		m := &message.MessageExt{QueueOffset: int64(i + 1000)}
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
	defer cs.shutdown()

	count := 2000
	msgs := []*message.MessageExt{&message.MessageExt{}}
	for i := 0; i < count; i++ {
		go func() {
			cs.submitConsumeRequestLater(&consumeConcurrentlyRequest{
				messages:     msgs,
				processQueue: newProcessQueue(),
				messageQueue: &message.Queue{},
			})
		}()
	}

	for int(atomic.LoadInt32(&cs.consumer.(*mockConcurrentlyConsumer).consumeCount)) != count {
	}
}

func TestGetConsumingMessageQueue(t *testing.T) {
	cs := newTestConcurrentlyService(t)
	cs.start()
	defer cs.shutdown()

	mqs := []message.Queue{message.Queue{}, message.Queue{QueueID: 1}}
	cs.newProcessQueue(&mqs[0])
	cs.newProcessQueue(&mqs[1])

	mqs1 := cs.messageQueues()
	assert.Equal(t, 2, len(mqs))
	q01, q02 := mqs[0], mqs[1]
	q11, q12 := mqs1[0], mqs1[1]
	assert.True(t, (q01 == q11 || q01 == q12) && (q02 == q11 || q02 == q12))
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
