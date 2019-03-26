package consumer

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zjykzk/rocketmq-client-go/message"
)

const (
	defaultLockInterval time.Duration = 20 * time.Second
)

var (
	errProcessQueueNotLocked = errors.New("process queue is not locked")
)

type messageQueueLocker interface {
	Lock(broker string, mqs []message.Queue) error
	Unlock(mq message.Queue) error
}

type consumeOrderlyService struct {
	*baseConsumeService

	messageModel        Model
	mqLocker            messageQueueLocker
	checkLockInterval   time.Duration
	delayUnlockDuration time.Duration
}

type orderlyServiceConfig struct {
	consumeServiceConfig

	messageModel      Model
	mqLocker          messageQueueLocker
	checkLockInterval time.Duration
}

func newConsumeOrderlyService(conf orderlyServiceConfig) (*consumeOrderlyService, error) {
	if conf.mqLocker == nil {
		return nil, errors.New("new consume orderly service error:empty message queue locker")
	}

	c, err := newConsumeService(conf.consumeServiceConfig)
	if err != nil {
		return nil, err
	}

	if conf.checkLockInterval <= 0 {
		conf.checkLockInterval = defaultLockInterval
	}

	return &consumeOrderlyService{
		baseConsumeService: c,

		messageModel:        conf.messageModel,
		mqLocker:            conf.mqLocker,
		checkLockInterval:   conf.checkLockInterval,
		delayUnlockDuration: 20 * time.Second,
	}, nil
}

func (cs *consumeOrderlyService) start() {
	if cs.messageModel == Clustering {
		cs.startFunc(func() { cs.lockQueues() }, cs.checkLockInterval)
		cs.startFunc(cs.dropExpiredProcessQueues, time.Second*10)
	}
}

func (cs *consumeOrderlyService) lockQueues() (err error) {
	mqs := cs.getMQsGroupByBroker()
	for _, mq := range mqs {
		err = cs.mqLocker.Lock(mq.broker, mq.mqs)
	}
	return
}

type mqsOfBroker struct {
	broker string
	mqs    []message.Queue
}

func (cs *consumeOrderlyService) getMQsGroupByBroker() []mqsOfBroker {
	mqs := make([]mqsOfBroker, 0, 8)
	cs.baseConsumeService.processQueues.Range(func(k, _ interface{}) bool {
		mq := k.(message.Queue)
		for i := range mqs {
			q := &mqs[i]
			if q.broker == mq.BrokerName {
				q.mqs = append(q.mqs, mq)
				return true
			}
		}

		mqs = append(mqs, mqsOfBroker{broker: mq.BrokerName, mqs: []message.Queue{mq}})
		return true
	})
	return mqs
}

func (cs *consumeOrderlyService) insertNewMessageQueue(mq *message.Queue) (
	pq *processQueue, ok bool,
) {
	cpq := newOrderProcessQueue()
	_, ok = cs.processQueues.LoadOrStore(*mq, cpq)
	if ok {
		cs.logger.Infof("message queue:%s exist", mq)
		return nil, false
	}
	return &cpq.processQueue, true
}

func (cs *consumeOrderlyService) flowControl(_ *processQueue) bool {
	return false
}

func (cs *consumeOrderlyService) check(pq *processQueue) error {
	q := (*orderProcessQueue)(unsafe.Pointer(pq))
	if q.isLockedInBroker() {
		return nil
	}

	return errProcessQueueNotLocked
}

func (cs *consumeOrderlyService) dropExpiredProcessQueues() {
	oqs := make([]*orderProcessQueue, 0, 32)
	mqs := make([]message.Queue, 0, 32)
	cs.processQueues.Range(func(k, v interface{}) bool {
		pq := v.(*orderProcessQueue)
		if !pq.isPullExpired(cs.pullExpiredInterval) {
			return true // next
		}
		oqs, mqs = append(oqs, pq), append(mqs, k.(message.Queue))
		return true
	})

	for i := range oqs {
		cs.unlockProcessQueueInBroker(mqs[i], oqs[i], time.Second)
	}
}

func (cs *consumeOrderlyService) unlockProcessQueueInBroker(
	mq message.Queue, oq *orderProcessQueue, timeout time.Duration,
) bool {
	if !oq.tryLockConsume(timeout) {
		cs.logger.Warnf(
			"[WRONG] [%s] is consuming, so cannot unlock it in the broker. maybe handed for a while, unlock count:%d",
			mq.String(), oq.incTryUnlockTime(),
		)
		return false
	}

	if oq.messageSize() > 0 {
		cs.logger.Infof("%s unlock begin", mq.String())
		cs.scheduler.scheduleFuncAfter(
			func() { cs.logger.Info("[%s] unlock delay, executed", mq); cs.mqLocker.Unlock(mq) },
			cs.delayUnlockDuration,
		)
	} else {
		cs.mqLocker.Unlock(mq)
	}

	oq.unLockConsume()
	return true
}

func (cs *consumeOrderlyService) dropAndRemoveProcessQueue(mq *message.Queue) bool {
	if cs.messageModel == Clustering {
		// TODO
	}

	return cs.baseConsumeService.dropAndRemoveProcessQueue(mq)
}

func newOrderProcessQueue() *orderProcessQueue {
	return &orderProcessQueue{
		timeoutLocker: newTimeoutLocker(),
	}
}

const (
	unlockedInBroker = iota
	lockedInBroker
)

type orderProcessQueue struct {
	processQueue

	timeoutLocker *timeoutLocker

	lockedInBroker int32
	tryUnlockTimes uint32
}

func (q *orderProcessQueue) isLockedInBroker() bool {
	return atomic.LoadInt32(&q.lockedInBroker) == lockedInBroker
}

func (q *orderProcessQueue) lockInBroker() bool {
	return atomic.CompareAndSwapInt32(&q.lockedInBroker, unlockedInBroker, lockedInBroker)
}

func (q *orderProcessQueue) isPullExpired(timeout time.Duration) bool {
	return time.Since(q.lastPullTime) >= timeout
}

func (q *orderProcessQueue) tryLockConsume(timeout time.Duration) bool {
	return q.timeoutLocker.tryLock(timeout)
}

func (q *orderProcessQueue) unLockConsume() {
	q.timeoutLocker.unlock()
}

func (q *orderProcessQueue) incTryUnlockTime() uint32 {
	return atomic.AddUint32(&q.tryUnlockTimes, 1)
}
