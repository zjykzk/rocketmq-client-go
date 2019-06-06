package consumer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/message"
)

const (
	defaultCheckLockInterval          = 20 * time.Second
	defaultLockedTimeInBroker         = 30 * time.Second
	defaultMaxConsumeContinuouslyTime = time.Minute
	defaultBatchSize                  = 1
)

var (
	errProcessQueueNotLocked = errors.New("process queue is not locked")
)

type messageQueueLocker interface {
	Lock(broker string, mqs []message.Queue) ([]message.Queue, error)
	Unlock(mq message.Queue) error
}

// ConsumeOrderlyStatus consume orderly result
type ConsumeOrderlyStatus int

// predefined consume concurrently result
const (
	OrderlySuccess ConsumeOrderlyStatus = iota
	SuspendCurrentQueueMoment
)

// OrderlyContext consume orderly context
type OrderlyContext struct {
	Queue                   *message.Queue
	autoCommit              bool
	SupsendCurrentQueueTime time.Duration
}

// OrderlyConsumer consume orderly logic
type OrderlyConsumer interface {
	Consume(messages []*message.Ext, ctx *OrderlyContext) ConsumeOrderlyStatus
}

// consumeOrderlyService the service to consume the message orderly
//
// for one consume queue, there is only one goroutine to consume
type consumeOrderlyService struct {
	*baseConsumeService

	batchSize       int
	consumer        OrderlyConsumer
	requestsOfQueue sync.Map // message queue -> request channel

	messageModel               Model
	mqLocker                   messageQueueLocker
	checkLockInterval          time.Duration
	unlockDelay                time.Duration
	lockedTimeInBroker         time.Duration
	maxConsumeContinuouslyTime time.Duration
	maxReconsumeTimes          int
}

type orderlyServiceConfig struct {
	consumeServiceConfig

	messageModel               Model
	mqLocker                   messageQueueLocker
	checkLockInterval          time.Duration
	lockedTimeInBroker         time.Duration
	maxConsumeContinuouslyTime time.Duration
	maxReconsumeTimes          int

	batchSize int
	consumer  OrderlyConsumer
}

func newConsumeOrderlyService(conf orderlyServiceConfig) (*consumeOrderlyService, error) {
	if conf.mqLocker == nil {
		return nil, errors.New("new consume orderly service error:empty message queue locker")
	}

	if conf.consumer == nil {
		return nil, errors.New("new consume orderly service error:empty orderly consumer")
	}

	c, err := newConsumeService(conf.consumeServiceConfig)
	if err != nil {
		return nil, err
	}

	if conf.checkLockInterval <= 0 {
		conf.checkLockInterval = defaultCheckLockInterval
	}

	if conf.lockedTimeInBroker <= 0 {
		conf.lockedTimeInBroker = defaultLockedTimeInBroker
	}

	if conf.maxConsumeContinuouslyTime <= 0 {
		conf.maxConsumeContinuouslyTime = defaultMaxConsumeContinuouslyTime
	}

	if conf.batchSize <= 0 {
		conf.batchSize = defaultBatchSize
	}

	return &consumeOrderlyService{
		baseConsumeService: c,

		batchSize:                  conf.batchSize,
		consumer:                   conf.consumer,
		messageModel:               conf.messageModel,
		mqLocker:                   conf.mqLocker,
		checkLockInterval:          conf.checkLockInterval,
		lockedTimeInBroker:         conf.lockedTimeInBroker,
		unlockDelay:                20 * time.Second,
		maxConsumeContinuouslyTime: conf.maxConsumeContinuouslyTime,
		maxReconsumeTimes:          conf.maxReconsumeTimes,
	}, nil
}

func (cs *consumeOrderlyService) start() {
	if cs.messageModel == Clustering {
		cs.startFunc(func() { cs.lockQueues() }, cs.checkLockInterval)
		cs.startFunc(cs.dropPullExpiredProcessQueues, time.Second*10)
	}
}

func (cs *consumeOrderlyService) lockQueues() (err error) {
	mqs := cs.getMQsGroupByBroker()
	var lockedMQs []message.Queue
	for _, mq := range mqs {
		lockedMQs, err = cs.mqLocker.Lock(mq.broker, mq.mqs)
		if err != nil {
			continue
		}

		for _, q := range lockedMQs {
			oq, ok := cs.getOrderProcessQueue(q)
			if !ok {
				continue
			}
			oq.lockInBroker(time.Now().UnixNano())
		}
	}
	return
}
func (cs *consumeOrderlyService) getOrderProcessQueue(mq message.Queue) (*orderProcessQueue, bool) {
	pq, ok := cs.processQueues.Load(mq)
	if !ok {
		return nil, false
	}

	return pq.(*orderProcessQueue), true
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
	lockedMQs, err := cs.mqLocker.Lock(mq.BrokerName, []message.Queue{*mq})
	if err != nil {
		cs.logger.Errorf("lock the message queue:%s, error:%s", mq, err)
		return nil, false
	}

	if len(lockedMQs) == 0 {
		cs.logger.Errorf("lock the message queue:%s failed", mq)
		return nil, false
	}

	cpq := newOrderProcessQueue()
	_, ok = cs.processQueues.LoadOrStore(*mq, cpq)
	if ok {
		cs.logger.Infof("message queue:%s exist", mq)
		return nil, false
	}

	cpq.lockInBroker(time.Now().UnixNano())
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

// this operation is suspicious of the intentions
// maybe it is patch for missing pull request
func (cs *consumeOrderlyService) dropPullExpiredProcessQueues() {
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

	for _, oq := range oqs {
		oq.drop()
	}

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

	if oq.messageCount() > 0 {
		cs.logger.Infof("%s unlock begin", mq.String())
		cs.scheduler.scheduleFuncAfter(
			func() { cs.logger.Infof("[%s] unlock delay, executed", mq.String()); cs.mqLocker.Unlock(mq) },
			cs.unlockDelay,
		)
	} else {
		cs.mqLocker.Unlock(mq)
	}

	oq.unLockConsume()
	return true
}

func (cs *consumeOrderlyService) dropAndRemoveProcessQueue(mq *message.Queue) bool {
	if cs.messageModel != Clustering {
		return cs.baseConsumeService.dropAndRemoveProcessQueue(mq)
	}

	v, ok := cs.processQueues.Load(*mq)
	if !ok {
		return false
	}

	pq := v.(*orderProcessQueue)
	pq.drop()
	if cs.unlockProcessQueueInBroker(*mq, pq, time.Second) {
		cs.processQueues.Delete(*mq)
		return true
	}

	return false
}

type consumeOrderlyRequest struct {
	processQueue *orderProcessQueue
	messageQueue *message.Queue
}

func (cs *consumeOrderlyService) consume(r *consumeOrderlyRequest) {
	pq, mq := r.processQueue, r.messageQueue
	if pq.isDropped() {
		cs.logger.Infof("[%s]'s process queue is dropped", mq)
		return
	}

	if cs.messageModel == Clustering &&
		(!pq.isLockedInBroker() || pq.isLockedInBrokerExpired(cs.lockedTimeInBroker)) {
		cs.lockAndConsumeLater(r, 100*time.Millisecond)
		return
	}

	start := time.Now()
	for {
		msgs := pq.takeMessage(cs.batchSize)
		if len(msgs) == 0 {
			break
		}

		pq.lockConsume()
		if pq.isDropped() { // the lock operation may supsend the current goroutine
			cs.logger.Infof("[%s] process queue is DROPPED after consume locker", mq)
			pq.unLockConsume()
			break
		}

		ctx := &OrderlyContext{autoCommit: true, Queue: mq, SupsendCurrentQueueTime: -1}
		status := cs.consumer.Consume(msgs, ctx)
		pq.unLockConsume()

		if status != OrderlySuccess {
			cs.logger.Warnf("consumer returned status is not OK %d", status)
		}

		// TODO statistic
		cs.processConsumeResult(msgs, status, ctx, r)

		if pq.isDropped() { // the consume operation may cost too much
			cs.logger.Infof("[%s] process queue is DROPPED", mq)
			break
		}

		if !cs.isLockedInBroker(r, start) {
			cs.lockAndConsumeLater(r, 10*time.Millisecond)
			break
		}

		consumeTime := time.Since(start)
		if consumeTime >= cs.maxConsumeContinuouslyTime {
			cs.logger.Infof(
				"[%s] CONSUME TOO LONG %s, max:%s, try later", consumeTime, cs.maxConsumeContinuouslyTime, mq,
			)
			cs.submitRequestLater(r, 10*time.Millisecond)
			break
		}
	}
}

func (cs *consumeOrderlyService) lockAndConsumeLater(r *consumeOrderlyRequest, delay time.Duration) {
	cs.scheduler.scheduleFuncAfter(func() {
		mqs, err := cs.mqLocker.Lock(r.messageQueue.BrokerName, []message.Queue{*r.messageQueue})
		delay := 3 * time.Second // delay for the process queue is filled with message
		if len(mqs) == 1 && err == nil {
			delay = 10 * time.Millisecond
		}
		cs.submitRequestLater(r, delay)
	}, delay)
}

func (cs *consumeOrderlyService) isLockedInBroker(r *consumeOrderlyRequest, start time.Time) bool {
	pq, mq := r.processQueue, r.messageQueue

	isClustering := cs.messageModel == Clustering
	if isClustering && !pq.isLockedInBroker() {
		cs.logger.Infof("[%s] is NOT LOCKED, try later", mq)
		return false
	}

	if isClustering && pq.isLockedInBrokerExpired(cs.lockedTimeInBroker) {
		cs.logger.Infof("[%s] is LOCKED EXPIRED, try later", mq)
		return false
	}

	return true
}

func (cs *consumeOrderlyService) submitRequestLater(r *consumeOrderlyRequest, delay time.Duration) {
	cs.scheduler.scheduleFuncAfter(func() { cs.submitRequest(r) }, delay)
}

func (cs *consumeOrderlyService) submitRequest(r *consumeOrderlyRequest) {
	requests, exist := cs.requestsOfQueue.Load(*r.messageQueue)
	if !exist {
		requests = make(chan *consumeOrderlyRequest, 8) // 8 is just experience value
		requests, exist = cs.requestsOfQueue.LoadOrStore(*r.messageQueue, requests)
	}

	ch := requests.(chan *consumeOrderlyRequest)

	if !exist {
		go cs.startConsume(ch)
	}

	select {
	case ch <- r:
	default:
		cs.logger.Warnf("submit request failed since the channel is full, size:%d", len(ch))
	}
}

func (cs *consumeOrderlyService) startConsume(requests chan *consumeOrderlyRequest) {
	cs.wg.Add(1)
	for {
		select {
		case r := <-requests:
			cs.consume(r)
		case <-cs.exitChan:
			cs.wg.Done()
			return
		}
	}
}

func (cs *consumeOrderlyService) processConsumeResult(
	msgs []*message.Ext, status ConsumeOrderlyStatus, ctx *OrderlyContext, req *consumeOrderlyRequest,
) (continueConsume bool) {
	var commitOffset int64 = -1
	switch status {
	case OrderlySuccess:
		commitOffset = cs.processSuccess(ctx.autoCommit, req.processQueue)
		continueConsume = true
	case SuspendCurrentQueueMoment:
		if cs.processSuspend(msgs, status, ctx, req) {
			continueConsume = false
		}
	}

	if commitOffset >= 0 && !req.processQueue.isDropped() {
		cs.offseter.updateOffset(req.messageQueue, commitOffset+1)
	}
	return
}

func (cs *consumeOrderlyService) processSuccess(isAutoCommit bool, oq *orderProcessQueue) int64 {
	// TODO statistic suc

	if !isAutoCommit {
		return 0
	}

	return oq.commit()
}

func (cs *consumeOrderlyService) processSuspend(
	msgs []*message.Ext, status ConsumeOrderlyStatus, ctx *OrderlyContext, req *consumeOrderlyRequest,
) (
	suspend bool,
) {
	// TODO statistic failed
	if cs.sendBackIfConsumeTooManyTimes(msgs, req.messageQueue.BrokerName) {
		suspend = true
	}

	if cs.incReconsumeTimesIfNotOverLimit(msgs) {
		suspend = true
	}

	if suspend {
		req.processQueue.reconsume()
		cs.submitRequestLater(req, ctx.SupsendCurrentQueueTime)
	} else if ctx.autoCommit {
		req.processQueue.commit()
	}

	return
}

func (cs *consumeOrderlyService) sendBackIfConsumeTooManyTimes(
	msgs []*message.Ext, broker string,
) (
	hasFailed bool,
) {
	for _, m := range msgs {
		if int(m.ReconsumeTimes) < cs.maxReconsumeTimes {
			continue
		}

		err := cs.messageSendBack.SendBack(m, 3+int(m.ReconsumeTimes), broker)
		if err != nil {
			hasFailed = true
			m.ReconsumeTimes++
		}
	}
	return
}

func (cs *consumeOrderlyService) incReconsumeTimesIfNotOverLimit(msgs []*message.Ext) (any bool) {
	for _, m := range msgs {
		if int(m.ReconsumeTimes) < cs.maxReconsumeTimes {
			m.ReconsumeTimes++
			any = true
		}
	}
	return
}

func (cs *consumeOrderlyService) submitConsumeRequest(
	_ []*message.Ext, pq *processQueue, mq *message.Queue,
) {
	cs.submitRequest(&consumeOrderlyRequest{
		processQueue: (*orderProcessQueue)(unsafe.Pointer(pq)),
		messageQueue: mq,
	})
}

func (cs *consumeOrderlyService) dropAndClear(mq *message.Queue) error {
	v, ok := cs.processQueues.Load(*mq)
	if !ok {
		return errors.New("no process table in the order service")
	}

	q := v.(*orderProcessQueue)
	q.clear()
	q.drop()

	return nil
}

func (cs *consumeOrderlyService) consumeMessageDirectly(
	msg *message.Ext, broker string,
) client.ConsumeMessageDirectlyResult {
	ret := client.ConsumeMessageDirectlyResult{AutoCommit: true, Order: true}

	msgs := []*message.Ext{msg}
	cs.resetRetryTopic(msgs)

	ctx := &OrderlyContext{
		Queue: &message.Queue{BrokerName: broker, Topic: msg.Topic, QueueID: msg.QueueID},
	}

	cs.logger.Infof("consume message DIRECTLY message:%s", msg)

	start := time.Now()

	status := cs.consumer.Consume(msgs, ctx)
	if status == OrderlySuccess {
		ret.Result = client.Success
	} else if status == SuspendCurrentQueueMoment {
		ret.Result = client.Later
	}

	ret.TimeCost = time.Since(start)

	cs.logger.Infof("consume message DIRECTLY message result:%v", ret)

	return ret
}

func (cs *consumeOrderlyService) properties() map[string]string {
	return map[string]string{
		"PROP_CONSUMEORDERLY": "true",
	}
}

func newOrderProcessQueue() *orderProcessQueue {
	return &orderProcessQueue{
		timeoutLocker:     newTimeoutLocker(),
		consumingMessages: make(map[offset]*message.Ext, 8),
	}
}

const (
	unlockedInBroker = iota
	lockedInBroker
)

type orderProcessQueue struct {
	processQueue
	consumingMessages map[offset]*message.Ext

	timeoutLocker *timeoutLocker

	lastLockTime   int64 // unixnano
	lockedInBroker int32
	tryUnlockTimes uint32
}

func (q *orderProcessQueue) isLockedInBroker() bool {
	return atomic.LoadInt32(&q.lockedInBroker) == lockedInBroker
}

func (q *orderProcessQueue) lockInBroker(timeNano int64) bool {
	ok := atomic.CompareAndSwapInt32(&q.lockedInBroker, unlockedInBroker, lockedInBroker)
	atomic.StoreInt64(&q.lastLockTime, timeNano)
	return ok
}

func (q *orderProcessQueue) isLockedInBrokerExpired(timeout time.Duration) bool {
	return time.Now().UnixNano()-atomic.LoadInt64(&q.lastLockTime) >= int64(timeout)
}

func (q *orderProcessQueue) isPullExpired(timeout time.Duration) bool {
	return time.Now().UnixNano()-atomic.LoadInt64(&q.lastPullTime) >= int64(timeout)
}

func (q *orderProcessQueue) lockConsume() {
	q.timeoutLocker.lock()
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

func (q *orderProcessQueue) takeMessage(count int) []*message.Ext {
	msgs := make([]*message.Ext, 0, count)
	q.Lock()
	for i := 0; i < count; i++ {
		k, m := q.messages.First()
		if k == nil { // no message
			break
		}

		msgs = append(msgs, m.(*message.Ext))
		q.messages.Remove(k)
		q.consumingMessages[k.(offset)] = m.(*message.Ext)
	}
	q.Unlock()
	return msgs
}

func (q *orderProcessQueue) reconsume() {
	q.Lock()
	for of, m := range q.consumingMessages {
		q.messages.Put(of, m)
	}
	q.clearConsumingMessages()
	q.Unlock()
}

func (q *orderProcessQueue) commit() int64 {
	var backup map[offset]*message.Ext
	q.Lock()
	backup = q.consumingMessages
	q.clearConsumingMessages()
	q.Unlock()

	atomic.AddInt32(&q.msgCount, -int32(len(backup)))

	maxOffset, bodySize := offset(-1), int64(0)
	for of, m := range backup {
		if maxOffset < of {
			maxOffset = of
		}

		bodySize += int64(len(m.Body))
	}

	atomic.AddInt64(&q.msgSize, -bodySize)

	return int64(maxOffset)
}

func (q *orderProcessQueue) clearConsumingMessages() {
	q.consumingMessages = make(map[offset]*message.Ext, 8)
}

func (q *orderProcessQueue) clear() {
	q.Lock()
	q.consumingMessages = nil
	q.messages.Clear()
	q.nextQueueOffset = 0
	q.Unlock()

	atomic.StoreInt64(&q.msgSize, 0)
	atomic.StoreInt32(&q.msgCount, 0)
}
