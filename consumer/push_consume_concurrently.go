package consumer

import (
	"errors"
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/message"
)

// ConsumeConcurrentlyStatus consume concurrently result
type ConsumeConcurrentlyStatus int

// predefined consume concurrently result
const (
	ConcurrentlySuccess ConsumeConcurrentlyStatus = iota
	ReconsumeLater
)

func (s ConsumeConcurrentlyStatus) String() string {
	switch s {
	case ConcurrentlySuccess:
		return "ConcurrentlySuccess"
	case ReconsumeLater:
		return "ReconsumeLater"
	default:
		return "UNKNOW ConsumeConcurrentlyStatus"
	}
}

// ConcurrentlyContext consume concurrently context
type ConcurrentlyContext struct {
	MessageQueue *message.Queue
	// message cosnume retry strategy
	// -1, no retry, put into DLQ directly
	// 0, broker control retry frequency
	// >0, client control retry frequency
	DelayLevelWhenNextConsume int
	// the index of the message, any message with the index greater than this one is consumed failed
	AckIndex int
}

// ConcurrentlyConsumer consumer consumes the messages concurrently
type ConcurrentlyConsumer interface {
	Consume(messages []*message.Ext, ctx *ConcurrentlyContext) ConsumeConcurrentlyStatus
}

type consumeConcurrentlyRequest struct {
	messages     []*message.Ext
	processQueue *processQueue
	messageQueue *message.Queue
}

type consumeConcurrentlyService struct {
	*baseConsumeService

	cleanExpiredInterval time.Duration

	consumer        ConcurrentlyConsumer
	consumeTimeout  time.Duration
	concurrentCount int
	consumeQueue    chan *consumeConcurrentlyRequest
	batchSize       int
	maxSpan         int

	consumeLaterInterval time.Duration
}

type concurrentlyServiceConfig struct {
	consumeServiceConfig

	consumer             ConcurrentlyConsumer
	consumeTimeout       time.Duration
	concurrentCount      int
	batchSize            int
	maxSpan              int
	cleanExpiredInterval time.Duration
}

func newConsumeConcurrentlyService(conf concurrentlyServiceConfig) (
	*consumeConcurrentlyService, error,
) {
	if conf.consumer == nil {
		return nil, errors.New("new consumer concurrently service error:empty consumer")
	}

	if conf.consumeTimeout <= 0 {
		return nil, errors.New("new consumer concurrently service error:empty consume timeout")
	}

	if conf.offseter == nil {
		return nil, errors.New("new consumer concurrently service error:empty offset updater")
	}

	if conf.concurrentCount <= 0 {
		conf.concurrentCount = 64
	}

	if conf.cleanExpiredInterval <= 0 {
		conf.cleanExpiredInterval = time.Second * 20
	}

	if conf.batchSize <= 0 {
		conf.batchSize = 1
	}

	if conf.maxSpan <= 0 {
		conf.maxSpan = 2000
	}

	c, err := newConsumeService(conf.consumeServiceConfig)
	if err != nil {
		return nil, err
	}

	pc := &consumeConcurrentlyService{
		baseConsumeService:   c,
		consumer:             conf.consumer,
		concurrentCount:      conf.concurrentCount,
		consumeQueue:         make(chan *consumeConcurrentlyRequest, conf.concurrentCount*3/2),
		consumeTimeout:       conf.consumeTimeout,
		batchSize:            conf.batchSize,
		maxSpan:              conf.maxSpan,
		cleanExpiredInterval: conf.cleanExpiredInterval,
		consumeLaterInterval: time.Second,
	}

	return pc, nil
}

func (cs *consumeConcurrentlyService) start() {
	cs.startFunc(cs.clearExpiredMessage, cs.cleanExpiredInterval)
	cs.startConsume()
	cs.logger.Info("consume concurrently STARTED")
}

func (cs *consumeConcurrentlyService) shutdown() {
	cs.logger.Info("shutdown consume concurrently START")
	cs.baseConsumeService.shutdown()
	cs.logger.Info("shutdown consume concurrently END")
}

func (cs *consumeConcurrentlyService) startConsume() {
	for i := 0; i < cs.concurrentCount; i++ {
		cs.wg.Add(1)
		go func() {
			for {
				select {
				case <-cs.exitChan:
					cs.wg.Done()
					return
				case r := <-cs.consumeQueue:
					cs.consume(r)
				}
			}
		}()
	}
}

func (cs *consumeConcurrentlyService) consume(r *consumeConcurrentlyRequest) {
	processQueue := r.processQueue
	if processQueue.isDropped() {
		cs.logger.Infof("process queue is dropped:%s", r.messageQueue)
		return
	}

	ctx := &ConcurrentlyContext{MessageQueue: r.messageQueue}
	cs.resetRetryTopic(r.messages)
	begin := time.Now()
	status := cs.consumer.Consume(r.messages[:], ctx)
	consumeRT := time.Since(begin)
	if consumeRT > cs.consumeTimeout {
		cs.logger.Infof("consume timeout") // TODO
	}

	if processQueue.isDropped() {
		cs.logger.Warnf(
			"processQueue is dropped without process consume result. messageQueue=%v, msgs=%v",
			r.messageQueue, r.messages,
		)
		return
	}
	cs.processConsumeResult(status, ctx, r)
}

func (cs *consumeConcurrentlyService) processConsumeResult(
	status ConsumeConcurrentlyStatus, ctx *ConcurrentlyContext, r *consumeConcurrentlyRequest,
) {
	// TODO statistic

	failedIndex := ctx.AckIndex + 1
	if status == ReconsumeLater {
		failedIndex = 0
	}

	var removedMsgs []*message.Ext
	switch cs.messageModel {
	case BroadCasting:
		removedMsgs = cs.processBroadcasting(failedIndex, r)
	case Clustering:
		removedMsgs = cs.processClustering(failedIndex, ctx, r)
	default:
		cs.logger.Errorf("unknow model:%d", cs.messageModel)
		return
	}

	r.processQueue.removeMessages(removedMsgs)

	if !r.processQueue.isDropped() {
		cs.offseter.updateOffsetIfGreater(r.messageQueue, r.processQueue.queueOffsetToConsume())
	}
}

func (cs *consumeConcurrentlyService) processBroadcasting(
	failedIndex int, r *consumeConcurrentlyRequest,
) (
	removedMsgs []*message.Ext,
) {
	for _, m := range r.messages[failedIndex:] {
		cs.logger.Warnf("broadcasting, the message consumed failed and drop it, %s", m.String())
	}
	removedMsgs = r.messages
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (cs *consumeConcurrentlyService) processClustering(
	failedIndex int, ctx *ConcurrentlyContext, r *consumeConcurrentlyRequest,
) (
	removedMsgs []*message.Ext,
) {
	removedMsgs = make([]*message.Ext, failedIndex, len(r.messages))
	copy(removedMsgs, r.messages[:failedIndex])

	consumeFailedMsgs := r.messages[failedIndex:]
	sendbackFailedMsgs := make([]*message.Ext, 0, len(consumeFailedMsgs))
	delayLevel, broker := ctx.DelayLevelWhenNextConsume, ctx.MessageQueue.BrokerName
	for _, m := range consumeFailedMsgs {
		err := cs.messageSendBack.SendBack(m, delayLevel, broker)
		if err == nil {
			removedMsgs = append(removedMsgs, m)
			continue
		}

		m.ReconsumeTimes++
		sendbackFailedMsgs = append(sendbackFailedMsgs, m)
		cs.logger.Errorf("sendback consuming-failed message failed:%s", err)
	}

	if len(sendbackFailedMsgs) > 0 {
		r.messages = sendbackFailedMsgs
		cs.submitConsumeRequestLater(r)
	}

	return
}

func (cs *consumeConcurrentlyService) submitConsumeRequest(
	messages []*message.Ext, processQueue *processQueue, messageQueue *message.Queue,
) {
	count, batchSize := len(messages), cs.batchSize
	for i := 0; i < count; i += batchSize {
		r := &consumeConcurrentlyRequest{
			messages:     messages[i:min(count, i+batchSize)],
			processQueue: processQueue,
			messageQueue: messageQueue,
		}

		select {
		case cs.consumeQueue <- r:
		default:
			r.messages = messages[i:]
			cs.submitConsumeRequestLater(r)
			return
		}
	}
}

func (cs *consumeConcurrentlyService) submitConsumeRequestLater(r *consumeConcurrentlyRequest) {
	cs.scheduler.scheduleFuncAfter(func() { cs.consumeQueue <- r }, cs.consumeLaterInterval)
}

func (cs *consumeConcurrentlyService) newProcessQueue(mq *message.Queue) *processQueue {
	cpq := &concurrentProcessQueue{}
	cs.processQueues.LoadOrStore(*mq, cpq)
	return &cpq.processQueue
}

func (cs *consumeConcurrentlyService) putNewMessageQueue(mq *message.Queue) (*processQueue, bool) {
	cpq := &concurrentProcessQueue{}
	pq, loaded := cs.processQueues.LoadOrStore(*mq, cpq)
	if loaded {
		return nil, false
	}
	return &pq.(*concurrentProcessQueue).processQueue, true
}

func (cs *consumeConcurrentlyService) clearExpiredMessage() {
	queues := make([]*concurrentProcessQueue, 0, 32)
	cs.processQueues.Range(func(_, v interface{}) bool {
		queues = append(queues, v.(*concurrentProcessQueue))
		return true
	})

	for _, q := range queues {
		for i := 0; i < 16; i++ {
			m, ok := q.minOffsetMessage()
			if !ok {
				break
			}

			startConsumeTime, _ := m.GetConsumeStartTimestamp()
			if time.Now().UnixNano()-startConsumeTime < int64(cs.consumeTimeout) {
				break
			}

			err := cs.messageSendBack.SendBack(m, 3, "") // broker is empty
			if err != nil {
				cs.logger.Errorf("send message:%v failed:%s", m.String(), err)
				continue
			}

			if !q.removeIfMinOffset(m.QueueOffset) {
				cs.logger.Errorf("remove message:%v from q failed", m.String())
			}
		}
	}
}

func (cs *consumeConcurrentlyService) insertNewMessageQueue(mq *message.Queue) (
	pq *processQueue, ok bool,
) {
	cpq := &concurrentProcessQueue{}
	_, ok = cs.processQueues.LoadOrStore(*mq, cpq)
	if ok {
		cs.logger.Infof("message queue:%s exist", mq)
		return nil, false
	}
	return &cpq.processQueue, true
}

func (cs *consumeConcurrentlyService) flowControl(q *processQueue) bool {
	min, max := q.offsetRange()
	return int(max-min) > cs.maxSpan
}

func (cs *consumeConcurrentlyService) check(*processQueue) error {
	return nil // DO NOTHING
}

func (cs *consumeConcurrentlyService) dropAndClear(mq *message.Queue) error {
	v, ok := cs.processQueues.Load(*mq)
	if !ok {
		return errors.New("no process table in the concurrent service")
	}

	q := v.(*concurrentProcessQueue)
	q.drop()
	q.clear()

	return nil
}

func (cs *consumeConcurrentlyService) consumeMessageDirectly(
	msg *message.Ext, broker string,
) client.ConsumeMessageDirectlyResult {
	ret := client.ConsumeMessageDirectlyResult{AutoCommit: true, Order: false}

	msgs := []*message.Ext{msg}
	cs.resetRetryTopic(msgs)

	ctx := &ConcurrentlyContext{
		MessageQueue: &message.Queue{BrokerName: broker, Topic: msg.Topic, QueueID: msg.QueueID},
	}

	cs.logger.Infof("consume message DIRECTLY message:%s", msg)

	start := time.Now()

	status := cs.consumer.Consume(msgs, ctx)
	if status == ConcurrentlySuccess {
		ret.Result = client.Success
	} else if status == ReconsumeLater {
		ret.Result = client.Later
	}

	ret.TimeCost = time.Since(start)

	cs.logger.Infof("consume message DIRECTLY message result:%#v", ret)

	return ret
}

func (cs *consumeConcurrentlyService) properties() map[string]string {
	return map[string]string{
		"PROP_CONSUMEORDERLY": "false",
	}
}

type concurrentProcessQueue struct {
	processQueue
}

func (cpq *concurrentProcessQueue) minOffsetMessage() (m *message.Ext, ok bool) {
	cpq.RLock()
	if cpq.messages.Size() > 0 {
		_, v := cpq.messages.First()
		m = v.(*message.Ext)
		ok = true
	}
	cpq.RUnlock()
	return
}

func (cpq *concurrentProcessQueue) removeIfMinOffset(of int64) (ok bool) {
	cpq.Lock()
	if cpq.messages.Size() > 0 {
		_, v := cpq.messages.First()
		if of == v.(*message.Ext).QueueOffset {
			cpq.messages.Remove(offset(of))
		}
		ok = true
	}
	cpq.Unlock()
	return
}

func (cpq *concurrentProcessQueue) clear() {
	cpq.Lock()
	cpq.messages.Clear()
	cpq.nextQueueOffset = 0
	cpq.Unlock()
}
