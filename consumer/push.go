package consumer

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

var (
	defaultLastestConsumeTimestamp = time.Now().Add(-time.Minute * 30)
)

const (
	defaultThresholdCountOfQueue                    = 1000
	defaultThresholdSizeOfQueue                     = 100
	defaultThresholdCountOfTopic                    = -1
	defaultThresholdSizeOfTopic                     = -1
	defaultPullInterval               time.Duration = 0
	defaultConsumeBatchSize                         = 1
	defaultPullBatchSize                            = 32
	defaultPostSubscriptionWhenPull   bool          = false
	defaultConsumeTimeout                           = 15 * time.Minute
	defaultConsumeMessageBatchMaxSize               = 1
	defaultPushMaxReconsumeTimes                    = -1
)

// times defined
const (
	PullTimeDelayWhenException   = time.Second * 3
	PullTimeDelayWhenFlowControl = time.Millisecond * 50
	PullTimeDelayWhenPause       = time.Second
	BrokerSuspendMaxTime         = time.Second * 15
	ConsumerTimeoutWhenSuspend   = time.Second * 30
)

type consumerService interface {
	messageQueues() []message.Queue
	removeOldMessageQueue(mq *message.Queue) bool
	insertNewMessageQueue(mq *message.Queue) (*processQueue, bool)
	flowControl(*processQueue) bool
	check(*processQueue) error
}

type pullRequestDispatcher interface {
	submitRequestImmediately(r *pullRequest)
	submitRequestLater(r *pullRequest, delay time.Duration)
	shutdown()
}

// PushConsumer the consumer with push model
type PushConsumer struct {
	*consumer

	maxReconsumeTimes       int
	lastestConsumeTimestamp time.Time
	consumeTimeout          time.Duration

	thresholdCountOfQueue int
	thresholdSizeOfQueue  int
	thresholdCountOfTopic int
	thresholdSizeOfTopic  int

	pullInterval     time.Duration
	pullBatchSize    int
	consumeBatchSize int

	postSubscriptionWhenPull   bool
	consumeMessageBatchMaxSize int

	pause                 uint32
	consumeService        consumerService
	consumeServiceBuilder func() (consumerService, error)
	subscription          map[string]string

	queueControlFlowTotal uint32

	pullService pullRequestDispatcher
}

func newPushConsumer(group string, namesrvAddrs []string, logger log.Logger) *PushConsumer {
	pc := &PushConsumer{
		consumer: &consumer{
			logger:          logger,
			Config:          defaultConfig,
			Server:          rocketmq.Server{State: rocketmq.StateCreating},
			brokerSuggester: &brokerSuggester{table: make(map[string]int32, 32)},
		},
		maxReconsumeTimes:       defaultPushMaxReconsumeTimes,
		lastestConsumeTimestamp: defaultLastestConsumeTimestamp,
		consumeTimeout:          defaultConsumeTimeout,

		thresholdCountOfQueue: defaultThresholdCountOfQueue,
		thresholdSizeOfQueue:  defaultThresholdSizeOfQueue,
		thresholdCountOfTopic: defaultThresholdCountOfTopic,
		thresholdSizeOfTopic:  defaultThresholdSizeOfTopic,

		pullInterval:     defaultPullInterval,
		consumeBatchSize: defaultConsumeBatchSize,
		pullBatchSize:    defaultPullBatchSize,

		postSubscriptionWhenPull:   defaultPostSubscriptionWhenPull,
		consumeMessageBatchMaxSize: defaultConsumeMessageBatchMaxSize,

		subscription: make(map[string]string, 16),
	}
	pc.NameServerAddrs = namesrvAddrs
	pc.FromWhere = consumeFromLastOffset
	pc.MessageModel = Clustering
	pc.Typ = Pull
	pc.assigner = &Averagely{}
	pc.reblancer = pc
	pc.runnerInfo = pc.RunningInfo
	pc.GroupName = group

	pc.StartFunc = pc.start
	return pc
}

// NewConcurrentConsumer creates the push consumer consuming the message concurrently
func NewConcurrentConsumer(
	group string, namesrvAddrs []string, userConsumer ConcurrentlyConsumer, logger log.Logger,
) (
	pc *PushConsumer, err error,
) {
	if userConsumer == nil {
		return nil, errors.New("empty consumer service")
	}
	pc = newPushConsumer(group, namesrvAddrs, logger)

	pc.consumeServiceBuilder = func() (consumerService, error) {
		return newConsumeConcurrentlyService(concurrentlyServiceConfig{
			consumeServiceConfig: consumeServiceConfig{
				group:           group,
				logger:          logger,
				messageSendBack: pc,
				offseter:        pc.offseter,
			},
			consumeTimeout: pc.consumeTimeout,
			consumer:       userConsumer,
			batchSize:      pc.consumeBatchSize,
		})
	}
	return
}

func (pc *PushConsumer) start() error {
	pc.logger.Info("start pull consumer")
	err := pc.checkConfig()
	if err != nil {
		return err
	}

	pc.subscribe()

	err = pc.consumer.start()
	if err != nil {
		return err
	}

	pc.consumeService, err = pc.consumeServiceBuilder()
	if err != nil {
		pc.logger.Errorf("build consumer service error:%s", err)
		return err
	}

	pullService, err := newPullService(pullServiceConfig{
		messagePuller: pc,
		logger:        pc.logger,
	})
	if err != nil {
		return err
	}
	pc.pullService = pullService

	pc.buildShutdowner(pullService.shutdown)

	pc.updateTopicRouterInfoFromNamesrv()
	pc.registerFilter()
	pc.client.SendHeartbeat()
	pc.ReblanceQueue()
	pc.logger.Infof("start pull consumer:%s success", pc.GroupName)
	return nil
}

func (pc *PushConsumer) checkConfig() error {
	if len(pc.subscription) == 0 {
		return errors.New("empty subcription")
	}

	if pc.thresholdCountOfQueue < 1 || pc.thresholdCountOfQueue > 65535 {
		return errors.New("ThresholdCountOfQueue out of the range [1, 65535]")
	}

	if pc.thresholdSizeOfQueue < 1 || pc.thresholdSizeOfQueue > 1024 {
		return errors.New("ThresholdSizeOfQueue out of the range [1, 1024]")
	}

	thresholdCountOfTopic := pc.thresholdCountOfTopic
	if thresholdCountOfTopic != -1 && (thresholdCountOfTopic < 1 || thresholdCountOfTopic > 6553500) {
		return errors.New("ThresholdCountOfTopic out of the range [1, 6553500]")
	}

	thresholdSizeOfTopic := pc.thresholdSizeOfTopic
	if thresholdSizeOfTopic != -1 && (thresholdSizeOfTopic < 1 || thresholdSizeOfTopic > 102400) {
		return errors.New("ThresholdSizeOfTopic out of the range [1, 102400]")
	}

	if pc.pullInterval < 0 || pc.pullInterval > 65535 {
		return errors.New("PullInterval out of the range [0, 65535]")
	}

	if pc.consumeBatchSize < 1 || pc.consumeBatchSize > 1024 {
		return errors.New("ConsumeBatchSize out of the range [1, 1024]")
	}

	if pc.pullBatchSize < 1 || pc.pullBatchSize > 1024 {
		return errors.New("PullBatchSize out of the range [1, 1024]")
	}

	return nil
}

func (pc *PushConsumer) buildShutdowner(f func()) {
	shutdowner := &rocketmq.ShutdownCollection{}
	shutdowner.Add(
		rocketmq.ShutdownFunc(func() {
			pc.logger.Infof("shutdown PUSH consumer, group:%s, clientID:%s START", pc.GroupName, pc.ClientID)
		}),
		rocketmq.ShutdownFunc(f),
		pc.Shutdowner,
		rocketmq.ShutdownFunc(func() {
			pc.logger.Infof("shutdown PUSH consumer, group:%s, clientID:%s END", pc.GroupName, pc.ClientID)
		}),
	)

	pc.Shutdowner = shutdowner
}

func (pc *PushConsumer) subscribe() {
	if pc.MessageModel == Clustering {
		pc.consumer.Subscribe(retryTopic(pc.GroupName), subAll)
	}
}

func (pc *PushConsumer) updateTopicRouterInfoFromNamesrv() {
	for _, topic := range pc.subscribeData.Topics() {
		pc.client.UpdateTopicRouterInfoFromNamesrv(topic)
	}
}

// register the sql filter to the broker
func (pc *PushConsumer) registerFilter() {
	for _, d := range pc.subscribeData.Datas() {
		if IsTag(d.Typ) {
			continue
		}

		pc.client.RegisterFilter(pc.GroupName, d)
	}
}

// SendBack sends the message to the broker, the message will be consumed again after the at
// least time specified by the delayLevel
func (pc *PushConsumer) SendBack(m *message.Ext, delayLevel int, broker string) error {
	return pc.consumer.SendBack(m, delayLevel, pc.GroupName, broker)
}

func (pc *PushConsumer) reblance(topic string) {
	allQueues, newQueues, err := pc.reblanceQueue(topic)
	if err != nil {
		pc.logger.Errorf("reblance queue error:%s", err)
		return
	}
	if len(allQueues) == 0 {
		return
	}

	if pc.updateProcessTable(topic, newQueues) {
		pc.updateSubscribeVersion(topic)
		pc.updateThresholdOfQueue()
	}
}

func (pc *PushConsumer) updateProcessTable(topic string, mqs []*message.Queue) bool {
	tmpMQs := pc.consumeService.messageQueues()
	currentMQs := make([]*message.Queue, len(tmpMQs))
	for i := range currentMQs {
		currentMQs[i] = &tmpMQs[i]
	}

	changed := false
	// remove the mq not processed by the node
	for _, mq := range sub(currentMQs, mqs) {
		if pc.consumeService.removeOldMessageQueue(mq) {
			changed = true
		}
	}

	// insert new mq
	var pullRequests []pullRequest
	for _, mq := range sub(mqs, currentMQs) {
		pc.offseter.removeOffset(mq)
		offset, err := pc.computeWhereToPull(mq)
		if err != nil {
			pc.logger.Errorf("compute where to pull the message error:%s", err)
			continue
		}

		if pq, ok := pc.consumeService.insertNewMessageQueue(mq); ok {
			pc.logger.Infof("reblance: %s, new message queue added:%s", pc.Group(), mq)
			pullRequests = append(pullRequests, pullRequest{
				group:        pc.Group(),
				nextOffset:   offset,
				messageQueue: mq,
				processQueue: pq,
			})
			changed = true
		}
	}

	pc.dispatchPullRequest(pullRequests)
	return changed
}

func (pc *PushConsumer) dispatchPullRequest(reqs []pullRequest) {
	for i := range reqs {
		pc.pullService.submitRequestImmediately(&reqs[i])
	}
}

func sub(mqs1, mqs2 []*message.Queue) (r []*message.Queue) {
NEXT:
	for _, mq1 := range mqs1 {
		for _, mq2 := range mqs2 {
			if *mq1 == *mq2 {
				continue NEXT
			}
		}
		r = append(r, mq1)
	}
	return
}

func (pc *PushConsumer) updateSubscribeVersion(topic string) {
	data := pc.subscribeData.Get(topic)
	newVersion := time.Now().UnixNano() / int64(time.Millisecond)
	pc.logger.Infof(
		"[%s] reblance changed, update version from %d to %d",
		topic, data.Version, newVersion,
	)

	data.Version = newVersion
	pc.client.SendHeartbeat()
}

func (pc *PushConsumer) computeWhereToPull(mq *message.Queue) (offset int64, err error) {
	switch pc.FromWhere {
	case consumeFromLastOffset:
		return pc.computeFromLastOffset(mq)
	case consumeFromFirstOffset:
		return pc.computeFromFirstOffset(mq)
	case consumeFromTimestamp:
		return pc.computeFromTimestamp(mq)
	default:
		panic("unknow from type:" + pc.FromWhere.String())
	}
}

func (pc *PushConsumer) computeFromLastOffset(mq *message.Queue) (int64, error) {
	offset, err := pc.offseter.readOffset(mq, ReadOffsetFromStore)
	if err == nil {
		return offset, nil
	}

	pc.logger.Errorf("read LAST offset of %s, from the store error:%s", mq, err)
	if err != errOffsetNotExist {
		return 0, err
	}

	if strings.HasPrefix(mq.Topic, rocketmq.RetryGroupTopicPrefix) {
		return 0, nil
	}

	return pc.QueryMaxOffset(mq)
}

func (pc *PushConsumer) computeFromFirstOffset(mq *message.Queue) (int64, error) {
	offset, err := pc.offseter.readOffset(mq, ReadOffsetFromStore)
	if err == nil {
		return offset, nil
	}

	pc.logger.Errorf("read FIRST offset of %s, from the store error:%s", mq, err)
	if err == errOffsetNotExist {
		return 0, nil
	}

	return 0, err
}

func (pc *PushConsumer) computeFromTimestamp(mq *message.Queue) (int64, error) {
	offset, err := pc.offseter.readOffset(mq, ReadOffsetFromStore)
	if err == nil {
		return offset, nil
	}

	pc.logger.Errorf("read TIMESTAMP offset of %s, from the store error:%s", mq, err)
	if err != errOffsetNotExist {
		return 0, err
	}

	if strings.HasPrefix(mq.Topic, rocketmq.RetryGroupTopicPrefix) {
		return pc.QueryMaxOffset(mq)
	}

	return pc.searchOffset(mq)
}

func (pc *PushConsumer) searchOffset(mq *message.Queue) (int64, error) {
	var addr string
	broker := mq.BrokerName
	if r, err := pc.client.FindBrokerAddr(broker, rocketmq.MasterID, true); err == nil {
		addr = r.Addr
	} else {
		pc.logger.Errorf("find broker for error:%s", err)
		return 0, err
	}

	return pc.client.SearchOffsetByTimestamp(
		addr, mq.Topic, mq.QueueID, pc.lastestConsumeTimestamp, time.Second*3,
	)
}

func (pc *PushConsumer) updateThresholdOfQueue() {
	queueCount := len(pc.consumeService.messageQueues())
	if queueCount <= 0 {
		return
	}
	if pc.thresholdCountOfTopic != -1 {
		maxCountForQueue := pc.thresholdCountOfTopic / queueCount
		if maxCountForQueue < 1 {
			maxCountForQueue = 1
		}
		pc.thresholdCountOfQueue = maxCountForQueue
	}

	if pc.thresholdSizeOfTopic != -1 {
		maxSizeForQueue := pc.thresholdSizeOfTopic / queueCount
		if maxSizeForQueue < 1 {
			maxSizeForQueue = 1
		}
		pc.thresholdSizeOfQueue = maxSizeForQueue
	}
}

func (pc *PushConsumer) pull(r *pullRequest) {
	pq := r.processQueue
	if pq.isDropped() {
		pc.logger.Infof("pull request:%s is dropped", r)
		return
	}
	pq.updatePullTime(time.Now())

	if pc.consumer.CheckRunning() != nil {
		pc.logger.Infof("push consumer is not running, current:%s", pc.State)
		pc.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		return
	}

	if pc.isPause() {
		pc.pullService.submitRequestLater(r, PullTimeDelayWhenPause)
		pc.logger.Infof("push consumer is pausing")
		return
	}

	if pc.doesFlowControl(r) {
		return
	}

	if err := pc.consumeService.check(pq); err != nil {
		pc.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		pc.logger.Infof("consume service checking failed:%s", err)
		return
	}

	sub := pc.subscribeData.Get(r.messageQueue.Topic)
	if sub == nil {
		pc.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		pc.logger.Infof("cannot find subsciption %s", r.messageQueue.Topic)
		return
	}
}

func (pc *PushConsumer) doesFlowControl(r *pullRequest) bool {
	if pc.doCountFlowControl(r) {
		return true
	}

	if pc.doSizeFlowControl(r) {
		return true
	}

	if pc.doConsumeServiceFlowControl(r) {
		return true
	}
	return false
}

func (pc *PushConsumer) doCountFlowControl(r *pullRequest) bool {
	pq := r.processQueue
	cachedCount := pq.messageCount()
	if cachedCount <= int32(pc.thresholdCountOfQueue) {
		return false
	}

	pc.pullService.submitRequestLater(r, PullTimeDelayWhenFlowControl)
	pc.queueControlFlowTotal++

	if pc.queueControlFlowTotal%1000 == 0 {
		min, max := pq.offsetRange()
		pc.logger.Warnf(
			"COUNT FLOW CONTROL:the cached message count exceeds the threshold %d,"+
				"minOffset:%d,maxOffset:%d,count:%d, pull request:%s, flow controll total:%d",
			pc.thresholdCountOfQueue, min, max, cachedCount, r, pc.queueControlFlowTotal,
		)
	}
	return true
}

func (pc *PushConsumer) doSizeFlowControl(r *pullRequest) bool {
	pq := r.processQueue
	cachedSize := pq.messageSize()
	if cachedSize <= int64(pc.thresholdSizeOfQueue) {
		return false
	}

	pc.pullService.submitRequestLater(r, PullTimeDelayWhenFlowControl)
	pc.queueControlFlowTotal++

	if pc.queueControlFlowTotal%1000 == 0 {
		pc.logger.Warnf(
			"SIZE FLOW CONTROL:the cached message size exceeds the threshold %d,size:%dM, pull request:%s, flow controll total:%d",
			pc.thresholdSizeOfQueue, cachedSize>>20, r, pc.queueControlFlowTotal,
		)
	}
	return true
}

func (pc *PushConsumer) doConsumeServiceFlowControl(r *pullRequest) bool {
	pq := r.processQueue
	if !pc.consumeService.flowControl(pq) {
		return false
	}
	pc.pullService.submitRequestLater(r, PullTimeDelayWhenFlowControl)
	pc.queueControlFlowTotal++

	if pc.queueControlFlowTotal%1000 == 0 {
		min, max := pq.offsetRange()
		pc.logger.Warnf(
			"CONSUME SERVICE FLOW FLOW CONTROL:minOffset:%d,maxOffset:%d,pull request:%s,flow controll total:%d",
			min, max, r, pc.queueControlFlowTotal,
		)
	}

	return true
}

// Pause pause the consumer, this operation is thread-safe
func (pc *PushConsumer) Pause() {
	atomic.StoreUint32(&pc.pause, 1)
}

// UnPause un-pause the consumer, this operation is thread-safe
func (pc *PushConsumer) UnPause() {
	atomic.StoreUint32(&pc.pause, 0)
}

func (pc *PushConsumer) isPause() bool {
	return atomic.LoadUint32(&pc.pause) == 1
}
