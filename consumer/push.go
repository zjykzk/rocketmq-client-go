package consumer

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
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

type pullRequestDispatcher interface {
	submitRequestImmediately(r *pullRequest)
	submitRequestLater(r *pullRequest, delay time.Duration)
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
	consumeService        consumeService
	consumeServiceBuilder func() (consumeService, error)

	sched *scheduler

	queueControlFlowTotal uint32

	pullService pullRequestDispatcher
}

// NewConcurrentConsumer creates the push consumer consuming the message concurrently
func NewConcurrentConsumer(
	group string, namesrvAddrs []string, userConsumer ConcurrentlyConsumer, logger log.Logger,
) (
	c *PushConsumer, err error,
) {
	if userConsumer == nil {
		return nil, errors.New("empty consumer service")
	}
	c = newPushConsumer(group, namesrvAddrs, logger)

	c.consumeServiceBuilder = func() (consumeService, error) {
		cs, err := newConsumeConcurrentlyService(concurrentlyServiceConfig{
			consumeServiceConfig: consumeServiceConfig{
				group:           group,
				logger:          logger,
				messageSendBack: c,
				offseter:        c.offsetStorer,
			},
			consumeTimeout: c.consumeTimeout,
			consumer:       userConsumer,
			batchSize:      c.consumeBatchSize,
		})
		if err != nil {
			return nil, err
		}

		cs.start()

		shutdowner := &rocketmq.ShutdownCollection{}
		shutdowner.AddFirstFuncs(cs.shutdown)
		shutdowner.AddLast(c.Shutdowner)

		c.Shutdowner = shutdowner
		return cs, nil
	}
	return
}

func newPushConsumer(group string, namesrvAddrs []string, logger log.Logger) *PushConsumer {
	c := &PushConsumer{
		consumer: &consumer{
			logger:          logger,
			Config:          defaultConfig,
			Server:          rocketmq.Server{State: rocketmq.StateCreating},
			brokerSuggester: &brokerSuggester{table: make(map[string]int32, 32)},
			subscribeData:   client.NewSubcribeTable(),
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

		sched: newScheduler(2),
	}
	c.NameServerAddrs = namesrvAddrs
	c.FromWhere = ConsumeFromLastOffset
	c.MessageModel = Clustering
	c.Typ = Push
	c.assigner = &Averagely{}
	c.reblancer = c
	c.runnerInfo = c.RunningInfo
	c.GroupName = group

	c.StartFunc = c.start
	return c
}

func (c *PushConsumer) start() error {
	c.logger.Info("start pull consumer")
	err := c.checkConfig()
	if err != nil {
		return fmt.Errorf("push consumer configure error:%s", err)
	}

	c.subscribeRetryTopic()

	err = c.consumer.start()
	if err != nil {
		return err
	}

	shutdowner := &rocketmq.ShutdownCollection{}
	shutdowner.AddLastFuncs(c.shutdownStart)

	pullService, err := newPullService(pullServiceConfig{
		messagePuller: c,
		logger:        c.logger,
	})
	if err != nil {
		return err
	}
	c.pullService = pullService
	shutdowner.AddLastFuncs(pullService.shutdown)

	consumeService, err := c.consumeServiceBuilder()
	if err != nil {
		c.logger.Errorf("build consumer service error:%s", err)
		return err
	}
	c.consumeService = consumeService

	shutdowner.AddLastFuncs(c.sched.shutdown)
	shutdowner.AddLast(c.Shutdowner)
	shutdowner.AddLastFuncs(c.shutdownEnd)

	c.Shutdowner = shutdowner

	c.updateTopicRouterInfoFromNamesrv()
	c.registerFilter()
	c.client.SendHeartbeat()
	c.ReblanceQueue()
	c.logger.Infof("start pull consumer:%s success", c.GroupName)
	return nil
}

func (c *PushConsumer) checkConfig() error {

	if c.thresholdCountOfQueue < 1 || c.thresholdCountOfQueue > 65535 {
		return errors.New("ThresholdCountOfQueue out of the range [1, 65535]")
	}

	if c.thresholdSizeOfQueue < 1 || c.thresholdSizeOfQueue > 1024 {
		return errors.New("ThresholdSizeOfQueue out of the range [1, 1024]")
	}

	thresholdCountOfTopic := c.thresholdCountOfTopic
	if thresholdCountOfTopic != -1 && (thresholdCountOfTopic < 1 || thresholdCountOfTopic > 6553500) {
		return errors.New("ThresholdCountOfTopic out of the range [1, 6553500]")
	}

	thresholdSizeOfTopic := c.thresholdSizeOfTopic
	if thresholdSizeOfTopic != -1 && (thresholdSizeOfTopic < 1 || thresholdSizeOfTopic > 102400) {
		return errors.New("ThresholdSizeOfTopic out of the range [1, 102400]")
	}

	if c.pullInterval < 0 || c.pullInterval > 65535 {
		return errors.New("PullInterval out of the range [0, 65535]")
	}

	if c.consumeBatchSize < 1 || c.consumeBatchSize > 1024 {
		return errors.New("ConsumeBatchSize out of the range [1, 1024]")
	}

	if c.pullBatchSize < 1 || c.pullBatchSize > 1024 {
		return errors.New("PullBatchSize out of the range [1, 1024]")
	}

	return nil
}

func (c *PushConsumer) shutdownStart() {
	c.logger.Infof("shutdown PUSH consumer, group:%s, clientID:%s START", c.GroupName, c.ClientID)
}

func (c *PushConsumer) shutdownEnd() {
	c.logger.Infof("shutdown PUSH consumer, group:%s, clientID:%s END", c.GroupName, c.ClientID)
}

func (c *PushConsumer) subscribeRetryTopic() {
	if c.MessageModel == Clustering {
		c.consumer.Subscribe(retryTopic(c.GroupName), exprAll)
	}
}

func (c *PushConsumer) updateTopicRouterInfoFromNamesrv() {
	for _, topic := range c.subscribeData.Topics() {
		c.client.UpdateTopicRouterInfoFromNamesrv(topic)
	}
}

// register the sql filter to the broker
func (c *PushConsumer) registerFilter() {
	for _, d := range c.subscribeData.Datas() {
		if client.IsTag(d.Type) {
			continue
		}

		c.client.RegisterFilter(c.GroupName, d)
	}
}

// SendBack sends the message to the broker, the message will be consumed again after the at
// least time specified by the delayLevel
func (c *PushConsumer) SendBack(m *message.Ext, delayLevel int, broker string) error {
	return c.consumer.SendBack(m, delayLevel, c.GroupName, broker)
}

func (c *PushConsumer) reblance(topic string) {
	allQueues, newQueues, err := c.reblanceQueue(topic)
	if err != nil {
		c.logger.Errorf("reblance queue error:%s", err)
		return
	}
	if len(allQueues) == 0 {
		return
	}

	if c.updateProcessTableAndDispatchPullRequest(topic, newQueues) {
		c.updateSubscribeVersion(topic)
		c.updateThresholdOfQueue(topic)
	}
}

func (c *PushConsumer) updateProcessTableAndDispatchPullRequest(
	topic string, mqs []*message.Queue,
) bool {
	tmpMQs := c.consumeService.messageQueuesOfTopic(topic)
	currentMQs := make([]*message.Queue, len(tmpMQs))
	for i := range currentMQs {
		currentMQs[i] = &tmpMQs[i]
	}

	changed := false
	// remove the mq not processed by the node
	for _, mq := range sub(currentMQs, mqs) {
		if c.consumeService.dropAndRemoveProcessQueue(mq) {
			c.persistAndRemoveOffset(mq)
			changed = true
		}
	}

	// insert new mq
	var pullRequests []pullRequest
	for _, mq := range sub(mqs, currentMQs) {
		offset, err := c.computeWhereToPull(mq)
		if err != nil {
			c.logger.Errorf("compute where to pull the message error:%s", err)
			continue
		}

		if pq, ok := c.consumeService.insertNewMessageQueue(mq); ok {
			c.logger.Infof("reblance: %s, new message queue added:%s", c.Group(), mq)
			pullRequests = append(pullRequests, pullRequest{
				group:        c.Group(),
				nextOffset:   offset,
				messageQueue: mq,
				processQueue: pq,
			})
			changed = true
		}
	}

	c.dispatchPullRequest(pullRequests)
	return changed
}

func (c *PushConsumer) dispatchPullRequest(reqs []pullRequest) {
	for i := range reqs {
		c.pullService.submitRequestImmediately(&reqs[i])
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

func (c *PushConsumer) updateSubscribeVersion(topic string) {
	data := c.subscribeData.Get(topic)
	newVersion := time.Now().UnixNano() / int64(time.Millisecond)
	c.logger.Infof(
		"[%s] reblance changed, update version from %d to %d",
		topic, data.Version, newVersion,
	)

	data.Version = newVersion
	c.client.SendHeartbeat()
}

func (c *PushConsumer) computeWhereToPull(mq *message.Queue) (offset int64, err error) {
	switch c.FromWhere {
	case ConsumeFromLastOffset:
		return c.computeFromLastOffset(mq)
	case ConsumeFromFirstOffset:
		return c.computeFromFirstOffset(mq)
	case ConsumeFromTimestamp:
		return c.computeFromTimestamp(mq)
	default:
		panic("unknow from type:" + c.FromWhere.String())
	}
}

func (c *PushConsumer) computeFromLastOffset(mq *message.Queue) (int64, error) {
	offset, err := c.offsetStorer.readOffset(mq, ReadOffsetFromStore)
	if err == nil {
		return offset, nil
	}

	c.logger.Errorf("read LAST offset of %s, from the store error:%s", mq, err)
	if err != errOffsetNotExist {
		return 0, err
	}

	if strings.HasPrefix(mq.Topic, rocketmq.RetryGroupTopicPrefix) {
		return 0, nil
	}

	return c.QueryMaxOffset(mq)
}

func (c *PushConsumer) computeFromFirstOffset(mq *message.Queue) (int64, error) {
	offset, err := c.offsetStorer.readOffset(mq, ReadOffsetFromStore)
	if err == nil {
		return offset, nil
	}

	c.logger.Errorf("read FIRST offset of %s, from the store error:%s", mq, err)
	if err == errOffsetNotExist {
		return 0, nil
	}

	return 0, err
}

func (c *PushConsumer) computeFromTimestamp(mq *message.Queue) (int64, error) {
	offset, err := c.offsetStorer.readOffset(mq, ReadOffsetFromStore)
	if err == nil {
		return offset, nil
	}

	c.logger.Errorf("read TIMESTAMP offset of %s, from the store error:%s", mq, err)
	if err != errOffsetNotExist {
		return 0, err
	}

	if strings.HasPrefix(mq.Topic, rocketmq.RetryGroupTopicPrefix) {
		return c.QueryMaxOffset(mq)
	}

	return c.searchOffset(mq)
}

func (c *PushConsumer) searchOffset(mq *message.Queue) (int64, error) {
	var addr string
	broker := mq.BrokerName
	if r, err := c.client.FindBrokerAddr(broker, rocketmq.MasterID, true); err == nil {
		addr = r.Addr
	} else {
		c.logger.Errorf("find broker for error:%s", err)
		return 0, err
	}

	return c.client.SearchOffsetByTimestamp(
		addr, mq.Topic, mq.QueueID, c.lastestConsumeTimestamp, time.Second*3,
	)
}

func (c *PushConsumer) updateThresholdOfQueue(topic string) {
	queueCount := len(c.consumeService.messageQueuesOfTopic(topic))
	if queueCount <= 0 {
		return
	}
	if c.thresholdCountOfTopic != -1 {
		maxCountForQueue := c.thresholdCountOfTopic / queueCount
		if maxCountForQueue < 1 {
			maxCountForQueue = 1
		}
		c.thresholdCountOfQueue = maxCountForQueue
	}

	if c.thresholdSizeOfTopic != -1 {
		maxSizeForQueue := c.thresholdSizeOfTopic / queueCount
		if maxSizeForQueue < 1 {
			maxSizeForQueue = 1
		}
		c.thresholdSizeOfQueue = maxSizeForQueue
	}
}

func (c *PushConsumer) pull(r *pullRequest) {
	pq := r.processQueue
	if pq.isDropped() {
		c.logger.Infof("pull request:%s is dropped", r)
		return
	}
	pq.updatePullTime(time.Now())

	if c.consumer.CheckRunning() != nil {
		c.logger.Infof("push consumer is not running, current:%s", c.State)
		c.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		return
	}

	if c.isPause() {
		c.pullService.submitRequestLater(r, PullTimeDelayWhenPause)
		c.logger.Infof("push consumer is pausing")
		return
	}

	if c.doesFlowControl(r) {
		return
	}

	if err := c.consumeService.check(pq); err != nil {
		c.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		c.logger.Infof("consume service checking failed:%s", err)
		return
	}

	q := r.messageQueue
	sub := c.subscribeData.Get(q.Topic)
	if sub == nil {
		c.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		c.logger.Infof("cannot find subsciption %s", q.Topic)
		return
	}

	addr, err := c.findPullBrokerAddr(q)
	if err != nil {
		c.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		c.logger.Infof("find broker addr of %s error %s", q, err)
		return
	}

	if err = c.checkVersion(addr, sub.Type); err != nil {
		c.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		c.logger.Errorf("broker %s version error:%s", q.BrokerName, err)
	}

	commitOffset := c.readCommitOffset(q)
	sysFlag := c.buildSysFlag(addr.IsSlave, commitOffset > 0, sub)

	header := &rpc.PullHeader{
		ConsumerGroup:        c.GroupName,
		Topic:                q.Topic,
		QueueID:              q.QueueID,
		QueueOffset:          r.nextOffset,
		MaxCount:             int32(c.pullBatchSize),
		SysFlag:              sysFlag,
		CommitOffset:         commitOffset,
		SuspendTimeoutMillis: int64(BrokerSuspendMaxTime / time.Millisecond),
		Subscription:         sub.Expr,
		SubVersion:           sub.Version,
		ExpressionType:       sub.Type,
	}

	cb := &pullCallback{
		request: r,
		sub:     sub,
		processPullResponse: func(resp *rpc.PullResponse) *PullResult {
			var tags []string
			if !sub.IsClassFilterMode {
				tags = sub.Tags
			}
			return c.processPullResponse(resp, q, tags)
		},
		pullInterval:   c.pullInterval,
		consumeService: c.consumeService,
		offsetStorer:   c.offsetStorer,
		sched:          c.sched,
		logger:         c.logger,
		pullService:    c.pullService,
	}

	err = c.client.PullMessageAsync(addr.Addr, header, ConsumerTimeoutWhenSuspend, cb.run)
	if err != nil {
		c.pullService.submitRequestLater(r, PullTimeDelayWhenException)
		c.logger.Errorf("pull async error %s", err)
	}
}

func (c *PushConsumer) doesFlowControl(r *pullRequest) bool {
	if c.doCountFlowControl(r) {
		return true
	}

	if c.doSizeFlowControl(r) {
		return true
	}

	if c.doConsumeServiceFlowControl(r) {
		return true
	}
	return false
}

func (c *PushConsumer) doCountFlowControl(r *pullRequest) bool {
	pq := r.processQueue
	cachedCount := pq.messageCount()
	if cachedCount <= int32(c.thresholdCountOfQueue) {
		return false
	}

	c.pullService.submitRequestLater(r, PullTimeDelayWhenFlowControl)
	c.queueControlFlowTotal++

	if c.queueControlFlowTotal%1000 == 0 {
		min, max := pq.offsetRange()
		c.logger.Warnf(
			"COUNT FLOW CONTROL:the cached message count exceeds the threshold %d,"+
				"minOffset:%d,maxOffset:%d,count:%d, pull request:%s, flow controll total:%d",
			c.thresholdCountOfQueue, min, max, cachedCount, r, c.queueControlFlowTotal,
		)
	}
	return true
}

func (c *PushConsumer) doSizeFlowControl(r *pullRequest) bool {
	pq := r.processQueue
	cachedSize := pq.messageSize()
	if cachedSize <= int64(c.thresholdSizeOfQueue) {
		return false
	}

	c.pullService.submitRequestLater(r, PullTimeDelayWhenFlowControl)
	c.queueControlFlowTotal++

	if c.queueControlFlowTotal%1000 == 0 {
		c.logger.Warnf(
			"SIZE FLOW CONTROL:the cached message size exceeds the threshold %d,size:%dM, pull request:%s, flow controll total:%d",
			c.thresholdSizeOfQueue, cachedSize>>20, r, c.queueControlFlowTotal,
		)
	}
	return true
}

func (c *PushConsumer) doConsumeServiceFlowControl(r *pullRequest) bool {
	pq := r.processQueue
	if !c.consumeService.flowControl(pq) {
		return false
	}
	c.pullService.submitRequestLater(r, PullTimeDelayWhenFlowControl)
	c.queueControlFlowTotal++

	if c.queueControlFlowTotal%1000 == 0 {
		min, max := pq.offsetRange()
		c.logger.Warnf(
			"CONSUME SERVICE FLOW FLOW CONTROL:minOffset:%d,maxOffset:%d,pull request:%s,flow controll total:%d",
			min, max, r, c.queueControlFlowTotal,
		)
	}

	return true
}

func (c *PushConsumer) readCommitOffset(q *message.Queue) int64 {
	if c.MessageModel != Clustering {
		return 0
	}

	offset, _ := c.offsetStorer.readOffset(q, ReadOffsetFromMemory) // READ FROM MEMORY, NO ERROR
	return offset
}

func (c *PushConsumer) buildSysFlag(isSlave, commitOffset bool, sub *client.SubscribeData) int32 {
	subExpr, isClassFilter := c.getFilterInfo(sub)
	sysFlag := buildPullFlag(commitOffset, true, subExpr != "", isClassFilter)
	if isSlave {
		ClearCommitOffset(sysFlag)
	}
	return sysFlag
}

func (c *PushConsumer) getFilterInfo(sub *client.SubscribeData) (subExpr string, isClass bool) {
	isClass = sub.IsClassFilterMode

	if c.postSubscriptionWhenPull && !isClass {
		subExpr = sub.Expr
	}
	return
}

func (c *PushConsumer) checkVersion(broker *client.FindBrokerResult, typ string) error {
	if client.IsTag(typ) {
		return nil
	}

	if broker.Version < int32(rocketmq.V4_1_0_SNAPSHOT) {
		return fmt.Errorf(
			"the broker with version:%d does not support for filter message by %s",
			broker.Version, typ,
		)
	}
	return nil
}

// Suspend pause the consumer, this operation is thread-safe
func (c *PushConsumer) Suspend() {
	atomic.StoreUint32(&c.pause, 1)
}

// Resume un-pause the consumer, this operation is thread-safe
func (c *PushConsumer) Resume() {
	atomic.StoreUint32(&c.pause, 0)
}

func (c *PushConsumer) isPause() bool {
	return atomic.LoadUint32(&c.pause) == 1
}
