package consumer

import (
	"errors"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

var (
	defaultLastestConsumeTimestamp = time.Now().Add(time.Minute * 30)
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

type consumerService interface {
	messageQueues() []message.Queue
	removeOldMessageQueue(mq *message.Queue) bool
	insertNewMessageQueue(mq *message.Queue) (*processQueue, bool)
}

// PushConsumer the consumer with push model
type PushConsumer struct {
	*consumer
	MaxReconsumeTimes       int
	LastestConsumeTimestamp time.Time
	ConsumeTimeout          time.Duration

	ThresholdCountOfQueue int
	ThresholdSizeOfQueue  int
	ThresholdCountOfTopic int
	ThresholdSizeOfTopic  int

	PullInterval     time.Duration
	PullBatchSize    int
	ConsumeBatchSize int

	PostSubscriptionWhenPull   bool
	ConsumeMessageBatchMaxSize int

	consumerService        consumerService
	consumerServiceBuilder func() (consumerService, error)

	subscription map[string]string

	pullService *pullService
}

func newPushConsumer(group string, namesrvAddrs []string, logger log.Logger) *PushConsumer {
	pc := &PushConsumer{
		consumer: &consumer{
			logger: logger,
			Config: defaultConfig,
			Server: rocketmq.Server{State: rocketmq.StateCreating},
		},
		MaxReconsumeTimes:       defaultPushMaxReconsumeTimes,
		LastestConsumeTimestamp: defaultLastestConsumeTimestamp,
		ConsumeTimeout:          defaultConsumeTimeout,

		ThresholdCountOfQueue: defaultThresholdCountOfQueue,
		ThresholdSizeOfQueue:  defaultThresholdSizeOfQueue,
		ThresholdCountOfTopic: defaultThresholdCountOfTopic,
		ThresholdSizeOfTopic:  defaultThresholdSizeOfTopic,

		PullInterval:     defaultPullInterval,
		ConsumeBatchSize: defaultConsumeBatchSize,
		PullBatchSize:    defaultPullBatchSize,

		PostSubscriptionWhenPull:   defaultPostSubscriptionWhenPull,
		ConsumeMessageBatchMaxSize: defaultConsumeMessageBatchMaxSize,

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

	pc.StartFunc, pc.ShutdownFunc = pc.start, pc.shutdown
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

	pc.consumerServiceBuilder = func() (consumerService, error) {
		return newConsumeConcurrentlyService(concurrentlyServiceConfig{
			consumeServiceConfig: consumeServiceConfig{
				group:           group,
				logger:          logger,
				messageSendBack: pc,
				offseter:        pc.offseter,
			},
			consumeTimeout: pc.ConsumeTimeout,
			consumer:       userConsumer,
			batchSize:      pc.ConsumeBatchSize,
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

	pc.consumerService, err = pc.consumerServiceBuilder()
	if err != nil {
		pc.logger.Errorf("build consumer service error:%s", err)
		return err
	}

	pc.pullService, err = newPullService(pullServiceConfig{
		messagePuller: pc,
		logger:        pc.logger,
	})
	if err != nil {
		return err
	}

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

	if pc.ThresholdCountOfQueue < 1 || pc.ThresholdCountOfQueue > 65535 {
		return errors.New("ThresholdCountOfQueue out of the range [1, 65535]")
	}

	if pc.ThresholdSizeOfQueue < 1 || pc.ThresholdSizeOfQueue > 1024 {
		return errors.New("ThresholdSizeOfQueue out of the range [1, 1024]")
	}

	thresholdCountOfTopic := pc.ThresholdCountOfTopic
	if thresholdCountOfTopic != -1 && (thresholdCountOfTopic < 1 || thresholdCountOfTopic > 6553500) {
		return errors.New("ThresholdCountOfTopic out of the range [1, 6553500]")
	}

	thresholdSizeOfTopic := pc.ThresholdSizeOfTopic
	if thresholdSizeOfTopic != -1 && (thresholdSizeOfTopic < 1 || thresholdSizeOfTopic > 102400) {
		return errors.New("ThresholdSizeOfTopic out of the range [1, 102400]")
	}

	if pc.PullInterval < 0 || pc.PullInterval > 65535 {
		return errors.New("PullInterval out of the range [0, 65535]")
	}

	if pc.ConsumeBatchSize < 1 || pc.ConsumeBatchSize > 1024 {
		return errors.New("ConsumeBatchSize out of the range [1, 1024]")
	}

	if pc.PullBatchSize < 1 || pc.PullBatchSize > 1024 {
		return errors.New("PullBatchSize out of the range [1, 1024]")
	}

	return nil
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

func (pc *PushConsumer) shutdown() {
	pc.logger.Info("shutdown push consumer ")
	pc.consumer.shutdown()
	pc.pullService.shutdown()
	pc.logger.Info("shutdown push consumer OK")
}

// SendBack sends the message to the broker, the message will be consumed again after the at
// least time specified by the delayLevel
func (pc *PushConsumer) SendBack(m *message.Ext, delayLevel int, broker string) error {
	return nil //TODO
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
	tmpMQs := pc.consumerService.messageQueues()
	currentMQs := make([]*message.Queue, len(tmpMQs))
	for i := range currentMQs {
		currentMQs[i] = &tmpMQs[i]
	}

	changed := false
	// remove the mq not processed by the node
	for _, mq := range sub(currentMQs, mqs) {
		if pc.consumerService.removeOldMessageQueue(mq) {
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

		if pq, ok := pc.consumerService.insertNewMessageQueue(mq); ok {
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
		addr, mq.Topic, mq.QueueID, pc.LastestConsumeTimestamp, time.Second*3,
	)
}

func (pc *PushConsumer) updateThresholdOfQueue() {
	queueCount := len(pc.consumerService.messageQueues())
	if queueCount <= 0 {
		return
	}
	if pc.ThresholdCountOfTopic != -1 {
		maxCountForQueue := pc.ThresholdCountOfTopic / queueCount
		if maxCountForQueue < 1 {
			maxCountForQueue = 1
		}
		pc.ThresholdCountOfQueue = maxCountForQueue
	}

	if pc.ThresholdSizeOfTopic != -1 {
		maxSizeForQueue := pc.ThresholdSizeOfTopic / queueCount
		if maxSizeForQueue < 1 {
			maxSizeForQueue = 1
		}
		pc.ThresholdSizeOfQueue = maxSizeForQueue
	}
}

func (pc *PushConsumer) pull(r *pullRequest) {
	// TODO
}
