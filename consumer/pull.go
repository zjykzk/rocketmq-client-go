package consumer

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

const (
	defaultBrokerSuspendMaxTime = 2 * time.Second
	defaultTimeoutWhenSuspend   = 30 * time.Second
	defaultPullTimeout          = 10 * time.Second
)

// PullConsumer consumes the messages using pulling method
type PullConsumer struct {
	*consumer
	brokerSuspendMaxTime time.Duration
	timeoutWhenSuspend   time.Duration
	pullTimeout          time.Duration

	registerTopics   []string
	currentMessageQs []*message.Queue
}

// NewPullConsumer creates consumer using the pull model
func NewPullConsumer(group string, namesrvAddrs []string, logger log.Logger) *PullConsumer {
	c := &PullConsumer{
		consumer: &consumer{
			logger:          logger,
			Config:          defaultConfig,
			Server:          rocketmq.Server{State: rocketmq.StateCreating},
			brokerSuggester: &brokerSuggester{table: make(map[string]int32, 32)},
			subscribeData:   client.NewSubcribeTable(),
		},
	}
	c.StartFunc = c.start
	c.GroupName = group
	c.NameServerAddrs = namesrvAddrs
	c.FromWhere = ConsumeFromLastOffset
	c.MessageModel = Clustering
	c.Typ = Pull
	c.brokerSuspendMaxTime = defaultBrokerSuspendMaxTime
	c.timeoutWhenSuspend = defaultTimeoutWhenSuspend
	c.pullTimeout = defaultPullTimeout
	c.assigner = &Averagely{}
	c.reblancer = c
	c.runnerInfo = c.RunningInfo
	return c
}

// Start pull consumer
func (c *PullConsumer) start() error {
	c.logger.Info("start pull consumer")
	err := c.checkConfig()
	if err != nil {
		c.logger.Errorf("check config error:%s", err)
		return err
	}

	err = c.consumer.start()
	if err != nil {
		c.logger.Errorf("start consumer error:%s", err)
		return err
	}

	err = c.client.RegisterConsumer(c)
	if err != nil {
		c.logger.Errorf("register PULL consumer error:%s", err)
		return err
	}

	c.subscribe()

	c.logger.Infof("start pull consumer:%s success", c.GroupName)
	return nil
}

func (c *PullConsumer) checkConfig() error {
	if c.timeoutWhenSuspend < c.brokerSuspendMaxTime {
		return errors.New("ConsumerTimeoutWhenSuspend when suspend is less than BrokerSuspendMaxTime")
	}
	return nil
}

// Subscribe subscribe the topic dynamic
func (c *PullConsumer) Subscribe(topic string) {
	c.consumer.Subscribe(topic, exprAll)
}

func (c *PullConsumer) subscribe() {
	for _, t := range c.registerTopics {
		c.Subscribe(t)
	}
}

func (c *PullConsumer) reblance(topic string) {
	allQueues, newQueues, err := c.reblanceQueue(topic)
	if err != nil {
		c.logger.Errorf("reblance queue error:%s", err)
		return
	}
	if len(allQueues) == 0 {
		return
	}

	c.offsetStorer.updateQueues(newQueues...)

	if c.messageQueueChanger != nil && messageQueueChanged(c.currentMessageQs, newQueues) {
		c.messageQueueChanger.Change(topic, allQueues, newQueues)
	}
	c.currentMessageQs = newQueues
}

func messageQueueChanged(qs1, qs2 []*message.Queue) bool {
	if len(qs1) != len(qs2) {
		return true
	}

NEXT_Q1:
	for _, q1 := range qs1 {
		for _, q2 := range qs2 {
			if *q1 == *q2 {
				continue NEXT_Q1
			}
		}
		return true
	}

NEXT_Q2:
	for _, q2 := range qs2 {
		for _, q1 := range qs1 {
			if *q2 == *q1 {
				continue NEXT_Q2
			}
		}
		return true
	}

	return false
}

// PullSyncBlockIfNotFound pull the messages sync and block when no message
func (c *PullConsumer) PullSyncBlockIfNotFound(
	q *message.Queue, expr string, offset int64, maxCount int,
) (
	*PullResult, error,
) {
	if err := c.CheckRunning(); err != nil {
		return nil, err
	}
	return c.pullSync(q, expr, offset, maxCount, true)
}

// PullSync pull the messages sync
func (c *PullConsumer) PullSync(q *message.Queue, expr string, offset int64, maxCount int) (
	*PullResult, error,
) {
	if err := c.CheckRunning(); err != nil {
		return nil, err
	}
	return c.pullSync(q, expr, offset, maxCount, false)
}

func (c *PullConsumer) pullSync(
	q *message.Queue, expr string, offset int64, maxCount int, block bool,
) (
	*PullResult, error,
) {
	addr, err := c.findPullBrokerAddr(q)
	if err != nil {
		c.logger.Errorf("find pull address of %s error:%s", q, err)
		return nil, err
	}

	resp, err := c.client.PullMessageSync(
		addr.Addr,
		&rpc.PullHeader{
			ConsumerGroup:        c.GroupName,
			Topic:                q.Topic,
			QueueID:              q.QueueID,
			QueueOffset:          offset,
			MaxCount:             int32(maxCount),
			SysFlag:              buildPullFlag(false, block, true, false),
			CommitOffset:         0,
			SuspendTimeoutMillis: int64(c.brokerSuspendMaxTime / time.Millisecond),
			Subscription:         expr,
			SubVersion:           0,
			ExpressionType:       ExprTypeTag.String(),
		},
		c.pullTimeout,
	)
	if err != nil {
		c.logger.Errorf("pull message sync error:%s", err)
		return nil, err
	}

	return c.consumer.processPullResponse(resp, q, client.ParseTags(expr)), nil
}

// RunningInfo returns the consumter's running information
func (c *PullConsumer) RunningInfo() client.RunningInfo {
	millis := time.Millisecond
	prop := map[string]string{
		"consumerGroup":                    c.GroupName,
		"brokerSuspendMaxTimeMillis":       dToMsStr(c.brokerSuspendMaxTime),
		"consumerTimeoutMillisWhenSuspend": dToMsStr(c.timeoutWhenSuspend),
		"consumerPullTimeoutMillis":        dToMsStr(c.pullTimeout),
		"messageModel":                     c.MessageModel.String(),
		"registerTopics":                   strings.Join(c.subscribeData.Topics(), ", "),
		"unitMode":                         strconv.FormatBool(c.IsUnitMode),
		"maxReconsumeTimes":                strconv.FormatInt(int64(c.MaxReconsumeTimes), 10),
		"PROP_CONSUMER_START_TIMESTAMP":    strconv.FormatInt(c.startTime.UnixNano()/int64(millis), 10),
		"PROP_NAMESERVER_ADDR":             strings.Join(c.NameServerAddrs, ";"),
		"PROP_CONSUME_TYPE":                c.Type(),
		"PROP_CLIENT_VERSION":              rocketmq.CurrentVersion.String(),
	}
	return client.RunningInfo{
		Properties:    prop,
		Subscriptions: c.Subscriptions(),
	}
}

func dToMsStr(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Millisecond), 10)
}

// Register register the message queue changed event of the topics
// it must be called before calling Start function, otherwise the topic is not subscribe
func (c *PullConsumer) Register(topics []string, listener MessageQueueChanger) {
	c.registerTopics = topics
	c.messageQueueChanger = listener
}

// ResetOffset NOOPS
func (c *PullConsumer) ResetOffset(topic string, offsets map[message.Queue]int64) error {
	return nil // empty
}

// ConsumeMessageDirectly NOOPS
func (c *PullConsumer) ConsumeMessageDirectly(
	msg *message.Ext, broker string,
) (
	r client.ConsumeMessageDirectlyResult, err error,
) {
	return
}
