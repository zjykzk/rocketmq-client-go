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
	"github.com/zjykzk/rocketmq-client-go/remote"
)

// PullConsumer consumes the messages using pulling method
type PullConsumer struct {
	*consumer
	BrokerSuspendMaxTime       time.Duration
	ConsumerTimeoutWhenSuspend time.Duration
	ConsumerPullTimeout        time.Duration

	registerTopics   []string
	currentMessageQs []*message.Queue
}

// NewPullConsumer creates consumer using the pull model
func NewPullConsumer(group string, namesrvAddrs []string, logger log.Logger) *PullConsumer {
	c := &PullConsumer{
		consumer: &consumer{
			logger: logger,
			Config: defaultConfig,
			Server: rocketmq.Server{State: rocketmq.StateCreating},
		},
	}
	c.StartFunc, c.ShutdownFunc = c.start, c.shutdown
	c.GroupName = group
	c.NameServerAddrs = namesrvAddrs
	c.FromWhere = consumeFromLastOffset
	c.MessageModel = Clustering
	c.Typ = Pull
	c.BrokerSuspendMaxTime = 20 * time.Second
	c.ConsumerTimeoutWhenSuspend = 30 * time.Second
	c.ConsumerPullTimeout = 10 * time.Second
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
	c.subscribe()

	c.logger.Infof("start pull consumer:%s success", c.GroupName)
	return nil
}

func (c *PullConsumer) checkConfig() error {
	if c.ConsumerTimeoutWhenSuspend < c.BrokerSuspendMaxTime {
		return errors.New("ConsumerTimeoutWhenSuspend when suspend is less than BrokerSuspendMaxTime")
	}
	return nil
}

func (c *PullConsumer) subscribe() {
	for _, t := range c.registerTopics {
		c.consumer.Subscribe(t, subAll)
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

	c.offseter.updateQueues(newQueues...)

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
	return c.pullSync(q, expr, offset, maxCount, true)
}

// PullSync pull the messages sync
func (c *PullConsumer) PullSync(q *message.Queue, expr string, offset int64, maxCount int) (
	*PullResult, error,
) {
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
		addr,
		&rpc.PullHeader{
			ConsumerGroup:        c.GroupName,
			Topic:                q.Topic,
			QueueID:              q.QueueID,
			QueueOffset:          offset,
			MaxCount:             int32(maxCount),
			SysFlag:              buildPullFlag(false, block, true),
			CommitOffset:         0,
			SuspendTimeoutMillis: int64(c.BrokerSuspendMaxTime / time.Millisecond),
			Subscription:         expr,
			SubVersion:           0,
			ExpressionType:       ExprTypeTag,
		},
		c.ConsumerPullTimeout)
	if err != nil {
		c.logger.Errorf("pull message sync error:%s", err)
		return nil, err
	}

	c.brokerSuggester.put(q, int32(resp.SuggestBrokerID))
	pr := &PullResult{
		NextBeginOffset: resp.NextBeginOffset,
		MinOffset:       resp.MinOffset,
		MaxOffset:       resp.MaxOffset,
		Messages:        resp.Messages,
		Status:          calcStatusFromCode(resp.Code),
	}

	tags := ParseTags(expr)
	if len(tags) > 0 {
		pr.Messages = filterMessage(pr.Messages, tags)
	}

	return pr, nil
}

func (c *PullConsumer) findPullBrokerAddr(q *message.Queue) (string, error) {
	addr, err := c.client.FindBrokerAddr(q.BrokerName, c.selectBrokerID(q), false)
	if err != nil {
		c.client.UpdateTopicRouterInfoFromNamesrv(q.Topic)
		addr, err = c.client.FindBrokerAddr(q.BrokerName, c.selectBrokerID(q), false)
		if err != nil {
			return "", err
		}
	}
	return addr.Addr, err
}

func calcStatusFromCode(code remote.Code) PullStatus {
	switch code {
	case rpc.Success:
		return Found
	case rpc.PullNotFound:
		return NoNewMessage
	case rpc.PullRetryImmediately:
		return NoMatchedMessage
	case rpc.PullOffsetMoved:
		return OffsetIllegal
	default:
		panic("BUG:unprocess code:" + strconv.Itoa(int(code)))
	}
}

func filterMessage(msgs []*message.Ext, tags []string) []*message.Ext {
	needMsgs := make([]*message.Ext, 0, len(msgs))
	for _, m := range msgs {
		tag := m.GetTags()
		if tag == "" {
			continue
		}

		for _, t := range tags {
			if tag == t {
				needMsgs = append(needMsgs, m)
				break
			}
		}
	}
	return needMsgs
}

// RunningInfo returns the consumter's running information
func (c *PullConsumer) RunningInfo() client.RunningInfo {
	millis := time.Millisecond
	prop := map[string]string{
		"consumerGroup":                    c.GroupName,
		"brokerSuspendMaxTimeMillis":       dToMsStr(c.BrokerSuspendMaxTime),
		"consumerTimeoutMillisWhenSuspend": dToMsStr(c.ConsumerTimeoutWhenSuspend),
		"consumerPullTimeoutMillis":        dToMsStr(c.ConsumerPullTimeout),
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
func (c *PullConsumer) Register(topics []string, listener MessageQueueChanger) {
	c.registerTopics = topics
	c.messageQueueChanger = listener
}
