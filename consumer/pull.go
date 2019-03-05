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

// PullConsumer consumes the messages using pulling method
type PullConsumer struct {
	*consumer
	BrokerSuspendMaxTime       time.Duration
	ConsumerTimeoutWhenSuspend time.Duration
	ConsumerPullTimeout        time.Duration
	MaxReconsumeTimes          int32

	currentMessageQs []*message.Queue
}

// NewPullConsumer creates consumer using the pull model
func NewPullConsumer(group string, namesrvAddrs []string, logger log.Logger) *PullConsumer {
	c := &PullConsumer{
		consumer: &consumer{
			Logger: logger,
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
	c.MaxReconsumeTimes = 16
	c.assigner = &Averagely{}
	c.reblancer = c
	c.runnerInfo = c.RunningInfo
	return c
}

// Start pull consumer
func (c *PullConsumer) start() error {
	c.Logger.Info("start pull consumer")
	if c.GroupName == "" {
		return errors.New("start pull consumer error:empty group")
	}

	err := c.consumer.start()
	if err != nil {
		return err
	}

	c.Logger.Infof("start pull consumer:%s success", c.GroupName)
	return nil
}

func (c *PullConsumer) reblance(topic string) {
	allQueues, newQueues, err := c.reblanceQueue(topic)
	if err != nil {
		c.Logger.Errorf("reblance queue error:%s", err)
		return
	}
	if len(allQueues) == 0 {
		return
	}

	c.offseter.updateQueues(newQueues...)

	if c.MessageQueueChanged != nil && messageQueueChanged(c.currentMessageQs, newQueues) {
		c.MessageQueueChanged.Changed(topic, allQueues, newQueues)
	}
	c.currentMessageQs = newQueues
}

func messageQueueChanged(qs1, qs2 []*message.Queue) bool {
	if len(qs1) != len(qs2) {
		return true
	}

	for _, q1 := range qs1 {
		for _, q2 := range qs2 {
			if *q1 != *q2 {
				return true
			}
		}
	}

	for _, q2 := range qs2 {
		for _, q1 := range qs1 {
			if *q2 != *q1 {
				return true
			}
		}
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
) (*PullResult, error) {
	addr, err := c.client.FindBrokerAddr(q.BrokerName, c.selectBrokerID(q), false)
	if err != nil {
		c.client.UpdateTopicRouterInfoFromNamesrv(q.Topic)
		addr, err = c.client.FindBrokerAddr(q.BrokerName, c.selectBrokerID(q), false)
		if err != nil {
			return nil, err
		}
	}

	resp, err := c.client.PullMessageSync(
		addr.Addr,
		&rpc.PullHeader{
			ConsumerGroup:        c.GroupName,
			Topic:                q.Topic,
			QueueID:              q.QueueID,
			QueueOffset:          offset,
			MaxCount:             int32(maxCount),
			SysFlag:              buildPull(false, block, true),
			CommitOffset:         0,
			SuspendTimeoutMillis: int64(c.BrokerSuspendMaxTime / time.Millisecond),
			Subscription:         expr,
			SubVersion:           0,
			ExpressionType:       ExprTypeTag,
		},
		c.ConsumerPullTimeout)

	if err != nil {
		return nil, err
	}

	c.brokerSuggester.put(q, int32(resp.SuggestBrokerID))
	pr := &PullResult{
		NextBeginOffset: resp.NextBeginOffset,
		MinOffset:       resp.MinOffset,
		MaxOffset:       resp.MaxOffset,
		Messages:        resp.Messages,
	}
	switch resp.Code {
	case rpc.Success:
		pr.Status = Found
	case rpc.PullNotFound:
		pr.Status = NoNewMessage
		return pr, nil
	case rpc.PullRetryImmediately:
		pr.Status = NoMatchedMessage
		return pr, nil
	case rpc.PullOffsetMoved:
		pr.Status = OffsetIllegal
		return pr, nil
	default:
		panic("BUG:unprocess code:" + strconv.Itoa(int(resp.Code)))
	}

	tags := ParseTags(expr)
	if len(tags) == 0 {
		return pr, nil
	}

	filterMsgs := make([]*message.Ext, 0, len(pr.Messages))
	for _, m := range pr.Messages {
		tag := m.GetTags()
		if tag == "" {
			continue
		}

		for _, t := range tags {
			if tag == t {
				filterMsgs = append(filterMsgs, m)
				break
			}
		}
	}

	pr.Messages = filterMsgs
	return pr, nil
}

// RunningInfo returns the consumter's running information
func (c *PullConsumer) RunningInfo() client.RunningInfo {
	millis := time.Millisecond
	prop := map[string]string{
		"consumerGroup":                    c.GroupName,
		"brokerSuspendMaxTimeMillis":       strconv.FormatInt(int64(c.BrokerSuspendMaxTime/millis), 10),
		"consumerTimeoutMillisWhenSuspend": strconv.FormatInt(int64(c.ConsumerTimeoutWhenSuspend/millis), 10),
		"consumerPullTimeoutMillis":        strconv.FormatInt(int64(c.ConsumerPullTimeout/millis), 10),
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

// SendBack send back message
func (c *PullConsumer) SendBack(
	m *message.Ext, delayLevel int32, group, brokerName string,
) error {
	if group == "" {
		group = c.GroupName
	}

	addr := ""
	if brokerName != "" {
		if r, err := c.client.FindBrokerAddr(brokerName, rocketmq.MasterID, false); err == nil {
			if !r.IsSlave {
				addr = r.Addr
			}
		}
	}

	if addr == "" {
		if a, _, err := message.ParseMessageID(m.MsgID); err == nil {
			addr = a.String()
		} else {
			return err
		}
	}

	return c.client.SendBack(addr, &rpc.SendBackHeader{
		CommitOffset:      m.CommitLogOffset,
		Group:             group,
		DelayLevel:        delayLevel,
		MessageID:         m.MsgID,
		Topic:             m.Topic,
		MaxReconsumeTimes: c.MaxReconsumeTimes,
	}, time.Second*3)
}
