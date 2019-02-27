package producer

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote/rpc"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// Config the configuration of producer
type Config struct {
	rocketmq.Client
	SendMsgTimeout                   time.Duration
	CompressSizeThreshod             int32
	RetryTimesWhenSendFailed         int32
	RetryTimesWhenSendAsyncFailed    int32
	RetryAnotherBrokerWhenNotStoreOK bool
	MaxMessageSize                   int32
	CreateTopicKey                   string
	DefaultTopicQueueNums            int32
}

var defaultConfig = Config{
	Client: rocketmq.Client{
		HeartbeatBrokerInterval:       30 * time.Second,
		PollNameServerInterval:        30 * time.Second,
		PersistConsumerOffsetInterval: 5 * time.Second,
		InstanceName:                  "DEFAULT",
	},
	SendMsgTimeout:                   3 * time.Second,
	CompressSizeThreshod:             1 << 12, // 4K
	RetryTimesWhenSendFailed:         2,
	RetryTimesWhenSendAsyncFailed:    2,
	RetryAnotherBrokerWhenNotStoreOK: false,
	MaxMessageSize:                   1 << 22, // 4M
	CreateTopicKey:                   rocketmq.DefaultTopic,
	DefaultTopicQueueNums:            4,
}

// Producer sends messages
type Producer struct {
	Config
	rocketmq.Server
	topicPublishInfos topicPublishInfoTable
	client            client.MQClient
	mqFaultStrategy   *MQFaultStrategy

	Logger log.Logger
}

// NewProducer creates one procuder instance
func NewProducer(group string, namesrvAddrs []string, logger log.Logger) *Producer {
	p := &Producer{
		Config: defaultConfig,
		Logger: logger,
		Server: rocketmq.Server{State: rocketmq.StateCreating},
	}
	p.StartFunc, p.ShutdownFunc = p.start, p.shutdown
	p.GroupName = group
	p.NameServerAddrs = namesrvAddrs
	return p
}

// Start producer's worker
func (p *Producer) start() (err error) {
	p.Logger.Info("start producer")
	if p.GroupName != rocketmq.ClientInnerProducerGroup && p.InstanceName == "DEFAULT" {
		p.InstanceName = strconv.Itoa(os.Getpid())
	}

	p.ClientIP, err = rocketmq.GetIPStr()
	if err != nil {
		p.Logger.Errorf("no ip")
		return
	}

	p.topicPublishInfos.table = make(map[string]*topicPublishInfo)

	p.ClientID = client.BuildMQClientID(p.ClientIP, p.UnitName, p.InstanceName)
	p.client, err = client.NewMQClient(
		&client.Config{
			HeartbeatBrokerInterval: p.HeartbeatBrokerInterval,
			PollNameServerInterval:  p.PollNameServerInterval,
			NameServerAddrs:         p.NameServerAddrs,
		}, p.ClientID, p.Logger)
	if err != nil {
		return
	}

	err = p.client.RegisterProducer(p)
	if err != nil {
		p.Logger.Errorf("register producer error:%s", err.Error())
		return
	}

	err = p.client.Start()
	p.mqFaultStrategy = NewMQFaultStrategy(true)
	return
}

// Shutdown shutdown the producer
func (p *Producer) shutdown() {
	p.Logger.Info("shutdown producer:" + p.GroupName)
	p.client.UnregisterProducer(p.GroupName)
	p.client.Shutdown()
	p.Logger.Infof("shutdown producer:%s END", p.GroupName)
}

// Group returns the GroupName of the producer
func (p *Producer) Group() string {
	return p.GroupName
}

// PublishTopics returns the topics published by the producer
func (p *Producer) PublishTopics() []string {
	return p.topicPublishInfos.topics()
}

// Unpublish unpublish the topic
func (p *Producer) Unpublish(topic string) bool {
	return p.topicPublishInfos.delete(topic)
}

// UpdateTopicPublish updates the published information
// it always update the publish data, even no message sent under the topic by now
// the router must not be nil
func (p *Producer) UpdateTopicPublish(topic string, router *route.TopicRouter) {
	p.Logger.Debugf("update topic publish %s %s", topic, router.String())
	route.SortTopicQueue(router.Queues) // for the select consume queue is not duplicated by brokername
	qs := make([]*message.Queue, 0, 8)
	for _, q := range router.Queues {
		if !route.IsWritable(q.Perm) {
			continue
		}

		var b *route.Broker
		for _, b0 := range router.Brokers {
			if q.BrokerName == b0.Name {
				b = b0
				break
			}
		}

		if b == nil {
			continue
		}

		if _, ok := b.Addresses[rocketmq.MasterID]; !ok {
			continue
		}

		for i := 0; i < q.WriteCount; i++ {
			qs = append(qs, &message.Queue{Topic: topic, BrokerName: b.Name, QueueID: uint8(i)})
		}
	}

	tp := &topicPublishInfo{
		orderTopic:          false, // NOTE: unsupport the order now
		router:              router,
		queues:              qs,
		haveTopicRouterInfo: true,
	}

	prev := p.topicPublishInfos.put(topic, tp)
	if prev != nil {
		p.Logger.Info("UpdateTopicPublish prev is not null, " + prev.String())
	}
}

// NeedUpdateTopicPublish returns true if the published topic's consume queue is empty
// otherwise false
func (p *Producer) NeedUpdateTopicPublish(topic string) bool {
	pi := p.topicPublishInfos.get(topic)
	return pi != nil && !pi.hasQueue()
}

// SendSync sends the message
// the message must not be nil
func (p *Producer) SendSync(m *message.Message) (sendResult *SendResult, err error) {
	if m == nil {
		return nil, errEmptyMessage
	}

	if len(m.Body) == 0 {
		return nil, errEmptyBody
	}

	if m.Topic == "" {
		return nil, errEmptyTopic
	}

	pi, err := p.getRouters(m.Topic)
	if err != nil {
		return nil, err
	}

	m.SetUniqID(message.CreateUniqID())

	sysFlag := int32(0)
	if p.tryToCompress(m) {
		sysFlag |= message.Compress
	}

	return p.sendMessageWithFault(pi, m, sysFlag)
}

func (p *Producer) getRouters(topic string) (*topicPublishInfo, error) {
	pi := p.topicPublishInfos.get(topic)
	if pi == nil { // publish the topic
		pi = &topicPublishInfo{}
		p.topicPublishInfos.putIfAbsent(topic, pi)
	}

	if !pi.hasQueue() {
		err := p.client.UpdateTopicRouterInfoFromNamesrv(topic)
		if err != nil {
			p.Logger.Errorf("update topic router from namesrv error:%s", err)
			return nil, err
		}
		pi = p.topicPublishInfos.get(topic)
	}

	if !pi.hasQueue() {
		return nil, errNoRouters
	}

	return pi, nil
}

func (p *Producer) sendMessageWithFault(
	router *topicPublishInfo, m *message.Message, sysFlag int32,
) (
	sendResult *SendResult, err error,
) {
	var (
		q           *message.Queue
		brokersSent = make([]string, p.RetryTimesWhenSendFailed+1)
		retryCount  = int32(1)
		prevBody    = m.Body
	)

	startPoint := time.Now()
	prev := startPoint
	for maxSendCount := p.RetryTimesWhenSendFailed + 1; retryCount < maxSendCount; retryCount++ {
		q = p.mqFaultStrategy.SelectOneQueue(router, brokersSent[retryCount-1])
		sendResult, err = p.sendSync(m, q, sysFlag)

		now := time.Now()
		cost := now.Sub(prev) / 10e6

		prev = now
		brokersSent[retryCount] = q.BrokerName

		if err != nil {
			p.mqFaultStrategy.UpdateFault(q.BrokerName, int64(cost), true)
			p.Logger.Errorf(
				"resend at once %s RT:%dms, Queue:%s, err %s", m.GetUniqID(), cost/time.Millisecond, q, err,
			)
			p.Logger.Warn(m.String())
			continue
		}

		p.mqFaultStrategy.UpdateFault(q.BrokerName, int64(cost), false)
		goto END
	}

	p.Logger.Errorf("send %d times, still failed, cost %s, topic:%s, sendBrokers:%v",
		retryCount-1, time.Now().Sub(startPoint), m.Topic, brokersSent[1:])
END:
	m.Body = prevBody

	return
}

func (p *Producer) sendSync(m *message.Message, q *message.Queue, sysFlag int32) (
	*SendResult, error,
) {
	addr := p.client.GetMasterBrokerAddr(q.BrokerName)
	if addr == "" {
		p.Logger.Errorf("cannot find broker:" + q.BrokerName)
		return nil, errors.New("cannot find broker")
	}

	println("=============", q.BrokerName, addr)

	resp, err := rpc.SendMessageSync(
		p.client.RemotingClient(), addr, m.Body, p.buildSendHeader(m, q, sysFlag), p.SendMsgTimeout,
	)
	if err != nil {
		p.Logger.Errorf("request send message %s sync error:%v", m.String(), err)
		return nil, err
	}

	var sendResult *SendResult
	switch resp.Code {
	case rpc.FlushDiskTimeout:
		sendResult = &SendResult{Status: FlushDiskTimeout}
	case rpc.FlushSlaveTimeout:
		sendResult = &SendResult{Status: FlushSlaveTimeout}
	case rpc.SlaveNotAvailable:
		sendResult = &SendResult{Status: SlaveNotAvailable}
	case rpc.Success:
		sendResult = &SendResult{Status: OK}
	default:
		p.Logger.Errorf("broker reponse code:%d, error:%s", resp.Code, resp.Message)
		return nil, fmt.Errorf("code:%d, err:%s", resp.Code, resp.Message)
	}

	sendResult.UniqID = m.GetUniqID()
	sendResult.QueueOffset = resp.QueueOffset
	sendResult.Queue = q
	sendResult.RegionID = resp.RegionID
	sendResult.OffsetID = resp.MsgID
	sendResult.TraceOn = resp.TraceOn
	sendResult.TransactionID = resp.TransactionID

	return sendResult, nil
}

func (p *Producer) buildSendHeader(m *message.Message, q *message.Queue, sysFlag int32) (
	header *rpc.SendHeader,
) {
	header = &rpc.SendHeader{
		Group:                 p.GroupName,
		Topic:                 m.Topic,
		DefaultTopic:          p.CreateTopicKey,
		DefaultTopicQueueNums: p.DefaultTopicQueueNums,
		QueueID:               q.QueueID,
		SysFlag:               sysFlag,
		BornTimestamp:         rocketmq.UnixMilli(),
		Flag:                  m.Flag,
		Properties:            message.Properties2String(m.Properties),
		UnitMode:              p.IsUnitMode,
		Batch:                 false,
	}

	if strings.HasPrefix(header.Topic, rocketmq.RetryGroupTopicPrefix) {
		resumeTime := m.GetProperty(message.PropertyReconsumeTime)
		if resumeTime != "" {
			i, err := strconv.Atoi(resumeTime)
			if err != nil {
				panic("BUG: bad resume time:" + resumeTime)
			}
			m.ClearProperty(message.PropertyReconsumeTime)
			header.ReconsumeTimes = int32(i)
		}

		maxResumeTime := m.GetProperty(message.PropertyMaxReconsumeTimes)
		if maxResumeTime != "" {
			i, err := strconv.Atoi(maxResumeTime)
			if err != nil {
				panic("BUG: bad max resume time:" + maxResumeTime)
			}
			m.ClearProperty(message.PropertyMaxReconsumeTimes)
			header.MaxReconsumeTimes = int32(i)
		}
	}
	return
}

func (p *Producer) tryToCompress(m *message.Message) bool {
	return false // TODO
}
