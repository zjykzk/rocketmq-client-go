package producer

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zlib"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// Config the configuration of producer
type Config struct {
	rocketmq.Client
	SendMsgTimeout                   time.Duration
	CompressSizeThreshod             int32
	CompressLevel                    int32
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
	CompressLevel:                    5,
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
	topicPublishInfos *topicPublishInfoTable
	client            mqClient
	mqFaultStrategy   *MQFaultStrategy

	logger log.Logger
}

// New creates procuder
func New(group string, namesrvAddrs []string, logger log.Logger) *Producer {
	p := &Producer{
		Config:            defaultConfig,
		logger:            logger,
		Server:            rocketmq.Server{State: rocketmq.StateCreating},
		topicPublishInfos: &topicPublishInfoTable{table: make(map[string]*topicPublishInfo)},
	}
	p.StartFunc, p.ShutdownFunc = p.start, p.shutdown
	p.GroupName = group
	p.NameServerAddrs = namesrvAddrs
	return p
}

// Start producer's worker
func (p *Producer) start() (err error) {
	p.logger.Info("start producer")
	p.updateInstanceName()

	p.ClientIP, err = rocketmq.GetIPStr()
	if err != nil {
		p.logger.Errorf("no ip")
		return
	}

	p.ClientID = client.BuildMQClientID(p.ClientIP, p.UnitName, p.InstanceName)
	p.client, err = client.New(
		&client.Config{
			HeartbeatBrokerInterval: p.HeartbeatBrokerInterval,
			PollNameServerInterval:  p.PollNameServerInterval,
			NameServerAddrs:         p.NameServerAddrs,
		}, p.ClientID, p.logger)
	if err != nil {
		return
	}

	err = p.client.RegisterProducer(p)
	if err != nil {
		p.logger.Errorf("register producer error:%s", err.Error())
		return
	}

	err = p.client.Start()
	p.mqFaultStrategy = NewMQFaultStrategy(true)
	return
}

func (p *Producer) updateInstanceName() {
	if p.GroupName != rocketmq.ClientInnerProducerGroup && p.InstanceName == "DEFAULT" {
		p.InstanceName = strconv.Itoa(os.Getpid())
	}
}

// Shutdown shutdown the producer
func (p *Producer) shutdown() {
	p.logger.Info("shutdown producer:" + p.GroupName)
	p.client.UnregisterProducer(p.GroupName)
	p.client.Shutdown()
	p.logger.Infof("shutdown producer:%s END", p.GroupName)
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
	p.logger.Debugf("update topic publish %s %s", topic, router.String())
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
		p.logger.Info("UpdateTopicPublish prev is not null, " + prev.String())
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
	err = p.CheckRuning()
	if err != nil {
		return
	}

	return p.sendSync(&messageWrap{Message: m})
}

type messageWrap struct {
	*message.Message
	isBatch bool
}

func (p *Producer) sendSync(m *messageWrap) (sendResult *SendResult, err error) {

	err = p.checkMessage(m.Message)
	if err != nil {
		return
	}

	routers, err := p.getRouters(m.Topic)
	if err != nil {
		return
	}

	originBody := m.Body

	sysFlag := int32(0)
	ok, err := p.tryToCompress(m.Message)
	if err != nil {
		return
	}
	if ok {
		sysFlag |= message.Compress
	}

	m.SetUniqID(message.CreateUniqID())
	sendResult, err = p.sendMessageWithLatency(routers, m, sysFlag)

	m.Body = originBody

	return
}

func (p *Producer) checkMessage(m *message.Message) error {
	if m == nil {
		return errEmptyMessage
	}

	if len(m.Body) == 0 {
		return errEmptyBody
	}

	if len(m.Body) > int(p.MaxMessageSize) {
		return errLargeBody
	}

	return rocketmq.CheckTopic(m.Topic)
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
			p.logger.Errorf("update topic router from namesrv error:%s", err)
			return nil, err
		}
		pi = p.topicPublishInfos.get(topic)
	}

	if !pi.hasQueue() {
		return nil, errNoRouters
	}

	return pi, nil
}

func (p *Producer) sendMessageWithLatency(
	router *topicPublishInfo, m *messageWrap, sysFlag int32,
) (
	sendResult *SendResult, err error,
) {
	var (
		q           *message.Queue
		brokersSent = make([]string, p.RetryTimesWhenSendFailed+1)
		retryCount  = int32(1)
	)

	startPoint := time.Now()
	prev := startPoint
	for maxSendCount := p.RetryTimesWhenSendFailed + 1; retryCount < maxSendCount; retryCount++ {
		q = p.mqFaultStrategy.SelectOneQueue(router, brokersSent[retryCount-1])
		sendResult, err = p.sendMessageWithQueueSync(m, q, sysFlag)

		now := time.Now()
		cost := now.Sub(prev)

		prev = now
		brokersSent[retryCount] = q.BrokerName

		if err != nil {
			p.mqFaultStrategy.UpdateFault(q.BrokerName, cost, true)
			p.logger.Errorf("resend at once %s RT:%s, Queue:%s, err %s", m.GetUniqID(), cost, q, err)
			p.logger.Warn(m.String())
			continue
		}

		p.mqFaultStrategy.UpdateFault(q.BrokerName, cost, false)
		goto END
	}

	p.logger.Errorf("send %d times, still failed, cost %s, topic:%s, sendBrokers:%v",
		retryCount-1, time.Now().Sub(startPoint), m.Topic, brokersSent[1:])
END:

	return
}

func (p *Producer) sendMessageWithQueueSync(m *messageWrap, q *message.Queue, sysFlag int32) (
	*SendResult, error,
) {
	resp, err := p.client.SendMessageSync(
		q.BrokerName, m.Body, p.buildSendHeader(m, q, sysFlag), p.SendMsgTimeout,
	)
	if err != nil {
		p.logger.Errorf("request send message %s sync error:%v", m.String(), err)
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
		p.logger.Errorf("broker reponse code:%d, error:%s", resp.Code, resp.Message)
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

func (p *Producer) buildSendHeader(m *messageWrap, q *message.Queue, sysFlag int32) (
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
		Batch:                 m.isBatch,
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

func (p *Producer) tryToCompress(m *message.Message) (bool, error) {
	body := m.Body
	if len(body) < int(p.CompressSizeThreshod) {
		return false, nil
	}

	data, err := compress(body, int(p.CompressLevel))
	if err == nil {
		m.Body = data
	} else {
		return false, err
	}

	return true, nil
}

// compress compress the data, the result maybe different from the result generated by the java
// but they are compitable
func compress(data []byte, level int) ([]byte, error) {
	var b bytes.Buffer

	w, err := zlib.NewWriterLevel(&b, level)
	if err != nil {
		return nil, err
	}

	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// SendBatchSync send the batch message sync
func (p *Producer) SendBatchSync(batch *message.Batch) (sendResult *SendResult, err error) {
	m, err := batch.ToMessage()
	if err != nil {
		return
	}

	return p.sendSync(&messageWrap{Message: m, isBatch: true})
}
