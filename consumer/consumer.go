package consumer

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type queueAssigner interface {
	Assign(group, clientID string, clientIDs []string, qs []*message.Queue) ([]*message.Queue, error)
	Name() string
}

type offseter interface {
	persist() error
	updateQueues(...*message.Queue)
	updateOffsetIfGreater(mq *message.Queue, offset int64)
	persistOne(mq *message.Queue)
	removeOffset(mq *message.Queue) (offset int64, ok bool)
	readOffset(mq *message.Queue, typ int) (offset int64, err error)
}

type messageQueueReblancer interface {
	reblance(topic string)
}

// MessageQueueChanger the callback when the consume queue is changed
type MessageQueueChanger interface {
	Change(topic string, all, divided []*message.Queue)
}

// Config the configuration of consumer
type Config struct {
	rocketmq.Client
	ReblanceInterval  time.Duration
	MessageModel      Model
	Typ               Type
	FromWhere         fromWhere
	MaxReconsumeTimes int
}

const (
	defaultInstanceName  = "DEFAULT"
	defaultConsumerGroup = "DEFAULT_CONSUMER"
)

var defaultConfig = Config{
	Client: rocketmq.Client{
		HeartbeatBrokerInterval:       30 * time.Second,
		PollNameServerInterval:        30 * time.Second,
		PersistConsumerOffsetInterval: 5 * time.Second,
		InstanceName:                  defaultInstanceName,
	},
	MaxReconsumeTimes: 16,
	ReblanceInterval:  20 * time.Second, // 20s
}

type consumer struct {
	Config
	rocketmq.Server
	messageQueueChanger MessageQueueChanger

	subscribeQueues *client.SubscribeQueueTable
	subscribeData   *client.SubscribeDataTable
	topicRouters    *route.TopicRouterTable
	reblancer       messageQueueReblancer
	assigner        queueAssigner
	offseter        offseter
	startTime       time.Time

	runnerInfo func() client.RunningInfo

	brokerSuggester *brokerSuggester

	sync.WaitGroup
	exitChan chan struct{}

	client mqClient

	logger log.Logger
}

// Start the works of consumer
func (c *consumer) start() (err error) {
	err = c.checkConfig()
	if err != nil {
		return
	}

	c.ClientIP, err = rocketmq.GetIPStr()
	if err != nil {
		c.logger.Errorf("no ip")
		return
	}

	c.updateInstanceName()

	c.subscribeQueues = client.NewSubscribeQueueTable()
	c.subscribeData = client.NewSubcribeTable()
	c.topicRouters = route.NewTopicRouterTable()

	err = c.buildMQClient()
	if err != nil {
		c.logger.Errorf("new MQ client error:%s", err)
		return
	}

	err = c.client.RegisterConsumer(c)
	if err != nil {
		c.logger.Errorf("register producer error:%s", err.Error())
		return
	}

	err = c.client.Start()
	if err != nil {
		c.logger.Errorf("start mq client error:%s", err)
		return
	}

	err = c.initOffset()
	if err != nil {
		c.logger.Errorf("initialize the offset error:%s", err)
		return
	}

	c.startTime = time.Now()
	c.exitChan = make(chan struct{})

	c.scheduleTasks()
	return
}

func (c *consumer) checkConfig() error {
	err := rocketmq.CheckGroup(c.GroupName)
	if err != nil {
		return err
	}

	if c.GroupName == defaultConsumerGroup {
		return errors.New("default consumer group")
	}

	if c.assigner == nil {
		return errors.New("empty message queue assigner")
	}

	return nil
}

func (c *consumer) buildMQClient() (err error) {
	c.ClientID = client.BuildMQClientID(c.ClientIP, c.UnitName, c.InstanceName)
	c.client, err = client.New(
		&client.Config{
			HeartbeatBrokerInterval: c.HeartbeatBrokerInterval,
			PollNameServerInterval:  c.PollNameServerInterval,
			NameServerAddrs:         c.NameServerAddrs,
		}, c.ClientID, c.logger,
	)
	return
}

func (c *consumer) updateInstanceName() {
	if c.MessageModel == Clustering && c.InstanceName == defaultInstanceName {
		c.InstanceName = strconv.Itoa(os.Getpid())
	}
}

func (c *consumer) scheduleTasks() {
	c.schedule(time.Second, c.ReblanceInterval, c.ReblanceQueue)
	c.schedule(time.Second, c.PersistConsumerOffsetInterval, c.PersistOffset)
}

// Shutdown the works of consumer
func (c *consumer) shutdown() {
	c.logger.Infof("Shutdown consumer, group:%s, clientID:%s", c.GroupName, c.ClientID)
	c.client.UnregisterConsumer(c.GroupName)
	c.client.Shutdown()
	c.offseter.persist()
	close(c.exitChan)
	c.Wait()
	c.logger.Infof("Shutdown consumer, group:%s, clientID:%s OK", c.GroupName, c.ClientID)
}

func (c *consumer) initOffset() (err error) {
	switch c.MessageModel {
	case BroadCasting:
		c.offseter, err = newLocalStore(localStoreConfig{clientID: c.ClientID, group: c.GroupName})
	case Clustering:
		c.offseter, err = newRemoteStore(remoteStoreConfig{offsetOperAdaptor{c}, c.logger})
	default:
		err = fmt.Errorf("unknow message model:%v", c.MessageModel)
	}
	return
}

func (c *consumer) schedule(delay, period time.Duration, f func()) {
	c.Add(1)
	go func() {
		defer c.Done()
		select {
		case <-time.After(delay):
		case <-c.exitChan:
			return
		}

		f()
		ticker := time.NewTicker(period)
		for {
			select {
			case <-ticker.C:
				f()
			case <-c.exitChan:
				ticker.Stop()
				return
			}
		}
	}()
}

// ReblanceQueue reblances the consume queues between the different consumers
func (c *consumer) ReblanceQueue() {
	for _, t := range c.subscribeData.Topics() {
		c.reblancer.reblance(t)
	}
}

func (c *consumer) Group() string {
	return c.GroupName
}

func (c *consumer) SubscribeTopics() []string {
	return c.subscribeData.Topics()
}

func (c *consumer) NeedUpdateTopicSubscribe(topic string) bool {
	return c.subscribeData.Get(topic) != nil && len(c.subscribeQueues.Get(topic)) == 0
}

// UpdateTopicSubscribe only updates the subsribed topic
func (c *consumer) UpdateTopicSubscribe(topic string, router *route.TopicRouter) {
	if c.subscribeData.Get(topic) == nil {
		return
	}

	qs := make([]*message.Queue, 0, 8)
	for _, q := range router.Queues {
		if !route.IsReadable(q.Perm) {
			continue
		}

		for i := 0; i < q.ReadCount; i++ {
			qs = append(qs, &message.Queue{Topic: topic, BrokerName: q.BrokerName, QueueID: uint8(i)})
		}
	}

	c.subscribeQueues.Put(topic, qs)
	c.topicRouters.Put(topic, router)
}

func (c *consumer) ConsumeFromWhere() string {
	return c.FromWhere.String()
}

func (c *consumer) Model() string {
	return c.MessageModel.String()
}

func (c *consumer) Type() string {
	return c.Typ.String()
}

func (c *consumer) UnitMode() bool {
	return c.IsUnitMode
}

func (c *consumer) Subscriptions() []*client.SubscribeData {
	return c.subscribeData.Datas()
}

func (c *consumer) findBrokerAddrByTopic(topic string) string {
	tr := c.topicRouters.Get(topic)
	if tr == nil {
		c.logger.Warnf("cannot find topic:%s", topic)
		return ""
	}

	brokers := tr.Brokers
	if len(brokers) == 0 {
		return ""
	}

	return brokers[rand.Intn(len(brokers))].SelectAddress()
}

func (c *consumer) Subscribe(topic string, expr string) {
	if c.subscribeData.Get(topic) != nil {
		return
	}

	c.subscribeData.PutIfAbsent(topic, BuildSubscribeData(c.GroupName, topic, expr))
}

func (c *consumer) Unsubscribe(topic string) {
	c.subscribeData.Delete(topic)
	c.subscribeQueues.Delete(topic)
	c.topicRouters.Delete(topic)
}

func (c *consumer) selectBrokerID(q *message.Queue) int32 {
	id, exist := c.brokerSuggester.get(q)
	if !exist {
		id = rocketmq.MasterID
	}
	return id
}

// RunningInfo returns the consumter's running information
func (c *consumer) RunningInfo() client.RunningInfo {
	return c.runnerInfo()
}

func (c *consumer) findBrokerAddr(broker, topic string, mustMaster bool) (string, error) {
	addr, err := c.client.FindBrokerAddr(broker, rocketmq.MasterID, mustMaster)
	if err != nil {
		c.client.UpdateTopicRouterInfoFromNamesrv(topic)
		addr, err = c.client.FindBrokerAddr(broker, rocketmq.MasterID, mustMaster)
	}

	if err != nil {
		return "", err
	}

	return addr.Addr, nil
}

func (c *consumer) QueryConsumerOffset(q *message.Queue) (int64, error) {
	addr, err := c.findBrokerAddr(q.BrokerName, q.Topic, false)
	if err != nil {
		return 0, fmt.Errorf("cannot find broker address:%s %s, error:%s", q.BrokerName, q.Topic, err)
	}
	offset, rpcErr := c.client.QueryConsumerOffset(addr, q.Topic, c.Group(), int(q.QueueID), time.Second*5)
	if rpcErr == nil {
		return offset, nil
	}

	if rpcErr.Code == rpc.QueryNotFound {
		return 0, errOffsetNotExist
	}

	return offset, rpcErr
}

func (c *consumer) QueryMaxOffset(q *message.Queue) (int64, error) {
	addr, err := c.findBrokerAddr(q.BrokerName, q.Topic, true)
	if err != nil {
		return 0, fmt.Errorf("cannot find broker address:%s %s, error:%s", q.BrokerName, q.Topic, err)
	}
	offset, rpcErr := c.client.MaxOffset(addr, q.Topic, q.QueueID, time.Second*5)
	if rpcErr == nil {
		return offset, nil
	}

	return 0, rpcErr
}

func (c *consumer) UpdateOffset(q *message.Queue, offset int64, oneway bool) error {
	addr, err := c.findBrokerAddr(q.BrokerName, q.Topic, false)
	if err != nil {
		c.logger.Errorf("update offset failed:%s", err)
		return nil
	}
	if oneway {
		return c.client.UpdateConsumerOffsetOneway(addr, q.Topic, c.Group(), int(q.QueueID), offset)
	}
	return c.client.UpdateConsumerOffset(addr, q.Topic, c.Group(), int(q.QueueID), offset, time.Second*5)
}

func (c *consumer) PersistOffset() {
	err := c.offseter.persist()
	if err != nil {
		c.logger.Errorf("persist consume offset error:%s", err)
	}
}

func (c *consumer) reblanceQueue(topic string) ([]*message.Queue, []*message.Queue, error) {
	queues := c.subscribeQueues.Get(topic)
	if len(queues) == 0 {
		c.logger.Warn("no consumer queue of topic:" + topic)
		return nil, nil, nil
	}

	if c.MessageModel == BroadCasting {
		return queues, queues, nil
	}
	newQueues, err := c.reblanceClustering(topic, queues)
	if err != nil {
		return nil, nil, err
	}
	return queues, newQueues, nil
}

func (c *consumer) reblanceClustering(topic string, queues []*message.Queue) (
	[]*message.Queue, error,
) {
	clientIDs := c.getConsumerIDs(topic, c.GroupName)
	if len(clientIDs) == 0 {
		err := fmt.Errorf("no client id of group:" + c.GroupName)
		c.logger.Warn(err)
		return nil, err
	}

	// since the queues is readonly, if donot copy it, the sort operation following will cause
	// the data race
	all := make([]*message.Queue, len(queues))
	copy(all, queues)
	message.SortQueue(all)
	sort.Strings(clientIDs)
	divided, err := c.assigner.Assign(c.GroupName, c.ClientID, clientIDs, all)
	if err != nil {
		err = fmt.Errorf("reblance %s:%s clients:%v, error:%s", c.GroupName, c.ClientID, clientIDs, err)
		c.logger.Error(err)
		return nil, err
	}

	c.logger.Debugf(
		"message queue changed:%s, clientIDs:%v, all:%v, divided:%v",
		topic, clientIDs, all, divided,
	)
	return divided, nil
}

func (c *consumer) getConsumerIDs(topic, group string) []string {
	addr := c.findBrokerAddrByTopic(topic)
	if addr == "" {
		_ = c.client.UpdateTopicRouterInfoFromNamesrv(topic) // IGNORE the error
		addr = c.findBrokerAddrByTopic(topic)
	}

	if addr == "" {
		c.logger.Warn("GET CONSUMER IDS: no broker address found")
		return nil
	}

	clientIDs, err := c.client.GetConsumerIDs(addr, group, time.Second*3)
	if err != nil {
		c.logger.Errorf("get client ids error:%s, group %s, broker %s", err, group, addr)
		return nil
	}

	if len(clientIDs) == 0 {
		c.logger.Warnf("no consumer ids of %s in broker %s", group, addr)
	}

	return clientIDs
}

// SendBack send back message
func (c *consumer) SendBack(m *message.Ext, delayLevel int, group, brokerName string) error {
	if group == "" {
		group = c.GroupName
	}

	addr, err := c.findSendBackBrokerAddr(brokerName, m.MsgID)
	if err != nil {
		return err
	}

	return c.client.SendBack(addr, &rpc.SendBackHeader{
		CommitOffset:      m.CommitLogOffset,
		Group:             group,
		DelayLevel:        int32(delayLevel),
		MessageID:         m.MsgID,
		Topic:             m.Topic,
		MaxReconsumeTimes: c.MaxReconsumeTimes,
	}, time.Second*3)
}

func (c *consumer) findSendBackBrokerAddr(broker, msgIDInBroker string) (string, error) {
	if broker != "" {
		r, err := c.client.FindBrokerAddr(broker, rocketmq.MasterID, true)
		if err == nil {
			return r.Addr, nil
		}
		return "", err
	}

	a, _, err := message.ParseMessageID(msgIDInBroker)

	if err == nil {
		return a.String(), nil
	}
	return "", err
}

type offsetOperAdaptor struct {
	*consumer
}

func (oa offsetOperAdaptor) update(q *message.Queue, offset int64) error {
	return oa.UpdateOffset(q, offset, true)
}

func (oa offsetOperAdaptor) fetch(q *message.Queue) (int64, error) {
	return oa.QueryConsumerOffset(q)
}

func retryTopic(group string) string {
	return rocketmq.RetryGroupTopicPrefix + group
}
