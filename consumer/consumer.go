package consumer

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote/rpc"
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
	Changed(topic string, all, divided []*message.Queue)
}

// Config the configuration of consumer
type Config struct {
	rocketmq.Client
	ReblanceInterval time.Duration
	MessageModel     Model
	Typ              Type
	FromWhere        fromWhere
}

const (
	defaultInstanceName = "DEFAULT"
)

var defaultConfig = Config{
	Client: rocketmq.Client{
		HeartbeatBrokerInterval:       30 * time.Second,
		PollNameServerInterval:        30 * time.Second,
		PersistConsumerOffsetInterval: 5 * time.Second,
		InstanceName:                  defaultInstanceName,
	},
	ReblanceInterval: 20 * time.Second, // 20s
}

type consumer struct {
	Config
	rocketmq.Server
	MessageQueueChanged MessageQueueChanger

	subscribeQueues *client.QueueTable
	subscribeData   *client.DataTable
	topicRouters    *route.TopicRouterTable
	reblancer       messageQueueReblancer
	assigner        queueAssigner
	offseter        offseter
	startTime       time.Time
	rpc             rpcI

	runnerInfo func() client.RunningInfo

	brokerSuggester brokerSuggester

	sync.WaitGroup
	exitChan chan struct{}

	client client.MQClient

	Logger log.Logger
}

// Start the works of consumer
func (c *consumer) start() (err error) {
	c.ClientIP, err = rocketmq.GetIPStr()
	if err != nil {
		c.Logger.Errorf("no ip")
		return
	}

	if c.MessageModel == Clustering {
		c.InstanceName = strconv.Itoa(os.Getpid())
	}

	c.subscribeQueues = client.NewQueueTable()
	c.subscribeData = client.NewDataTable()
	c.topicRouters = route.NewTopicRouterTable()
	c.brokerSuggester.table = make(map[string]int32, 32)

	c.ClientID = client.BuildMQClientID(c.ClientIP, c.UnitName, c.InstanceName)
	c.client, err = client.NewMQClient(
		&client.Config{
			HeartbeatBrokerInterval: c.HeartbeatBrokerInterval,
			PollNameServerInterval:  c.PollNameServerInterval,
			NameServerAddrs:         c.NameServerAddrs,
		}, c.ClientID, c.Logger)
	if err != nil {
		c.Logger.Errorf("new MQ client error:%s", err)
		return
	}

	err = c.client.RegisterConsumer(c)
	if err != nil {
		c.Logger.Errorf("register producer error:%s", err.Error())
		return
	}

	err = c.client.Start()
	if err != nil {
		c.Logger.Errorf("start mq client error:%s", err)
		return
	}

	err = c.initOffset()
	if err != nil {
		c.Logger.Errorf("initialize the offset error:%s", err)
		return
	}

	c.rpc = rpc.NewRPC(c.client.RemotingClient())
	c.startTime = time.Now()
	c.exitChan = make(chan struct{})
	c.schedule(time.Second, c.ReblanceInterval, c.ReblanceQueue)
	c.schedule(time.Second, c.PersistConsumerOffsetInterval, c.PersistOffset)
	return
}

// Shutdown the works of consumer
func (c *consumer) shutdown() {
	c.Logger.Infof("Shutdown consumer, group:%s, clientID:%s", c.GroupName, c.ClientID)
	c.client.UnregisterConsumer(c.GroupName)
	c.client.Shutdown()
	c.offseter.persist()
	close(c.exitChan)
	c.Wait()
	c.Logger.Infof("Shutdown consumer, group:%s, clientID:%s OK", c.GroupName, c.ClientID)
}

func (c *consumer) initOffset() (err error) {
	switch c.MessageModel {
	case BroadCasting:
		c.offseter, err = newLocalStore(localStoreConfig{clientID: c.ClientID, group: c.GroupName})
	case Clustering:
		c.offseter, err = newRemoteStore(remoteStoreConfig{offsetOperAdaptor{c}, c.Logger})
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

		ticker := time.NewTicker(period)
		f()
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

func (c *consumer) Subscriptions() []*client.Data {
	return c.subscribeData.Datas()
}

func (c *consumer) findBrokerAddrByTopic(topic string) string {
	tr := c.topicRouters.Get(topic)
	if tr == nil {
		c.Logger.Warnf("cannot find topic:%s", topic)
		return ""
	}

	brokers := tr.Brokers
	if len(brokers) == 0 {
		return ""
	}

	return brokers[rand.Intn(len(brokers))].SelectAddress()
}

func (c *consumer) Subscribe(topic string) {
	if c.subscribeData.Get(topic) != nil {
		return
	}

	c.subscribeData.PutIfAbsent(topic, BuildSubscribeData(c.GroupName, topic, ""))
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
	offset, rpcErr := c.rpc.QueryConsumerOffset(addr, q.Topic, c.Group(), int(q.QueueID), time.Second*5)
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
	offset, rpcErr := c.rpc.MaxOffset(addr, q.Topic, q.QueueID, time.Second*5)
	if rpcErr == nil {
		return offset, nil
	}

	return 0, rpcErr
}

func (c *consumer) UpdateOffset(q *message.Queue, offset int64, oneway bool) error {
	addr, err := c.findBrokerAddr(q.BrokerName, q.Topic, false)
	if err != nil {
		c.Logger.Errorf("update offset failed:%s", err)
		return nil
	}
	if oneway {
		return c.rpc.UpdateConsumerOffsetOneway(addr, q.Topic, c.Group(), int(q.QueueID), offset)
	}
	return c.rpc.UpdateConsumerOffset(addr, q.Topic, c.Group(), int(q.QueueID), offset, time.Second*5)
}

func (c *consumer) PersistOffset() {
	err := c.offseter.persist()
	if err != nil {
		c.Logger.Errorf("persist consume offset error:%s", err)
	}
}

func (c *consumer) reblanceClustering(topic string, queues []*message.Queue) (
	[]*message.Queue, error,
) {
	clientIDs := c.getConsumerIDs(topic, c.GroupName)
	if len(clientIDs) == 0 {
		err := fmt.Errorf("no client id of group:" + c.GroupName)
		c.Logger.Warn(err)
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
		c.Logger.Error(err)
		return nil, err
	}

	c.Logger.Debugf(
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
		c.Logger.Warn("GET CONSUMER IDS: no broker address found")
		return nil
	}

	clientIDs, err := c.rpc.GetConsumerIDs(addr, group, time.Second*3)
	if err != nil {
		c.Logger.Errorf("get client ids error:%s, group %s, broker %s", err, group, addr)
		return nil
	}

	if len(clientIDs) == 0 {
		c.Logger.Warnf("no consumer ids of %s in broker %s", group, addr)
	}

	return clientIDs
}

func (c *consumer) reblanceQueue(topic string) ([]*message.Queue, []*message.Queue, error) {
	queues := c.subscribeQueues.Get(topic)
	if len(queues) == 0 {
		c.Logger.Warn("no consumer queue of topic:" + topic)
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
