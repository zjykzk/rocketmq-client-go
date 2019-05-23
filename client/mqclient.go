package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type processor func(*remote.ChannelContext, *remote.Command) (*remote.Command, error)

// MQClient the client commuicate with broker
// it's shared by all consumer & producer with the same client id
type MQClient struct {
	Config
	sync.RWMutex
	nameSrvMutex sync.Mutex
	sync.WaitGroup

	exitChan chan struct{}
	state    rocketmq.State

	remote.Client

	clientID string

	consumers consumerColl
	producers producerColl
	admins    adminColl

	brokerAddrs    brokerAddrTable
	brokerVersions brokerVersionTable

	routersOfTopic *route.TopicRouterTable

	processors map[remote.Code]processor

	logger log.Logger
}

var errBadNamesrvAddrs = errors.New("bad name server address")

type mqClientColl struct {
	sync.RWMutex
	eles map[string]*MQClient
}

func (cc *mqClientColl) delete(clientID string) bool {
	cc.Lock()
	_, ok := cc.eles[clientID]
	if ok {
		delete(cc.eles, clientID)
	}
	cc.Unlock()
	return ok
}

var mqClients = mqClientColl{
	eles: make(map[string]*MQClient),
}

var errEmptyClientID = errors.New("empty client id")
var errEmptyNameSrvAddress = errors.New("empty name server address")

func newMQClient(config *Config, clientID string, logger log.Logger) (c *MQClient, err error) {
	c = &MQClient{
		clientID:       clientID,
		exitChan:       make(chan struct{}),
		consumers:      consumerColl{eles: make(map[string]Consumer)},
		producers:      producerColl{producers: make(map[string]Producer)},
		admins:         adminColl{eles: make(map[string]Admin)},
		brokerAddrs:    brokerAddrTable{table: make(map[string]map[int32]string)},
		brokerVersions: brokerVersionTable{table: make(map[string]map[string]int32)},
		routersOfTopic: route.NewTopicRouterTable(),
		logger:         logger,
	}

	c.initProcessor()

	c.Config = *config
	if c.HeartbeatBrokerInterval <= 0 {
		c.HeartbeatBrokerInterval = 1000 * 30
	}
	if c.PollNameServerInterval <= 0 {
		c.PollNameServerInterval = 1000 * 30
	}
	c.Client, err = remote.NewClient(remote.ClientConfig{
		ReadTimeout:  config.HeartbeatBrokerInterval * 2,
		WriteTimeout: time.Millisecond * 100,
		DialTimeout:  time.Second,
	}, c.processRequest, logger)
	return
}

// New create the client
func New(config *Config, clientID string, logger log.Logger) (c *MQClient, err error) {
	if clientID == "" {
		return nil, errEmptyClientID
	}

	if len(config.NameServerAddrs) == 0 {
		return nil, errEmptyNameSrvAddress
	}

	mqClients.RLock()
	c, ok := mqClients.eles[clientID]
	mqClients.RUnlock()

	if ok {
		return c, nil
	}

	mqClients.Lock()
	c, ok = mqClients.eles[clientID]
	if !ok {
		c, err = newMQClient(config, clientID, logger)
		mqClients.eles[clientID] = c
	}
	mqClients.Unlock()
	return
}

// RegisterConsumer registers consumer
func (c *MQClient) RegisterConsumer(co Consumer) error {
	group := co.Group()
	if group == "" || co == nil {
		return errors.New("bad consumer params")
	}

	if _, suc := c.consumers.putIfAbsent(group, co); !suc {
		c.logger.Error("the consumer group [" + group + "] exist")
		return fmt.Errorf("consumer %s has registered", group)
	}
	return nil
}

// UnregisterConsumer unregister producer
func (c *MQClient) UnregisterConsumer(group string) {
	c.consumers.delete(group)
	c.unregisterClient("", group)
}

// RegisterProducer registers producer
func (c *MQClient) RegisterProducer(p Producer) error {
	group := p.Group()
	if group == "" || p == nil {
		return errors.New("bad producer params")
	}

	if _, suc := c.producers.putIfAbsent(group, p); !suc {
		c.logger.Error("the producer group [" + group + "] exist")
		return fmt.Errorf("producer [%s] registered", group)
	}
	return nil
}

// UnregisterProducer unregister producer
func (c *MQClient) UnregisterProducer(group string) {
	c.producers.delete(group)
	c.unregisterClient(group, "")
}

func (c *MQClient) unregisterClient(producerGroup, consumerGroup string) {
	clientID, timeout := c.clientID, 3*time.Second
	c.Lock()
	for _, name := range c.brokerAddrs.brokerNames() {
		for _, addr := range c.brokerAddrs.brokerAddrs(name) {
			err := rpc.UnregisterClient(
				c.Client, addr.addr, clientID, producerGroup, consumerGroup, timeout,
			)
			c.logger.Infof(
				"unregister client p:%s c:%s from addr %s, err:%v",
				producerGroup, consumerGroup, addr.addr, err,
			)
		}
	}
	c.Unlock()
}

// RegisterAdmin registers admin
func (c *MQClient) RegisterAdmin(a Admin) error {
	group := a.Group()
	if group == "" || a == nil {
		return errors.New("bad admin params")
	}

	if _, suc := c.admins.putIfAbsent(group, a); !suc {
		c.logger.Error("the admin group [" + group + "] exist")
		return fmt.Errorf("admin %s has registered", group)
	}
	return nil
}

// UnregisterAdmin unregister producer
func (c *MQClient) UnregisterAdmin(group string) {
	c.admins.delete(group)
}

// Start client tasks
func (c *MQClient) Start() error {
	c.Lock()
	defer c.Unlock()
	switch c.state {
	case rocketmq.StateCreating:
		c.Client.Start()
		c.scheduleTasks()
		c.state = rocketmq.StateRunning
	case rocketmq.StateRunning:
		c.logger.Warnf("the client of [%s] is RUNNING", c.clientID)
	case rocketmq.StateStopped:
		return errors.New("stopped")
	case rocketmq.StateStartFailed:
		return fmt.Errorf("[%s] has created before, and failed", c.clientID)
	}
	return nil
}

// Shutdown client
func (c *MQClient) Shutdown() {
	c.logger.Infof("shutdown mqclient, state %s START", c.state.String())
	if c.producers.size() > 0 { // NOTE here is different from ROCKETMQ JAVA SDK, since the DefaultMQProducer is not stored here
		c.logger.Info("producer not empty ignore")
		return
	}

	if c.consumers.size() > 0 {
		c.logger.Info("consumer is not empty ignore")
		return
	}

	if c.admins.size() > 0 {
		c.logger.Info("admin not empty ignore")
		return
	}

	c.Lock()
	switch c.state {
	case rocketmq.StateRunning:
		c.Client.Shutdown()
		close(c.exitChan)
		c.Wait()
		mqClients.delete(c.clientID)
	case rocketmq.StateCreating, rocketmq.StateStopped:
	default:
	}
	c.Unlock()
	c.logger.Info("shutdown mqclient END")
}

// UpdateTopicRouterInfoFromNamesrv udpate the topic for the producer/consumer from the namesrv
func (c *MQClient) UpdateTopicRouterInfoFromNamesrv(topic string) (err error) {
	_, err = c.updateTopicRouterInfoFromNamesrv(topic)
	return
}

// GetMasterBrokerAddr returns the master broker address
func (c *MQClient) GetMasterBrokerAddr(brokerName string) string {
	return c.brokerAddrs.get(brokerName, rocketmq.MasterID)
}

// GetMasterBrokerAddrs returns all the master broker addresses
func (c *MQClient) GetMasterBrokerAddrs() []string {
	return c.brokerAddrs.getByBrokerID(rocketmq.MasterID)
}

// FindBrokerResult the data returned by FindBrokerAddr
type FindBrokerResult struct {
	Addr    string
	IsSlave bool
	Version int32
}

// FindBrokerAddr finds the broker address, returns the address with specified broker id first
// otherwise, returns any one
func (c *MQClient) FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (
	*FindBrokerResult, error,
) {
	addr := c.brokerAddrs.get(brokerName, hintBrokerID)
	if addr != "" {
		return &FindBrokerResult{
			Addr:    addr,
			IsSlave: hintBrokerID != rocketmq.MasterID,
			Version: c.brokerVersions.get(brokerName, addr),
		}, nil
	}

	if lock {
		return nil, fmt.Errorf("broker of %s not exist", brokerName)
	}

	return c.FindAnyBrokerAddr(brokerName)
}

// FindAnyBrokerAddr returns any broker whose name is the specified name
func (c *MQClient) FindAnyBrokerAddr(brokerName string) (
	*FindBrokerResult, error,
) {
	addrData, exist := c.brokerAddrs.anyOneAddrOf(brokerName)
	if !exist {
		return nil, fmt.Errorf("broker of %s not exist", brokerName)
	}

	return &FindBrokerResult{
		Addr:    addrData.addr,
		IsSlave: addrData.brokerID != rocketmq.MasterID,
		Version: c.brokerVersions.get(brokerName, addrData.addr),
	}, nil
}

func (c *MQClient) getTopicRouteInfo(topic string) (*route.TopicRouter, error) {
	var err error
	l := len(c.NameServerAddrs)
	for i, cc := rand.Intn(l), l; cc > 0; i, cc = i+1, cc-1 {
		addr := c.NameServerAddrs[i%l]
		router, e := rpc.GetTopicRouteInfo(c.Client, addr, topic, 3*time.Second)
		if e == nil {
			return router, nil
		}

		err = e
		c.logger.Errorf("request broker cluster info from %s, error:%s", addr, e)
	}
	return nil, err
}

func (c *MQClient) updateTopicRouterInfoFromNamesrv(topic string) (updated bool, err error) {
	c.nameSrvMutex.Lock()
	defer c.nameSrvMutex.Unlock()

	router, err := c.getTopicRouteInfo(topic)
	if err != nil {
		c.logger.Errorf("get topic router info error:%s", err)
		return
	}

	if !c.isDiff(topic, router) {
		c.logger.Infof("no diff of topic %s", topic)
		return
	}

	for _, bd := range router.Brokers {
		c.brokerAddrs.put(bd.Name, bd.Addresses)
	}

	for _, co := range c.consumers.coll() {
		co.UpdateTopicSubscribe(topic, router)
	}

	for _, p := range c.producers.coll() {
		p.UpdateTopicPublish(topic, router)
	}

	c.routersOfTopic.Put(topic, router)
	c.logger.Infof("topic router updated [%s->%v]", topic, router)

	return true, nil
}

func (c *MQClient) isDiff(topic string, router *route.TopicRouter) bool {
	route.SortBrokerData(router.Brokers)
	route.SortTopicQueue(router.Queues)

	old := c.routersOfTopic.Get(topic)
	if old == nil || !old.Equal(router) {
		c.logger.Infof("topic [%s] route info changed, old:%s, new:%s", topic, old, router)
		return true
	}

	for _, co := range c.consumers.coll() {
		if co.NeedUpdateTopicSubscribe(topic) {
			return true
		}
	}

	for _, p := range c.producers.coll() {
		if p.NeedUpdateTopicPublish(topic) {
			return true
		}
	}

	return false
}

func (c *MQClient) selectNamesrv() string {
	return c.NameServerAddrs[rand.Intn(len(c.NameServerAddrs))]
}

// SendHeartbeat send the heart beart to the broker server, it locked when sending the data
func (c *MQClient) SendHeartbeat() {
	hr := c.prepareHeartbeatData()

	hasConsumer, hasProducer := len(hr.Consumers) != 0, len(hr.Producers) != 0
	if !hasConsumer && !hasProducer {
		c.logger.Warn("send heartbeat, but no consumer and no producer")
		return
	}

	if c.brokerAddrs.size() == 0 {
		c.logger.Warn("send heartbeat, but broker address is empty")
		return
	}

	c.Lock()
	defer c.Unlock()
	for _, name := range c.brokerAddrs.brokerNames() {
		addrs := c.brokerAddrs.brokerAddrs(name)
		if len(addrs) == 0 {
			c.logger.Debug("broker [" + name + "] address is empty")
			continue
		}

		for i := range addrs {
			id, addr := addrs[i].brokerID, addrs[i].addr
			if !hasConsumer && id != rocketmq.MasterID {
				continue
			}

			version, err := rpc.SendHeartbeat(c.Client, addr, hr, 3*time.Second)
			if err != nil {
				c.logger.Error("send heartbeat to " + addr + " failed, err:" + err.Error())
				if c.isBrokerAddrInRouter(addr) {
					c.logger.Error("send heartbeat to " + addr + " failed, but it is in the router")
				}
				continue
			}

			c.brokerVersions.put(name, addr, int32(version))
			c.logger.Infof("send heartbeat to broker[%d %s %s], data:%v", id, name, addr, hr)
		}
	}
}

func (c *MQClient) prepareHeartbeatData() *rpc.HeartbeatRequest {
	ps := c.producers.coll()
	producers := make([]rpc.Producer, len(ps))
	for i := range ps {
		producers[i].Group = ps[i].Group()
	}

	cs := c.consumers.coll()
	consumers := make([]*rpc.Consumer, len(cs))
	for i := range consumers {
		c1 := cs[i]

		consumers[i] = &rpc.Consumer{
			FromWhere:    c1.ConsumeFromWhere(),
			Group:        c1.Group(),
			Model:        c1.Model(),
			Type:         c1.Type(),
			UnitMode:     c1.UnitMode(),
			Subscription: toRPCSubscriptionDatas(c1.Subscriptions()),
		}
	}

	return &rpc.HeartbeatRequest{
		ClientID:  c.clientID,
		Producers: producers,
		Consumers: consumers,
	}
}

func toRPCSubscriptionDatas(datas []*SubscribeData) []*rpc.SubscribeData {
	subscriptionDatas := make([]*rpc.SubscribeData, len(datas))
	for i, d := range datas {
		subscriptionDatas[i] = (*rpc.SubscribeData)(d)
	}
	return subscriptionDatas
}

func (c *MQClient) cleanOfflineBroker() {
	c.nameSrvMutex.Lock()
	defer c.nameSrvMutex.Unlock()

	for _, name := range c.brokerAddrs.brokerNames() {
		addrs, invalidCount := c.brokerAddrs.brokerAddrs(name), 0
		if len(addrs) == 0 {
			c.logger.Errorf("empty addr of broker:%s", name)
			continue
		}

		for _, addr := range addrs {
			if !c.isBrokerAddrInRouter(addr.addr) {
				c.logger.Debugf("addr %s is not in route, delete it", addr.addr)
				c.brokerAddrs.deleteAddr(name, addr.brokerID)
				invalidCount++
			}
		}
		if invalidCount == len(addrs) {
			c.brokerAddrs.deleteBroker(name)
		}
	}
}

func (c *MQClient) isBrokerAddrInRouter(addr string) bool {
	for _, r := range c.routersOfTopic.Routers() {
		for _, broker := range r.Brokers {
			for _, addr1 := range broker.Addresses {
				if addr == addr1 {
					return true
				}
			}
		}
	}
	return false
}

func (c *MQClient) scheduleTasks() {
	c.schedule(10*time.Millisecond, c.PollNameServerInterval, c.updateTopicRoute)
	c.schedule(time.Second, c.HeartbeatBrokerInterval, func() {
		c.SendHeartbeat()
		c.cleanOfflineBroker()
	})
}

func (c *MQClient) schedule(delay, period time.Duration, f func()) {
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
			case <-c.exitChan:
				ticker.Stop()
				return
			case <-ticker.C:
				f()
			}
		}
	}()
}

func (c *MQClient) updateTopicRoute() {
	topics := make([]string, 0, 256)

	for _, c := range c.consumers.coll() {
		topics = union(topics, c.SubscribeTopics())
	}

	for _, p := range c.producers.coll() {
		topics = union(topics, p.PublishTopics())
	}

	for _, t := range topics {
		c.updateTopicRouterInfoFromNamesrv(t)
	}
}

func union(s1, s2 []string) []string {
	r := s1[:]
OUT:
	for _, t2 := range s2 {
		for _, t1 := range s1 {
			if t1 == t2 {
				continue OUT
			}
		}
		r = append(r, t2)
	}
	return r
}

func (c *MQClient) initProcessor() {
	c.processors = map[remote.Code]processor{
		rpc.NotifyConsumerIdsChanged:    c.consumerIDsChanged,
		rpc.CheckTransactionState:       c.checkTransactionState,
		rpc.ResetConsumerClientOffset:   c.resetOffset,
		rpc.GetConsumerStatusFromClient: c.getConsumerStatusFromClient,
		rpc.GetConsumerRunningInfo:      c.getConsumerRunningInfo,
		rpc.ConsumeMessageDirectlyCode:  c.consumeMessageDirectly,
	}
}

func (c *MQClient) processRequest(ctx *remote.ChannelContext, cmd *remote.Command) (processed bool) {
	proc, ok := c.processors[cmd.Code]
	if !ok {
		return
	}

	c.logger.Debugf("processed Request:%v", cmd)

	processed = true
	resp, err := proc(ctx, cmd)

	if err != nil {
		c.logger.Errorf("process error:%s", err)
	}

	if cmd.IsOneway() {
		return
	}

	if resp == nil {
		return
	}

	resp.Opaque = cmd.Opaque
	resp.MarkResponse()
	cmd, err = c.Client.RequestSync(ctx.Address, resp, 3*time.Second)
	c.logger.Debugf("send response result:%s, err:%v", cmd, err)

	return
}

func (c *MQClient) checkTransactionState(ctx *remote.ChannelContext, cmd *remote.Command) (
	resp *remote.Command, err error,
) {
	return // TODO
}

func (c *MQClient) consumerIDsChanged(ctx *remote.ChannelContext, cmd *remote.Command) (
	resp *remote.Command, err error,
) {
	c.logger.Infof(
		"receive broker's notification[%s], the consumer group: %s changed, rebalance immediately",
		ctx.String(), cmd.ExtFields["group"],
	)

	for _, co := range c.consumers.coll() {
		co.ReblanceQueue()
	}
	return
}

func (c *MQClient) getConsumerRunningInfo(ctx *remote.ChannelContext, cmd *remote.Command) (
	resp *remote.Command, err error,
) {
	group := cmd.ExtFields["consumerGroup"]
	co := c.consumers.get(group)
	if co == nil {
		c.logger.Errorf("no consumer of group:%s", group)
		resp = remote.NewCommand(rpc.SystemError, nil)
		resp.Remark = fmt.Sprintf("The Consumer Group <%s> not exist in this consumer", group)
		return
	}

	info := co.RunningInfo()
	data, err := json.Marshal(info)
	if err != nil {
		c.logger.Errorf("marshal running info error:%s", err)
		return
	}

	resp = remote.NewCommandWithBody(rpc.Success, nil, data)
	return
}

func (c *MQClient) resetOffset(ctx *remote.ChannelContext, cmd *remote.Command) (
	resp *remote.Command, err error,
) {
	topic, group := cmd.ExtFields["topic"], cmd.ExtFields["group"]
	timestamp, isForce := cmd.ExtFields["timestamp"], cmd.ExtFields["isForce"]
	c.logger.Infof(
		"invoke reset offset operation from broker:%s.topic:%s,group:%s,timestamp:%s, isForce:%s",
		ctx.String(), topic, group, timestamp, isForce,
	)

	if len(cmd.Body) == 0 {
		c.logger.Info("reset offset, but empty body")
		return
	}

	offsets, err := parseResetOffsetRequest(string(cmd.Body))
	if err != nil {
		c.logger.Errorf("parse reset offsets error:%s", err)
		return
	}

	consumer := c.consumers.get(group)
	if consumer == nil {
		return
	}

	err = consumer.ResetOffset(topic, offsets)
	if err != nil {
		c.logger.Errorf("consumer of group:%s reset offset error:%s", group, err)
	}
	return
}

func (c *MQClient) consumeMessageDirectly(ctx *remote.ChannelContext, cmd *remote.Command) (
	resp *remote.Command, err error,
) {
	ms, err := message.Decode(cmd.Body)
	if err != nil {
		c.logger.Errorf("decode message error:%s", err)
		return
	}

	if len(ms) != 1 {
		err = fmt.Errorf("bad message count:%d", len(ms))
		c.logger.Error(err.Error())
		return
	}

	group := cmd.ExtFields["consumerGroup"]
	co := c.consumers.get(group)
	if co == nil {
		c.logger.Errorf("no consumer of group:%s", group)
		resp = remote.NewCommand(rpc.SystemError, nil)
		resp.Remark = fmt.Sprintf("The Consumer Group <%s> not exist in this consumer", group)
		return
	}

	broker := cmd.ExtFields["brokerName"]
	result, err := co.ConsumeMessageDirectly(ms[0], broker)
	if err != nil {
		resp = remote.NewCommand(rpc.SystemError, nil)
		resp.Remark = fmt.Sprintf("consume %s failed by consumer of group '%s'", ms[0], group)
		return
	}

	data, err := json.Marshal(result)
	if err != nil {
		c.logger.Errorf("marshal consume directly error:%s", err)
		return
	}

	resp = remote.NewCommandWithBody(rpc.Success, nil, data)
	return
}

func (c *MQClient) getConsumerStatusFromClient(ctx *remote.ChannelContext, cmd *remote.Command) (
	resp *remote.Command, err error,
) {
	return // TODO
}

// AdminCount return the registered admin count
func (c *MQClient) AdminCount() int {
	return c.admins.size()
}

// ConsumerCount return the registered consumer count
func (c *MQClient) ConsumerCount() int {
	return c.consumers.size()
}

// ProducerCount return the registered producer count
func (c *MQClient) ProducerCount() int {
	return c.producers.size()
}
