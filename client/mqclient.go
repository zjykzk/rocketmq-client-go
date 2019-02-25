package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/remote/net"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// MQClient exchange the message with server
type MQClient interface {
	Start() error
	Shutdown()

	RegisterProducer(p producer) error
	UnregisterProducer(group string)
	RegisterConsumer(co consumer) error
	UnregisterConsumer(group string)
	RegisterAdmin(a admin) error
	UnregisterAdmin(group string)
	UpdateTopicRouterInfoFromNamesrv(topic string) error

	AdminCount() int
	ConsumerCount() int
	ProducerCount() int

	GetMasterBrokerAddr(brokerName string) string
	GetMasterBrokerAddrs() []string
	FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (*FindBrokerResult, error)
	FindAnyBrokerAddr(brokerName string) (*FindBrokerResult, error)
	RemotingClient() remote.Client
	SendHeartbeat()
}

type mqClient struct {
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

	logger log.Logger
}

var errBadNamesrvAddrs = errors.New("bad name server address")

type mqClientColl struct {
	sync.RWMutex
	eles map[string]MQClient
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
	eles: make(map[string]MQClient),
}

var errEmptyClientID = errors.New("empty client id")
var errEmptyNameSrvAddress = errors.New("empty name server address")

func newMQClient(config *Config, clientID string, logger log.Logger) MQClient {
	c := &mqClient{
		clientID:       clientID,
		exitChan:       make(chan struct{}),
		consumers:      consumerColl{eles: make(map[string]consumer)},
		producers:      producerColl{eles: make(map[string]producer)},
		admins:         adminColl{eles: make(map[string]admin)},
		brokerAddrs:    brokerAddrTable{table: make(map[string]map[int32]string)},
		brokerVersions: brokerVersionTable{table: make(map[string]map[string]int32)},
		routersOfTopic: route.NewTopicRouterTable(),
		logger:         logger,
	}

	c.Config = *config
	if c.HeartbeatBrokerInterval <= 0 {
		c.HeartbeatBrokerInterval = 1000 * 30
	}
	if c.PollNameServerInterval <= 0 {
		c.PollNameServerInterval = 1000 * 30
	}
	c.Client = remote.NewClient(&net.Config{
		ReadTimeout:  config.HeartbeatBrokerInterval * 2,
		WriteTimeout: time.Millisecond * 100,
		DialTimeout:  time.Second,
	}, c.processRequest, logger)
	return c
}

// NewMQClient create the client
func NewMQClient(config *Config, clientID string, logger log.Logger) (
	MQClient, error,
) {
	if clientID == "" {
		return nil, errEmptyClientID
	}

	if len(config.NameServerAddrs) == 0 {
		return nil, errEmptyNameSrvAddress
	}

	mqClients.Lock()
	c, ok := mqClients.eles[clientID]
	if !ok {
		c = newMQClient(config, clientID, logger)
		mqClients.eles[clientID] = c
	}
	mqClients.Unlock()
	return c, nil
}

// RegisterConsumer registers consumer
func (c *mqClient) RegisterConsumer(co consumer) error {
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
func (c *mqClient) UnregisterConsumer(group string) {
	c.consumers.delete(group)
	c.unregisterClient("", group)
}

// RegisterProducer registers producer
func (c *mqClient) RegisterProducer(p producer) error {
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
func (c *mqClient) UnregisterProducer(group string) {
	c.producers.delete(group)
	c.unregisterClient(group, "")
}

func (c *mqClient) unregisterClient(producerGroup, consumerGroup string) {
	clientID, timeout := c.clientID, 3*time.Second
	c.Lock()
	for _, name := range c.brokerAddrs.brokerNames() {
		for _, addr := range c.brokerAddrs.brokerAddrs(name) {
			err := unregisterClient(c.Client, addr.addr, clientID, producerGroup, consumerGroup, timeout)
			c.logger.Infof(
				"unregister client p:%s c:%s from addr %s, err:%v",
				producerGroup, consumerGroup, addr.addr, err,
			)
		}
	}
	c.Unlock()
}

// RegisterAdmin registers admin
func (c *mqClient) RegisterAdmin(a admin) error {
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
func (c *mqClient) UnregisterAdmin(group string) {
	c.admins.delete(group)
}

// Start client tasks
func (c *mqClient) Start() error {
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
func (c *mqClient) Shutdown() {
	c.logger.Infof("shutdown mqclient, state %s", c.state.String())
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

func (c *mqClient) updateTopicRoute() {
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

// UpdateTopicRouterInfoFromNamesrv udpate the topic for the producer/consumer from the namesrv
func (c *mqClient) UpdateTopicRouterInfoFromNamesrv(topic string) (err error) {
	_, err = c.updateTopicRouterInfoFromNamesrv(topic)
	return
}

// GetMasterBrokerAddr returns the master broker address
func (c *mqClient) GetMasterBrokerAddr(brokerName string) string {
	return c.brokerAddrs.get(brokerName, rocketmq.MasterID)
}

// GetMasterBrokerAddrs returns all the master broker addresses
func (c *mqClient) GetMasterBrokerAddrs() []string {
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
func (c *mqClient) FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (
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
func (c *mqClient) FindAnyBrokerAddr(brokerName string) (
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

func (c *mqClient) getTopicRouteInfo(topic string) (router *route.TopicRouter, err error) {
	l := len(c.NameServerAddrs)
	for i, cc := rand.Intn(l), l; cc > 0; i, cc = i+1, cc-1 {
		addr := c.NameServerAddrs[i%l]
		router, err = getTopicRouteInfo(c.Client, addr, topic, 3*time.Second)
		if err == nil {
			return
		}

		c.logger.Errorf("request broker cluster info from %s, error:%s", addr, err)
	}
	return
}

func (c *mqClient) updateTopicRouterInfoFromNamesrv(topic string) (updated bool, err error) {
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

func (c *mqClient) isDiff(topic string, router *route.TopicRouter) bool {
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

func (c *mqClient) selectNamesrv() string {
	return c.NameServerAddrs[rand.Intn(len(c.NameServerAddrs))]
}

func (c *mqClient) SendHeartbeat() {
	hr := c.prepareHeartbeatData()

	hasConsumer, hasProducer := len(hr.Consumers) != 0, len(hr.Producers) != 0
	if !hasConsumer && !hasProducer {
		c.logger.Warn("send heartbeat, but no consumer and no producer")
		return
	}

	if c.brokerAddrs.size() == 0 {
		c.logger.Warn("send heartbeat, but broker address is empty")
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

			version, err := sendHeartbeat(c.Client, addr, hr, 3*time.Second)
			if err != nil {
				c.logger.Error("send heartbeat to " + addr + " failed, err:" + err.Error())
				if c.isBrokerAddrInRouter(addr) {
					c.logger.Error("send heartbeat to " + addr + " failed, but it is in the router")
				}
				continue
			}

			c.brokerVersions.put(name, addr, version)
			c.logger.Infof("send heartbeat to broker[%d %s %s], data:%v", id, name, addr, hr)
		}
	}
}

func (c *mqClient) prepareHeartbeatData() *HeartbeatRequest {
	ps := c.producers.coll()
	producers := make([]Producer, len(ps))
	for i := range ps {
		producers[i].Group = ps[i].Group()
	}

	cs := c.consumers.coll()
	consumers := make([]*Consumer, len(cs))
	for i := range consumers {
		c1 := cs[i]
		consumers[i] = &Consumer{
			FromWhere:    c1.ConsumeFromWhere(),
			Group:        c1.Group(),
			Model:        c1.Model(),
			Type:         c1.Type(),
			UnitMode:     c1.UnitMode(),
			Subscription: c1.Subscriptions(),
		}
	}

	return &HeartbeatRequest{
		ClientID:  c.clientID,
		Producers: producers,
		Consumers: consumers,
	}
}

func (c *mqClient) cleanOfflineBroker() {
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

func (c *mqClient) isBrokerAddrInRouter(addr string) bool {
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

func (c *mqClient) scheduleTasks() {
	c.schedule(10*time.Millisecond, c.PollNameServerInterval, c.updateTopicRoute)
	c.schedule(time.Second, c.HeartbeatBrokerInterval, func() {
		c.SendHeartbeat()
		c.cleanOfflineBroker()
	})
}

func (c *mqClient) schedule(delay, period time.Duration, f func()) {
	c.Add(1)
	go func() {
		defer c.Done()

		select {
		case <-time.After(delay):
		case <-c.exitChan:
			return
		}

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

func (c *mqClient) processRequest(ctx *net.ChannelContext, cmd *remote.Command) bool {
	switch cmd.Code {
	case remote.NotifyConsumerIdsChanged:
		for _, co := range c.consumers.coll() {
			co.ReblanceQueue()
		}
	case remote.CheckTransactionState:
	case remote.ResetConsumerClientOffset:
	case remote.GetConsumerStatusFromClient:
	case remote.GetConsumerRunningInfo:
		group := cmd.ExtFields["consumerGroup"]
		co := c.consumers.get(group)
		if co == nil {
			c.logger.Errorf("no consumer of group:%s", group)
			cmd.Code = remote.SystemError
			cmd.Remark = fmt.Sprintf("The Consumer Group <%s> not exist in this consumer", group)
		} else {
			info := co.RunningInfo()
			cmd.Body, _ = json.Marshal(info)
			cmd.Code = remote.Success
			cmd.Remark = ""
		}
		cmd, err := c.Client.RequestSync(ctx.Address, cmd, time.Second)
		c.logger.Debugf("GetConsumerRunningInfo result:%s, err:%v", cmd, err)
	case remote.ConsumeMessageDirectly:
	default:
		return false
	}
	c.logger.Debugf("process Request:%v", cmd)
	return true
}

func (c *mqClient) RemotingClient() remote.Client {
	return c.Client
}

func (c *mqClient) AdminCount() int {
	return c.admins.size()
}

func (c *mqClient) ConsumerCount() int {
	return c.consumers.size()
}

func (c *mqClient) ProducerCount() int {
	return c.producers.size()
}
