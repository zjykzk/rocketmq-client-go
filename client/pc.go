package client

import (
	"sync"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// Producer interface needed by reblance
type Producer interface {
	Group() string
	PublishTopics() []string
	UpdateTopicPublish(topic string, router *route.TopicRouter)
	NeedUpdateTopicPublish(topic string) bool
}

type producerColl struct {
	sync.RWMutex
	producers map[string]Producer // key: group name, NOTE donot modify directly
}

func (pc *producerColl) coll() []Producer {
	pc.RLock()
	coll, i := make([]Producer, len(pc.producers)), 0
	for _, p := range pc.producers {
		coll[i] = p
		i++
	}
	pc.RUnlock()
	return coll
}

func (pc *producerColl) putIfAbsent(group string, p Producer) (prev Producer, suc bool) {
	pc.Lock()
	prev, exist := pc.producers[group]
	if !exist {
		pc.producers[group] = p
		suc = true
	}
	pc.Unlock()
	return
}

func (pc *producerColl) contains(group string) bool {
	pc.RLock()
	_, b := pc.producers[group]
	pc.RUnlock()
	return b
}

func (pc *producerColl) delete(group string) {
	pc.Lock()
	delete(pc.producers, group)
	pc.Unlock()
}

func (pc *producerColl) size() int {
	pc.RLock()
	sz := len(pc.producers)
	pc.RUnlock()
	return sz
}

// RunningInfo consumer running information
type RunningInfo struct {
	Properties    map[string]string `json:"properties"`
	Subscriptions []*SubscribeData  `json:"subscriptionSet"`
	// MQTable map[string]*ProcessQueueInfo TODO
	// Statuses map[string]ConsumerStatus TODO
}

// Consumer interface needed by reblance
type Consumer interface {
	Group() string
	SubscribeTopics() []string
	UpdateTopicSubscribe(topic string, router *route.TopicRouter)
	NeedUpdateTopicSubscribe(topic string) bool
	ConsumeFromWhere() string
	Model() string
	Type() string
	UnitMode() bool
	Subscriptions() []*SubscribeData
	ReblanceQueue()
	RunningInfo() RunningInfo
	ResetOffset(topic string, offsets map[message.Queue]int64) error
	ConsumeMessageDirectly(msg *message.Ext, broker string) (ConsumeMessageDirectlyResult, error)
}

type consumerColl struct {
	sync.RWMutex
	eles map[string]Consumer // key: group name, NOTE: donot modify directly
}

func (cc *consumerColl) coll() []Consumer {
	cc.RLock()
	coll, i := make([]Consumer, len(cc.eles)), 0
	for _, c := range cc.eles {
		coll[i] = c
		i++
	}
	cc.RUnlock()
	return coll
}

func (cc *consumerColl) putIfAbsent(group string, c Consumer) (prev Consumer, suc bool) {
	cc.Lock()
	prev, exist := cc.eles[group]
	if !exist {
		cc.eles[group] = c
		suc = true
	}
	cc.Unlock()
	return
}

func (cc *consumerColl) contains(group string) bool {
	cc.RLock()
	_, b := cc.eles[group]
	cc.RUnlock()
	return b
}

func (cc *consumerColl) get(group string) Consumer {
	cc.RLock()
	c := cc.eles[group]
	cc.RUnlock()
	return c
}

func (cc *consumerColl) delete(group string) {
	cc.Lock()
	delete(cc.eles, group)
	cc.Unlock()
}

func (cc *consumerColl) size() int {
	cc.RLock()
	sz := len(cc.eles)
	cc.RUnlock()
	return sz
}

// Admin admin operation
type Admin interface {
	Group() string
}

type adminColl struct {
	sync.RWMutex
	eles map[string]Admin // key: group name, NOTE: donot modify directly
}

func (ac *adminColl) putIfAbsent(group string, c Admin) (prev Admin, suc bool) {
	ac.Lock()
	prev, exist := ac.eles[group]
	if !exist {
		ac.eles[group] = c
		suc = true
	}
	ac.Unlock()
	return
}

func (ac *adminColl) delete(group string) {
	ac.Lock()
	delete(ac.eles, group)
	ac.Unlock()
}

func (ac *adminColl) size() int {
	ac.RLock()
	sz := len(ac.eles)
	ac.RUnlock()
	return sz
}
