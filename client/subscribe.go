package client

import (
	"fmt"
	"sync"

	"github.com/zjykzk/rocketmq-client-go/message"
)

// SubscribeQueueTable contains the message queues of topic, the operations is thread-safe
type SubscribeQueueTable struct {
	locker        sync.RWMutex
	queuesOfTopic map[string][]*message.Queue // key: topic
}

// NewSubscribeQueueTable creates the QueueTable
func NewSubscribeQueueTable() *SubscribeQueueTable {
	return &SubscribeQueueTable{
		queuesOfTopic: make(map[string][]*message.Queue, 128),
	}
}

// Put stores the queues and returns the previous queue
func (t *SubscribeQueueTable) Put(topic string, q []*message.Queue) []*message.Queue {
	t.locker.Lock()
	prev := t.queuesOfTopic[topic]
	t.queuesOfTopic[topic] = q
	t.locker.Unlock()
	return prev
}

// Get returns the queues of the topic
func (t *SubscribeQueueTable) Get(topic string) []*message.Queue {
	t.locker.RLock()
	queues := t.queuesOfTopic[topic]
	t.locker.RUnlock()
	return queues
}

// Topics returns the topics
func (t *SubscribeQueueTable) Topics() []string {
	i := 0
	t.locker.RLock()
	topics := make([]string, len(t.queuesOfTopic))
	for k := range t.queuesOfTopic {
		topics[i] = k
		i++
	}
	t.locker.RUnlock()
	return topics
}

// Delete returns the topics
func (t *SubscribeQueueTable) Delete(topic string) []*message.Queue {
	t.locker.Lock()
	prev, ok := t.queuesOfTopic[topic]
	if ok {
		delete(t.queuesOfTopic, topic)
	}
	t.locker.Unlock()
	return prev
}

// SubscribeData subscription information
type SubscribeData struct {
	Topic             string   `json:"topic"`
	Expr              string   `json:"subString"`
	Type              string   `json:"expressionType"`
	Tags              []string `json:"tagsSet"`
	Codes             []uint32 `json:"codeSet"`
	Version           int64    `json:"subVersion"`
	IsClassFilterMode bool     `json:"classFilterMode"`
	FilterClassSource string   `json:"-"`
}

// Equal returns true if equals another, otherwise false
func (s *SubscribeData) Equal(o *SubscribeData) bool {
	if s.Topic != o.Topic {
		return false
	}

	if s.Expr != o.Expr {
		return false
	}

	if s.Version != o.Version {
		return false
	}

	if s.Type != o.Type {
		return false
	}

	if len(s.Tags) != len(o.Tags) {
		return false
	}

	if len(s.Codes) != len(o.Codes) {
		return false
	}

	for i := range s.Tags {
		if s.Tags[i] != o.Tags[i] {
			return false
		}
	}

	for i := range s.Codes {
		if s.Codes[i] != o.Codes[i] {
			return false
		}
	}

	if s.FilterClassSource != o.FilterClassSource {
		return false
	}

	if s.IsClassFilterMode != s.IsClassFilterMode {
		return false
	}

	return true
}

func (s *SubscribeData) String() string {
	return fmt.Sprintf(
		"SubscribeData [topic=%s,expr=%s,type=%s,tags=%v,codes=%v,version=%d,isclass=%t,classsource=%s]",
		s.Topic, s.Expr, s.Type, s.Tags, s.Codes, s.Version, s.IsClassFilterMode, s.FilterClassSource,
	)
}

// SubscribeDataTable contains the subscription information of topic, the operations is thread-safe
// NOTE: donot modify directly
type SubscribeDataTable struct {
	locker     sync.RWMutex
	subOfTopic map[string]*SubscribeData // key: topic
}

// NewSubcribeTable creates one DataTable
func NewSubcribeTable() *SubscribeDataTable {
	return &SubscribeDataTable{
		subOfTopic: make(map[string]*SubscribeData, 8),
	}
}

// Put stores the subcribe data and returns the previous one
func (t *SubscribeDataTable) Put(topic string, d *SubscribeData) *SubscribeData {
	t.locker.Lock()
	prev := t.subOfTopic[topic]
	t.subOfTopic[topic] = d
	t.locker.Unlock()
	return prev
}

// PutIfAbsent stores the subcribe data and returns the previous one
func (t *SubscribeDataTable) PutIfAbsent(topic string, d *SubscribeData) *SubscribeData {
	t.locker.Lock()
	prev, ok := t.subOfTopic[topic]
	if !ok {
		t.subOfTopic[topic] = d
	}
	t.locker.Unlock()
	return prev
}

// Get returns the subcribe data of the topic
func (t *SubscribeDataTable) Get(topic string) *SubscribeData {
	t.locker.RLock()
	d := t.subOfTopic[topic]
	t.locker.RUnlock()
	return d
}

// Topics returns the topics
func (t *SubscribeDataTable) Topics() []string {
	i := 0
	t.locker.RLock()
	topics := make([]string, len(t.subOfTopic))
	for k := range t.subOfTopic {
		topics[i] = k
		i++
	}
	t.locker.RUnlock()
	return topics
}

// Datas returns the subscribed datas
func (t *SubscribeDataTable) Datas() []*SubscribeData {
	i := 0
	t.locker.RLock()
	datas := make([]*SubscribeData, len(t.subOfTopic))
	for _, d := range t.subOfTopic {
		datas[i] = d
		i++
	}
	t.locker.RUnlock()
	return datas
}

// Delete deletes the data of the specified topic, and return the previous one
func (t *SubscribeDataTable) Delete(topic string) *SubscribeData {
	t.locker.Lock()
	prev, ok := t.subOfTopic[topic]
	if ok {
		delete(t.subOfTopic, topic)
	}
	t.locker.Unlock()
	return prev
}
