package consumer

import (
	"fmt"
	"sync"

	"github.com/zjykzk/rocketmq-client-go/message"
)

// QueueTable contains the message queues of topic, the operations is thread-safe
type QueueTable struct {
	locker sync.RWMutex
	table  map[string][]*message.Queue // key: topic
}

// NewQueueTable creates the QueueTable
func NewQueueTable() *QueueTable {
	return &QueueTable{
		table: make(map[string][]*message.Queue, 128),
	}
}

// Put stores the queues and returns the previous queue
func (t *QueueTable) Put(topic string, q []*message.Queue) []*message.Queue {
	t.locker.Lock()
	prev := t.table[topic]
	t.table[topic] = q
	t.locker.Unlock()
	return prev
}

// Get returns the queues of the topic
func (t *QueueTable) Get(topic string) []*message.Queue {
	t.locker.RLock()
	queues := t.table[topic]
	t.locker.RUnlock()
	return queues
}

// Topics returns the topics
func (t *QueueTable) Topics() []string {
	i := 0
	t.locker.RLock()
	topics := make([]string, len(t.table))
	for k := range t.table {
		topics[i] = k
		i++
	}
	t.locker.RUnlock()
	return topics
}

// Delete returns the topics
func (t *QueueTable) Delete(topic string) []*message.Queue {
	t.locker.Lock()
	prev, ok := t.table[topic]
	if ok {
		delete(t.table, topic)
	}
	t.locker.Unlock()
	return prev
}

// Data subscription information
type Data struct {
	Topic   string   `json:"topic"`
	Expr    string   `json:"subString"`
	Typ     string   `json:"expressionType"`
	Tags    []string `json:"tagsSet"`
	Codes   []uint32 `json:"codeSet"`
	Version int64    `json:"subVersion"`
}

// Equal returns true if equals another, otherwise false
func (s *Data) Equal(o *Data) bool {
	if s.Topic != o.Topic {
		return false
	}

	if s.Expr != o.Expr {
		return false
	}

	if s.Version != o.Version {
		return false
	}

	if s.Typ != o.Typ {
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

	return true
}

func (s *Data) String() string {
	return fmt.Sprintf("SubscribeData [topic=%s,expr=%s,type=%s,tags=%v,codes=%v,version=%d]",
		s.Topic, s.Expr, s.Typ, s.Tags, s.Codes, s.Version)
}

// DataTable contains the subscription information of topic, the operations is thread-safe
// NOTE: donot modify directly
type DataTable struct {
	locker sync.RWMutex
	table  map[string]*Data // key: topic
}

// NewDataTable creates one DataTable
func NewDataTable() *DataTable {
	return &DataTable{
		table: make(map[string]*Data, 8),
	}
}

// Put stores the subcribe data and returns the previous one
func (t *DataTable) Put(topic string, d *Data) *Data {
	t.locker.Lock()
	prev := t.table[topic]
	t.table[topic] = d
	t.locker.Unlock()
	return prev
}

// PutIfAbsent stores the subcribe data and returns the previous one
func (t *DataTable) PutIfAbsent(topic string, d *Data) *Data {
	t.locker.Lock()
	prev, ok := t.table[topic]
	if !ok {
		t.table[topic] = d
	}
	t.locker.Unlock()
	return prev
}

// Get returns the subcribe data of the topic
func (t *DataTable) Get(topic string) *Data {
	t.locker.RLock()
	d := t.table[topic]
	t.locker.RUnlock()
	return d
}

// Topics returns the topics
func (t *DataTable) Topics() []string {
	i := 0
	t.locker.RLock()
	topics := make([]string, len(t.table))
	for k := range t.table {
		topics[i] = k
		i++
	}
	t.locker.RUnlock()
	return topics
}

// Datas returns the subscribed datas
func (t *DataTable) Datas() []*Data {
	i := 0
	t.locker.RLock()
	datas := make([]*Data, len(t.table))
	for _, d := range t.table {
		datas[i] = d
		i++
	}
	t.locker.RUnlock()
	return datas
}

// Delete deletes the data of the specified topic, and return the previous one
func (t *DataTable) Delete(topic string) *Data {
	t.locker.Lock()
	prev, ok := t.table[topic]
	if ok {
		delete(t.table, topic)
	}
	t.locker.Unlock()
	return prev
}
