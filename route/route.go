package route

import (
	"bytes"
	"fmt"
	"sync"
)

// TopicRouter topic route information
// NOTE: donot modified directly
type TopicRouter struct {
	OrderTopicConf string              `json:"orderTopicConf"`
	Queues         []*TopicQueue       `json:"queueDatas"`
	Brokers        []*Broker           `json:"brokerDatas"`
	FilterServer   map[string][]string `json:"filterServerTable"`
}

// Equal judge equals with other topic route information
func (t *TopicRouter) Equal(o *TopicRouter) bool {
	if t.OrderTopicConf != o.OrderTopicConf {
		return false
	}

	if len(t.Queues) != len(o.Queues) {
		return false
	}

	for i, q := range t.Queues {
		if !q.Equal(o.Queues[i]) {
			return false
		}
	}

	if len(t.Brokers) != len(o.Brokers) {
		return false
	}
	for i, b := range t.Brokers {
		if !b.Equal(o.Brokers[i]) {
			return false
		}
	}

	if len(t.FilterServer) != len(o.FilterServer) {
		return false
	}
	for k, v := range t.FilterServer {
		v1, ok := o.FilterServer[k]
		if !ok {
			return false
		}
	OUT:
		for _, s := range v {
			for _, s1 := range v1 {
				if s == s1 {
					continue OUT
				}
			}
			return false
		}
	}
	return true
}

func (t *TopicRouter) String() string {
	return fmt.Sprintf("topic route:[OrderTopicConf=%s,queue=%s,broker=%s,FilterServer=%s]",
		t.OrderTopicConf, t.Queues, t.Brokers, t.FilterServer)
}

// TopicRouterTable contains routers of topic
type TopicRouterTable struct {
	sync.RWMutex
	routersOfTopic map[string]*TopicRouter // key: topic
}

// NewTopicRouterTable creates topic's router table, the operations are thread-safe
func NewTopicRouterTable() *TopicRouterTable {
	return &TopicRouterTable{routersOfTopic: make(map[string]*TopicRouter, 256)}
}

// Put stores routers of topic
func (t *TopicRouterTable) Put(topic string, router *TopicRouter) *TopicRouter {
	t.Lock()
	prev := t.routersOfTopic[topic]
	t.routersOfTopic[topic] = router
	t.Unlock()
	return prev
}

// Get get the routers of topic
func (t *TopicRouterTable) Get(topic string) *TopicRouter {
	t.RLock()
	r := t.routersOfTopic[topic]
	t.RUnlock()
	return r
}

// Topics return all the topics
func (t *TopicRouterTable) Topics() []string {
	t.RLock()
	ts := make([]string, 0, len(t.routersOfTopic))
	for topic := range t.routersOfTopic {
		ts = append(ts, topic)
	}
	t.RUnlock()
	return ts
}

// Routers return all the routers
func (t *TopicRouterTable) Routers() []*TopicRouter {
	t.RLock()
	rs := make([]*TopicRouter, 0, len(t.routersOfTopic))
	for _, r := range t.routersOfTopic {
		rs = append(rs, r)
	}
	t.RUnlock()
	return rs
}

// Delete deletes routers of topic
func (t *TopicRouterTable) Delete(topic string) *TopicRouter {
	t.Lock()
	prev := t.routersOfTopic[topic]
	delete(t.routersOfTopic, topic)
	t.Unlock()
	return prev
}

func (t *TopicRouterTable) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	buf.WriteString("TopicRouterTable:[")
	t.RLock()
	for k, v := range t.routersOfTopic {
		buf.WriteString(k)
		buf.WriteByte('=')
		buf.WriteString(v.String())
		buf.WriteByte(',')
	}
	t.RUnlock()
	buf.WriteByte(']')
	return string(buf.Bytes())
}
