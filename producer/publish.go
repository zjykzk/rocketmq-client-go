package producer

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// NOTE: donot modify directly
type topicPublishInfo struct {
	orderTopic          bool
	haveTopicRouterInfo bool // FIXME: unknow the usage of this field, now it is not used to judge whether the topic has router
	queues              []*message.Queue
	router              *route.TopicRouter
	nextQueueIndex      uint32
}

func (p *topicPublishInfo) String() string {
	return fmt.Sprintf("TopicPublishInfo [orderTopic="+strconv.FormatBool(p.orderTopic)+
		", messageQueueList=%v, sendWhichQueue=%v, haveTopicRouterInfo="+
		strconv.FormatBool(p.haveTopicRouterInfo)+"]", p.queues, p.router)
}

func (p *topicPublishInfo) SelectOneQueue() *message.Queue {
	return p.queues[atomic.AddUint32(&p.nextQueueIndex, 1)%uint32(len(p.queues))]
}

func (p *topicPublishInfo) NextQueueIndex() uint32 {
	return atomic.AddUint32(&p.nextQueueIndex, 1)
}

func (p *topicPublishInfo) MessageQueues() []*message.Queue {
	return p.queues
}

func (p *topicPublishInfo) WriteCount(broker string) int {
	for _, q := range p.router.Queues {
		if q.BrokerName == broker {
			return q.WriteCount
		}
	}
	return -1
}

// SelectOneQueueHint select the broker whose name is not the excludeBroker
// if not found,  select one randomly
func (p *topicPublishInfo) SelectOneQueueHint(excludeBroker string) *message.Queue {
	if excludeBroker == "" {
		return p.SelectOneQueue()
	}

	n, c := atomic.AddUint32(&p.nextQueueIndex, 1), len(p.queues)
	for range p.queues {
		q := p.queues[n%uint32(c)]
		n++

		if q.BrokerName == excludeBroker {
			continue
		}

		return q
	}
	return p.SelectOneQueue()
}

func (p *topicPublishInfo) hasQueue() bool {
	return len(p.queues) > 0
}

type topicPublishInfoTable struct {
	sync.RWMutex
	table map[string]*topicPublishInfo // key: topic, NOTE: donot modify directly
}

func (t *topicPublishInfoTable) putIfAbsent(topic string, p *topicPublishInfo) *topicPublishInfo {
	t.Lock()
	old, ok := t.table[topic]
	if !ok {
		t.table[topic] = p
	}
	t.Unlock()
	return old
}

func (t *topicPublishInfoTable) put(topic string, p *topicPublishInfo) *topicPublishInfo {
	t.Lock()
	old := t.table[topic]
	t.table[topic] = p
	t.Unlock()
	return old
}

func (t *topicPublishInfoTable) get(topic string) *topicPublishInfo {
	t.RLock()
	p := t.table[topic]
	t.RUnlock()
	return p
}

func (t *topicPublishInfoTable) topics() []string {
	t.RLock()
	ts, i := make([]string, len(t.table)), 0
	for k := range t.table {
		ts[i] = k
		i++
	}
	t.RUnlock()
	return ts
}

func (t *topicPublishInfoTable) delete(topic string) bool {
	t.Lock()
	_, ok := t.table[topic]
	if ok {
		delete(t.table, topic)
	}
	t.Unlock()
	return ok
}
