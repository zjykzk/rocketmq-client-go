package consumer

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zjykzk/rocketmq-client-go/consumer/internel/tree"
	"github.com/zjykzk/rocketmq-client-go/message"
)

const (
	normal = iota
	dropped
)

type offset int64

func (o offset) CompareTo(o1 tree.Key) int {
	if o > o1.(offset) {
		return 1
	}
	if o < o1.(offset) {
		return -1
	}
	return 0
}

type processQueue struct {
	sync.RWMutex

	dropped         int32
	msgCount        int32
	msgSize         int64
	nextQueueOffset int64
	messages        tree.LLRBTree // queue offset -> message

	lastPullTime int64 // unixnano
}

func newProcessQueue() *processQueue {
	return &processQueue{}
}

func (pq *processQueue) putMessages(msgs []*message.Ext) {
	newCount := 0
	pq.Lock()
	for _, m := range msgs {
		old := pq.messages.Put(offset(m.QueueOffset), m)
		if old == nil {
			newCount++
			pq.nextQueueOffset = m.QueueOffset + 1
			atomic.AddInt64(&pq.msgSize, int64(len(m.Body)))
		}
	}
	atomic.AddInt32(&pq.msgCount, int32(newCount))
	pq.Unlock()
}

func (pq *processQueue) removeMessages(msgs []*message.Ext) {
	removedCount := 0
	pq.Lock()
	for _, m := range msgs {
		if old := pq.messages.Remove(offset(m.QueueOffset)); old != nil {
			atomic.AddInt64(&pq.msgSize, -int64(len(m.Body)))
			removedCount++
		}
	}
	atomic.AddInt32(&pq.msgCount, -int32(removedCount))
	pq.Unlock()

	return
}

func (pq *processQueue) queueOffsetToConsume() (of int64) {
	pq.RLock()
	if pq.messages.Size() > 0 {
		k, _ := pq.messages.First()
		of = int64(k.(offset))
	} else {
		of = pq.nextQueueOffset
	}
	pq.RUnlock()
	return
}

func (pq *processQueue) drop() bool {
	return atomic.CompareAndSwapInt32(&pq.dropped, normal, dropped)
}

func (pq *processQueue) isDropped() bool {
	return atomic.LoadInt32(&pq.dropped) == dropped
}

func (pq *processQueue) updatePullTime(t time.Time) {
	atomic.StoreInt64(&pq.lastPullTime, t.UnixNano())
}

func getConsumeStartTime(m *message.Ext) int64 {
	v := m.GetProperty(message.PropertyConsumeStartTimestamp)
	i, _ := strconv.ParseInt(v, 10, 64)
	return i
}

func (pq *processQueue) messageCount() int32 {
	return atomic.LoadInt32(&pq.msgCount)
}

func (pq *processQueue) messageSize() int64 {
	return atomic.LoadInt64(&pq.msgSize)
}

func (pq *processQueue) offsetRange() (min, max int64) {
	pq.RLock()
	if pq.messages.Size() > 0 {
		minK, _ := pq.messages.First()
		maxK, _ := pq.messages.Last()
		min, max = int64(minK.(offset)), int64(maxK.(offset))
	}
	pq.RUnlock()

	return
}
