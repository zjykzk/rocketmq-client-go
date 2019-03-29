package consumer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/zjykzk/rocketmq-client-go/message"
)

var (
	errOffsetNotExist = errors.New("offset not exist")
)

// read offset type
const (
	ReadOffsetFromMemory = iota
	ReadOffsetFromStore
	ReadOffsetMemoryFirstThenStore
)

type offsets struct {
	sync.RWMutex `json:"-"`
	Broker       string `json:"broker"`
	// index is the queue id, and map contains (topic, offset) paire
	// the offset is begin point to be pulled
	OffsetOfTopic [256]map[string]int64 `json:"offset_of_topic"`
}

func newOffsets(broker string) *offsets {
	return &offsets{Broker: broker}
}

func (of *offsets) updateIfGreater(q *message.Queue, offset int64) {
	of.Lock()
	m := of.getOrNew(q)
	if m[q.Topic] < offset {
		m[q.Topic] = offset
	}
	of.Unlock()
}

func (of *offsets) update(q *message.Queue, offset int64) {
	of.Lock()
	m := of.getOrNew(q)
	m[q.Topic] = offset
	of.Unlock()
}

func (of *offsets) getOrNew(q *message.Queue) map[string]int64 {
	m := of.OffsetOfTopic[q.QueueID]
	if m == nil {
		m = map[string]int64{}
		of.OffsetOfTopic[q.QueueID] = m
	}

	return m
}

func (of *offsets) remove(q *message.Queue) (offset int64, ok bool) {
	of.Lock()
	m := of.OffsetOfTopic[q.QueueID]
	if m != nil {
		offset, ok = m[q.Topic]
		delete(m, q.Topic)
	}
	of.Unlock()
	return
}

func (of *offsets) read(q *message.Queue) (offset int64, ok bool) {
	of.RLock()
	m := of.OffsetOfTopic[q.QueueID]
	if m != nil {
		offset, ok = m[q.Topic]
	}
	of.RUnlock()
	return
}

func (of *offsets) String() string {
	s := "offsets["
	of.RLock()
	for queueID, o := range of.OffsetOfTopic {
		if o == nil {
			continue
		}
		for topic, offset := range o {
			s += fmt.Sprintf("broker:%s,topic:%s,queueID:%d,offset:%d", of.Broker, topic, queueID, offset)
		}
	}
	of.RUnlock()
	return s + "]"
}

func (of *offsets) queuesAndOffets() (queues []message.Queue, offsets []int64) {
	of.RLock()
	for i, m := range of.OffsetOfTopic {
		for t, o := range m {
			queues = append(queues, message.Queue{BrokerName: of.Broker, Topic: t, QueueID: uint8(i)})
			offsets = append(offsets, o)
		}
	}
	of.RUnlock()
	return
}

type baseStore struct {
	sync.RWMutex

	Offsets []*offsets

	readOffsetFromStore func(*message.Queue) (int64, error)
}

func (bs *baseStore) updateOffset0(q *message.Queue, offset int64, ifGreater bool) {
	bs.RLock()
	of, found := bs.findOffsets(q.BrokerName)
	bs.RUnlock()

	if found {
		goto UPDATE
	}

	of = newOffsets(q.BrokerName)
	of.OffsetOfTopic[q.QueueID] = map[string]int64{q.Topic: offset}

	bs.Lock()
	_, found = bs.findOffsets(q.BrokerName)
	if !found {
		bs.Offsets = append(bs.Offsets, of)
	}
	bs.Unlock()

	if !found {
		return
	}

UPDATE:
	if ifGreater {
		of.updateIfGreater(q, offset)
	} else {
		of.update(q, offset)
	}
}

func (bs *baseStore) findOffsets(broker string) (of *offsets, ok bool) {
	for _, o := range bs.Offsets {
		if o.Broker == broker {
			of, ok = o, true
			break
		}
	}
	return
}

func (bs *baseStore) readOffsetFromMemory(q *message.Queue) (int64, bool) {
	bs.RLock()
	of, ok := bs.findOffsets(q.BrokerName)
	bs.RUnlock()
	if !ok {
		return 0, false
	}
	return of.read(q)
}

func readOffset(ofs []*offsets, q *message.Queue) (int64, bool) {
	for _, of := range ofs {
		if of.Broker != q.BrokerName {
			continue
		}

		r, ok := of.read(q)
		return r, ok
	}

	return 0, false
}

func (bs *baseStore) queuesAndOffsets() (queues []message.Queue, offsets []int64) {
	bs.RLock()
	ofs := bs.Offsets
	bs.RUnlock()
	for _, of := range ofs {
		qs, os := of.queuesAndOffets()
		queues = append(queues, qs...)
		offsets = append(offsets, os...)
	}
	return
}

func (bs *baseStore) updateOffset(q *message.Queue, offset int64) {
	bs.updateOffset0(q, offset, false)
}

func (bs *baseStore) updateOffsetIfGreater(q *message.Queue, offset int64) {
	bs.updateOffset0(q, offset, true)
}

// readOffset returns the offset of the queue
// return -1 if the queue is not exist
func (bs *baseStore) readOffset(q *message.Queue, readOffsetType int) (int64, error) {
	switch readOffsetType {
	case ReadOffsetMemoryFirstThenStore:
		of, ok := bs.readOffsetFromMemory(q)
		if ok {
			return of, nil
		}
		return bs.readOffsetFromStore(q)
	case ReadOffsetFromMemory:
		of, ok := bs.readOffsetFromMemory(q)
		if !ok {
			return -1, nil
		}
		return of, nil
	case ReadOffsetFromStore:
		return bs.readOffsetFromStore(q)
	default:
		return 0, fmt.Errorf("bad read offset type:%d", readOffsetType)
	}
}

func (bs *baseStore) removeOffset(q *message.Queue) (int64, bool) {
	var offsets *offsets
	bs.RLock()
	for _, of := range bs.Offsets {
		if of.Broker == q.BrokerName {
			offsets = of
			break
		}
	}
	bs.RUnlock()

	if offsets == nil {
		return 0, false
	}
	return offsets.remove(q)
}
