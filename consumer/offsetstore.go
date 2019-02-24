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

	Broker        string                `json:"broker"`          // readlony
	OffsetOfTopic [256]map[string]int64 `json:"offset_of_topic"` // index is the queue id, and map contains (topic, offset) paire
}

func newOffsets(broker string) *offsets {
	return &offsets{Broker: broker}
}

func (of *offsets) updateIfGreater(q *message.Queue, offset int64) {
	of.Lock()
	o, ok := of.OffsetOfTopic[q.QueueID][q.Topic]
	if ok && o < offset {
		of.OffsetOfTopic[q.QueueID][q.Topic] = offset
	}
	of.Unlock()
}

func (of *offsets) update(q *message.Queue, offset int64) {
	of.Lock()
	_, ok := of.OffsetOfTopic[q.QueueID][q.Topic]
	if ok {
		of.OffsetOfTopic[q.QueueID][q.Topic] = offset
	}
	of.Unlock()
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
	var of *offsets
	bs.RLock()
	for _, o := range bs.Offsets {
		if o.Broker == q.BrokerName {
			of = o
			break
		}
	}
	bs.RUnlock()

	if of != nil {
		if ifGreater {
			of.updateIfGreater(q, offset)
		} else {
			of.update(q, offset)
		}
		return
	}

	of = newOffsets(q.BrokerName)
	of.OffsetOfTopic[q.QueueID] = map[string]int64{q.Topic: offset}

	var found bool
	bs.Lock()
	for _, o := range bs.Offsets {
		if o.Broker == q.BrokerName {
			of = o
			found = true
			break
		}
	}
	if !found {
		bs.Offsets = append(bs.Offsets, of)
	}
	bs.Unlock()

	if !found {
		return
	}

	if ifGreater {
		of.updateIfGreater(q, offset)
	} else {
		of.update(q, offset)
	}
}

func (bs *baseStore) readOffsetFromMemory(q *message.Queue) (int64, bool) {
	bs.RLock()
	of, ok := readOffset(bs.Offsets, q)
	bs.RUnlock()
	return of, ok
}

func readOffset(ofs []*offsets, q *message.Queue) (int64, bool) {
	for _, of := range ofs {
		if of.Broker != q.BrokerName {
			continue
		}

		r, ok := of.OffsetOfTopic[q.QueueID][q.Topic]
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
		}
	}
	bs.RUnlock()

	if offsets == nil {
		return 0, false
	}
	return offsets.remove(q)
}
