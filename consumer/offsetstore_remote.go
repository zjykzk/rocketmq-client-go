package consumer

import (
	"errors"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type offsetRemoteOper interface {
	// update the offset the queue to the remote store
	update(q *message.Queue, offset int64) error
	// fetch the offset of the queue from the remote store
	fetch(q *message.Queue) (offset int64, err error)
}

type remoteStore struct {
	*baseStore
	offsetOper offsetRemoteOper

	logger log.Logger
}

type remoteStoreConfig struct {
	offsetOper offsetRemoteOper
	logger     log.Logger
}

func newRemoteStore(conf remoteStoreConfig) (*remoteStore, error) {
	if conf.offsetOper == nil {
		return nil, errors.New("empty offset operation")
	}
	if conf.logger == nil {
		return nil, errors.New("empty logger")
	}

	rs := &remoteStore{baseStore: &baseStore{}, offsetOper: conf.offsetOper, logger: conf.logger}
	rs.baseStore.readOffsetFromStore = rs.readOffsetFromStore
	return rs, nil
}

func (rs *remoteStore) readOffsetFromStore(q *message.Queue) (int64, error) {
	return rs.offsetOper.fetch(q)
}

func (rs *remoteStore) persist() error {
	curQueues, _ := rs.queuesAndOffsets()
	for _, q := range curQueues {
		rs.persistOne(&q)
	}
	return nil
}

func (rs *remoteStore) persistOne(q *message.Queue) {
	of, ok := rs.readOffsetFromMemory(q)
	if !ok {
		return
	}

	err := rs.offsetOper.update(q, of)
	rs.logger.Infof("[persist] persist queue:%s, error:%v", q, err)
}

// updateQueues persists the offset to the remote
// clear the queues not contained in the specified queue
func (rs *remoteStore) updateQueues(qs ...*message.Queue) {
	for i := range qs {
		rs.persistOne(qs[i])
	}

	curQueues, _ := rs.queuesAndOffsets()
CMP:
	for i := range curQueues {
		q := &curQueues[i]
		for _, q1 := range qs {
			if *q == *q1 {
				continue CMP
			}
		}
		rs.removeOffset(q)
		rs.logger.Infof("remove offset %s", q)
	}
}
