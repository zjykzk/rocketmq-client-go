package consumer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeOffsetRemoteOper struct {
	runUpdate bool
	updateErr error
	updateRet int64

	fetchErr error
	fetchRet int64
}

func (m *fakeOffsetRemoteOper) update(q *message.Queue, offset int64) error {
	m.runUpdate = true
	return m.updateErr
}

func (m *fakeOffsetRemoteOper) fetch(q *message.Queue) (int64, error) {
	return m.fetchRet, m.fetchErr
}

func TestRemoteStore(t *testing.T) {
	fakeRemoteOper := &fakeOffsetRemoteOper{}
	// new
	rs, err := newRemoteStore(remoteStoreConfig{})
	assert.NotNil(t, err)
	rs, err = newRemoteStore(remoteStoreConfig{
		offsetOper: fakeRemoteOper,
	})
	assert.NotNil(t, err)
	rs, err = newRemoteStore(remoteStoreConfig{
		offsetOper: fakeRemoteOper,
		logger:     log.Std,
	})
	assert.Nil(t, err)

	q := &message.Queue{}
	// read from store
	fakeRemoteOper.fetchRet = 10
	of, err := rs.readOffsetFromStore(q)
	assert.Equal(t, int64(10), of)
	assert.Equal(t, fakeRemoteOper.fetchErr, err)

	// update
	newQueue := &message.Queue{QueueID: 200, Topic: "new "}
	rs.updateOffset(newQueue, 2)
	of, err = rs.readOffset(newQueue, ReadOffsetFromMemory)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), of)

	// persistOne
	rs.persistOne(&message.Queue{QueueID: 2})
	assert.False(t, fakeRemoteOper.runUpdate)

	rs.updateOffset(q, 2)
	rs.persistOne(q)
	assert.True(t, fakeRemoteOper.runUpdate)
	fakeRemoteOper.updateErr = errors.New("bad update")
	rs.persistOne(q)
	assert.True(t, fakeRemoteOper.runUpdate)

	q1 := &message.Queue{BrokerName: "b1", QueueID: 1}
	// persist and clear
	rs.updateOffset(q1, 1)
	fakeRemoteOper.runUpdate = false
	rs.updateQueues(q1)
	assert.True(t, fakeRemoteOper.runUpdate)

	_, ok := rs.readOffsetFromMemory(q)
	assert.False(t, ok)
}
