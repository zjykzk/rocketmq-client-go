package consumer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type mockOffsetRemoteOper struct {
	runUpdate bool
	updateErr error
	updateRet int64

	fetchErr error
	fetchRet int64
}

func (m *mockOffsetRemoteOper) update(q *message.Queue, offset int64) error {
	m.runUpdate = true
	return m.updateErr
}

func (m *mockOffsetRemoteOper) fetch(q *message.Queue) (int64, error) {
	return m.fetchRet, m.fetchErr
}

func TestRemoteStore(t *testing.T) {
	mockRemoteOper := &mockOffsetRemoteOper{}
	// new
	rs, err := newRemoteStore(remoteStoreConfig{})
	assert.NotNil(t, err)
	rs, err = newRemoteStore(remoteStoreConfig{
		offsetOper: mockRemoteOper,
	})
	assert.NotNil(t, err)
	rs, err = newRemoteStore(remoteStoreConfig{
		offsetOper: mockRemoteOper,
		logger:     log.Std,
	})
	assert.Nil(t, err)

	q := &message.Queue{}
	// read from store
	mockRemoteOper.fetchRet = 10
	of, err := rs.readOffsetFromStore(q)
	assert.Equal(t, int64(10), of)
	assert.Equal(t, mockRemoteOper.fetchErr, err)

	rs.updateOffset(q, 0)

	// persistOne
	rs.persistOne(&message.Queue{QueueID: 2})
	assert.False(t, mockRemoteOper.runUpdate)
	rs.persistOne(q)
	assert.True(t, mockRemoteOper.runUpdate)
	mockRemoteOper.updateErr = errors.New("bad update")
	rs.persistOne(q)
	assert.True(t, mockRemoteOper.runUpdate)

	q1 := &message.Queue{BrokerName: "b1", QueueID: 1}
	// persist and clear
	rs.updateOffset(q1, 1)
	mockRemoteOper.runUpdate = false
	rs.updateQueues(q1)
	assert.True(t, mockRemoteOper.runUpdate)

	_, ok := rs.readOffsetFromMemory(q)
	assert.False(t, ok)
}
