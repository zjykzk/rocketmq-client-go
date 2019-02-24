package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	q := &Queue{Topic: "topic", BrokerName: "name", QueueID: 3}
	assert.Equal(t, "name@3@topic", q.HashKey())

	qs := []*Queue{
		&Queue{
			Topic:      "3",
			BrokerName: "b",
			QueueID:    0,
		},
		&Queue{
			Topic:      "2",
			BrokerName: "b1",
			QueueID:    1,
		},
		&Queue{
			Topic:      "2",
			BrokerName: "b1",
			QueueID:    0,
		},
		&Queue{
			Topic:      "2",
			BrokerName: "b",
			QueueID:    0,
		},
	}

	SortQueue(qs)
	q = qs[0]
	assert.Equal(t, "2", q.Topic)
	assert.Equal(t, "b", q.BrokerName)
	assert.Equal(t, uint8(0), q.QueueID)
	q = qs[1]
	assert.Equal(t, "2", q.Topic)
	assert.Equal(t, "b1", q.BrokerName)
	assert.Equal(t, uint8(0), q.QueueID)
	q = qs[2]
	assert.Equal(t, "2", q.Topic)
	assert.Equal(t, "b1", q.BrokerName)
	assert.Equal(t, uint8(1), q.QueueID)
	q = qs[3]
	assert.Equal(t, "3", q.Topic)
	assert.Equal(t, "b", q.BrokerName)
	assert.Equal(t, uint8(0), q.QueueID)
}
