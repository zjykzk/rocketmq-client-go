package producer

import (
	"testing"

	"github.com/zjykzk/rocketmq-client-go/message"

	"github.com/stretchr/testify/assert"
)

type mockTopicRouter struct {
	writeCount     int
	nextQueueIndex uint32
}

func (tr *mockTopicRouter) SelectOneQueue() *message.Queue {
	return &message.Queue{
		BrokerName: "SEL",
		Topic:      "SEL",
	}
}
func (tr *mockTopicRouter) NextQueueIndex() uint32 {
	tr.nextQueueIndex++
	return tr.nextQueueIndex
}
func (tr *mockTopicRouter) MessageQueues() []*message.Queue {
	return []*message.Queue{
		&message.Queue{
			BrokerName: "b1",
			Topic:      "b1",
			QueueID:    0,
		},
		&message.Queue{
			BrokerName: "b1",
			Topic:      "b1",
			QueueID:    1,
		},
		&message.Queue{
			BrokerName: "b2",
			Topic:      "b2",
			QueueID:    1,
		},
	}
}
func (tr *mockTopicRouter) WriteCount(broker string) int {
	tr.writeCount++
	if tr.writeCount == 1 {
		return 0
	}
	return tr.writeCount
}
func (tr *mockTopicRouter) SelectOneQueueHint(lastBroker string) *message.Queue {
	return &message.Queue{
		BrokerName: "HINT",
		Topic:      "HINT",
	}
}

func TestLatency(t *testing.T) {
	fs := NewMQFaultStrategy(true)

	fs.UpdateFault("b1", 1, false)
	assert.True(t, fs.faultLatency.Available("b1"))

	fs.UpdateFault("b1", 99, false)
	assert.True(t, fs.faultLatency.Available("b1"))

	fs.UpdateFault("b1", 49, false)
	assert.True(t, fs.faultLatency.Available("b1"))

	fs.UpdateFault("b1", 50, false)
	assert.True(t, fs.faultLatency.Available("b1"))

	fs.UpdateFault("b1", 100, false)
	assert.True(t, fs.faultLatency.Available("b1"))

	fs.sendLatencyFaultEnable = false
	fs.UpdateFault("b1", 1200, false)
	assert.True(t, fs.faultLatency.Available("b1"))

	fs.sendLatencyFaultEnable = true
	fs.UpdateFault("b1", 600, false)
	assert.False(t, fs.faultLatency.Available("b1"))
}

func TestSelectOneQueue(t *testing.T) {
	fs, tp := NewMQFaultStrategy(false), &mockTopicRouter{}

	q := fs.SelectOneQueue(tp, "b1")
	assert.Equal(t, "HINT", q.BrokerName)
	assert.Equal(t, "HINT", q.Topic)

	fs.sendLatencyFaultEnable = true
	q = fs.SelectOneQueue(tp, "b1")
	assert.Equal(t, "b2", q.BrokerName)
	assert.Equal(t, "b2", q.Topic)
	assert.Equal(t, uint8(1), q.QueueID)
	fs.UpdateFault("b1", 0, true)
	fs.UpdateFault("b2", 0, true)

	q = fs.SelectOneQueue(tp, "not exist")
	assert.Equal(t, "HINT", q.BrokerName)
	assert.Equal(t, "HINT", q.Topic)
	assert.Equal(t, 1, tp.writeCount)

	q = fs.SelectOneQueue(tp, "not exist")
	assert.True(t, q.BrokerName == "b1" || q.BrokerName == "b2")
	assert.Equal(t, "SEL", q.Topic)
	assert.Equal(t, 2, tp.writeCount)

	fs.UpdateFault("not exist", 0, false)
	q = fs.SelectOneQueue(tp, "not exist")
	assert.Equal(t, "not exist", q.BrokerName)
	assert.Equal(t, "SEL", q.Topic)
	assert.Equal(t, uint8(0), q.QueueID)
	assert.Equal(t, 3, tp.writeCount)
}
