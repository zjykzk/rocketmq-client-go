package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestPutMessages(t *testing.T) {
	pq := newProcessQueue()
	pq.putMessages([]*message.Ext{{}, {QueueOffset: 1}, {QueueOffset: 1}})
	assert.Equal(t, int64(0), pq.msgSize)
	assert.Equal(t, int32(2), pq.msgCount)
}
