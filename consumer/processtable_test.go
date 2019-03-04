package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestPutMessages(t *testing.T) {
	pq := newProcessQueue()
	pq.putMessages([]*message.Ext{{}, {QueueOffset: 1}, {QueueOffset: 1}})
	assert.Equal(t, int64(0), pq.msgSize)
	assert.Equal(t, int32(2), pq.msgCount)
}

func TestPullExpired(t *testing.T) {
	pq := &processQueue{}
	pq.updatePullTime(time.Now())
	assert.False(t, pq.isPullExpired(time.Second))
	assert.True(t, pq.isPullExpired(time.Nanosecond))
}
