package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestPutMessages(t *testing.T) {
	pq := newProcessQueue()
	pq.putMessages([]*message.MessageExt{{}, {QueueOffset: 1}, {QueueOffset: 1}})
	assert.Equal(t, 0, pq.msgSize)
	assert.Equal(t, 2, pq.msgCount)
}

func TestPullExpired(t *testing.T) {
	pq := &processQueue{}
	pq.updatePullTime(time.Now())
	assert.False(t, pq.isPullExpired(time.Second))
	assert.True(t, pq.isPullExpired(time.Nanosecond))
}
