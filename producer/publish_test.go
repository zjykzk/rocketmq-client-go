package producer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestSelectOneQueueNotOf(t *testing.T) {
	p := &topicPublishInfo{
		queues: []*message.Queue{{BrokerName: "a"}, {BrokerName: "a"}},
	}

	// empty broker
	q := p.SelectOneQueueNotOf("")
	assert.Equal(t, p.queues[1], q)

	// no selectable and random one
	q = p.SelectOneQueueNotOf("a")
	assert.Equal(t, p.queues[p.nextQueueIndex%uint32(len(p.queues))], q)

	// got the one not equal the broker
	i := p.nextQueueIndex
	q = p.SelectOneQueueNotOf("b")
	assert.Equal(t, p.queues[i%uint32(len(p.queues))], q)
}
