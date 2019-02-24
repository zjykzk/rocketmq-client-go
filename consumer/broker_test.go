package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestSuggester(t *testing.T) {
	bs := brokerSuggester{
		table: make(map[string]int32),
	}

	assert.Equal(t, 0, bs.put(&message.Queue{}, 1))
	id, exist := bs.get(&message.Queue{})
	assert.True(t, exist)
	assert.Equal(t, 1, id)

	_, exist = bs.get(&message.Queue{Topic: "t"})
	assert.False(t, exist)
}
