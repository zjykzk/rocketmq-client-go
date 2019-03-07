package client

import (
	"testing"

	"github.com/zjykzk/rocketmq-client-go/message"

	"github.com/stretchr/testify/assert"
)

func TestData(t *testing.T) {
	sq := SubscribeData{}
	assert.True(t, sq.Equal(&sq))

	sq1 := SubscribeData{Topic: "t"}
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Expr = "new"
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Version = 2
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Typ = "type"
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Tags = []string{"t"}
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Tags = []string{"t", "t2"}
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Codes = []uint32{1}
	assert.True(t, !sq.Equal(&sq1))

	sq = sq1
	sq1.Codes = []uint32{2}
	assert.True(t, !sq.Equal(&sq1))
}

func TestDataTable(t *testing.T) {
	dt := NewSubcribeTable()

	prev := dt.Put("topic", &SubscribeData{})
	assert.Nil(t, prev)

	prev = dt.PutIfAbsent("topic", nil)
	assert.NotNil(t, prev)

	assert.NotNil(t, dt.Get("topic"))

	ds := dt.Datas()
	assert.Equal(t, 1, len(ds))

	prev = dt.Delete("topic")
	assert.NotNil(t, prev)

	assert.Nil(t, dt.Get("topic"))
}

func TestQueueTable(t *testing.T) {
	qt := NewSubscribeQueueTable()
	topic := "topic"

	prev := qt.Put(topic, []*message.Queue{&message.Queue{Topic: topic}})
	assert.Nil(t, prev)

	prev = qt.Get(topic)
	assert.NotNil(t, prev)
	assert.Equal(t, 1, len(prev))
	assert.Equal(t, topic, prev[0].Topic)

	topics := qt.Topics()
	assert.Equal(t, 1, len(topics))
	assert.Equal(t, []string{"topic"}, topics)

	prev = qt.Delete(topic)
	assert.NotNil(t, prev)
	assert.Equal(t, 1, len(prev))
	assert.Equal(t, topic, prev[0].Topic)

	assert.Nil(t, qt.Get(topic))
}
