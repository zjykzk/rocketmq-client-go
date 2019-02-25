package consumer

import (
	"os"
	"testing"

	"github.com/zjykzk/rocketmq-client-go/message"

	"github.com/stretchr/testify/assert"
)

func TestNewLocalOffset(t *testing.T) {
	_, err := newLocalStore(localStoreConfig{
		rootPath: "testdata",
		clientID: "",
	})
	assert.NotNil(t, err)

	_, err = newLocalStore(localStoreConfig{
		rootPath: "testdata",
		clientID: "clientID",
		group:    "",
	})
	assert.NotNil(t, err)

	ls, err := newLocalStore(localStoreConfig{
		rootPath: "testdata",
		clientID: "clientID",
		group:    "group",
	})
	assert.Nil(t, err)
	assert.NotNil(t, ls)
}

func TestLocalOffset(t *testing.T) {
	ls, err := newLocalStore(localStoreConfig{
		rootPath: "testdata",
		clientID: "clientID",
		group:    "group",
	})
	assert.Nil(t, err)
	err = ls.load()
	t.Log(err)
	assert.Nil(t, err)

	q := &message.Queue{Topic: "TestLocalOffset", BrokerName: "b", QueueID: 1}
	ls.updateOffset(q, 1)
	of, err := ls.readOffset(q, ReadOffsetFromMemory)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), of)

	ls.updateOffsetIfGreater(q, 0)
	of, err = ls.readOffset(q, ReadOffsetFromMemory)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), of)

	ls.updateOffsetIfGreater(q, 2)
	of, err = ls.readOffset(q, ReadOffsetFromMemory)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), of)

	of, err = ls.readOffset(q, ReadOffsetFromStore)
	assert.Nil(t, err)

	of, err = ls.readOffset(q, ReadOffsetMemoryFirstThenStore)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), of)

	of, err = ls.readOffset(&message.Queue{BrokerName: "notexist"}, ReadOffsetFromMemory)
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), of)

	of, err = ls.readOffset(&message.Queue{BrokerName: "notexist"}, ReadOffsetMemoryFirstThenStore)
	assert.Nil(t, err)

	assert.Nil(t, ls.persist())
	// backup
	assert.Nil(t, ls.persist())
	f, err := os.Open(bakPath(ls.path))
	f.Close()
	assert.Nil(t, err)

	ls, err = newLocalStore(localStoreConfig{
		rootPath: "testdata",
		clientID: "clientID",
		group:    "group",
	})
	assert.Nil(t, err)

	assert.Nil(t, ls.load())
	of, err = ls.readOffset(q, ReadOffsetFromMemory)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), of)

	of, err = ls.readOffset(q, ReadOffsetFromStore)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), of)

	// get the offsets and message queue
	ls.updateOffset(&message.Queue{BrokerName: "n", Topic: "t"}, 3)
	queues, offsets := ls.queuesAndOffsets()
	assert.Equal(t, []message.Queue{
		{BrokerName: "b", Topic: "TestLocalOffset", QueueID: 1},
		{BrokerName: "n", Topic: "t", QueueID: 0},
	}, queues)
	assert.Equal(t, []int64{2, 3}, offsets)

	remove(ls)
}

func remove(ls *localStore) {
	os.RemoveAll("testdata")
}
