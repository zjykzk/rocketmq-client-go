package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

func newTestConsumeService(t *testing.T) *baseConsumeService {
	cs, err := newConsumeService(consumeServiceConfig{
		group:           "test consume service",
		messageSendBack: &fakeSendback{},
		logger:          log.Std,
		offseter:        &fakeOffsetStorer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	cs.pullExpiredInterval = time.Millisecond
	return cs
}

func TestNewService(t *testing.T) {
	_, err := newConsumeService(consumeServiceConfig{group: "g"})
	assert.NotNil(t, err)

	_, err = newConsumeService(consumeServiceConfig{group: "g", logger: log.Std})
	assert.NotNil(t, err)

	_, err = newConsumeService(consumeServiceConfig{
		group: "g", logger: log.Std, messageSendBack: &fakeSendback{},
	})
	assert.NotNil(t, err)

	_, err = newConsumeService(consumeServiceConfig{
		group:           "g",
		logger:          log.Std,
		messageSendBack: &fakeSendback{},
		offseter:        &fakeOffsetStorer{},
	})
	assert.Nil(t, err)
}

func TestRemoveOldMessageQueue(t *testing.T) {
	cs := newTestConsumeService(t)
	mq := &message.Queue{}
	cs.processQueues.Store(message.Queue{}, &processQueue{})
	assert.True(t, cs.dropAndRemoveProcessQueue(mq))
	assert.False(t, cs.dropAndRemoveProcessQueue(mq))
}
