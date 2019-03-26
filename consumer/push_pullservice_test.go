package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeMessagePuller struct {
	runPull bool
}

func (p *fakeMessagePuller) pull(r *pullRequest) { p.runPull = true }

func TestNewPullService(t *testing.T) {
	_, err := newPullService(pullServiceConfig{})
	assert.NotNil(t, err)
	_, err = newPullService(pullServiceConfig{messagePuller: &fakeMessagePuller{}})
	assert.NotNil(t, err)
	ps, err := newPullService(pullServiceConfig{
		messagePuller: &fakeMessagePuller{},
		logger:        log.Std,
	})
	assert.Nil(t, err)
	assert.NotNil(t, ps)
	assert.Equal(t, defaultRequestBufferSize, ps.requestBufferSize)
}

func TestPullService(t *testing.T) {
	ps, err := newPullService(pullServiceConfig{
		messagePuller: &fakeMessagePuller{},
		logger:        log.Std,
	})
	if err != nil {
		t.Fatal(err)
	}

	r := &pullRequest{
		messageQueue: &message.Queue{},
	}

	ps.submitRequestImmediately(r)
	count := func() int {
		c := 0
		ps.queuesOfMessageQueue.Range(func(_, _ interface{}) bool {
			c++
			return true
		})
		return c
	}
	assert.Equal(t, 1, count())
	ps.submitRequestImmediately(r)
	assert.Equal(t, 1, count())

	ps.shutdown()
}

func TestSubmitRequestLater(t *testing.T) {
	ps, err := newPullService(pullServiceConfig{
		messagePuller: &fakeMessagePuller{},
		logger:        log.Std,
	})
	if err != nil {
		t.Fatal(err)
	}

	ps.submitRequestLater(&pullRequest{messageQueue: &message.Queue{}}, time.Second)
	assert.Equal(t, 1, len(ps.sched.queue.tasks()))
}

func TestGetOrCreateRequestQueue(t *testing.T) {
	ps, err := newPullService(pullServiceConfig{
		messagePuller: &fakeMessagePuller{},
		logger:        log.Std,
	})
	if err != nil {
		t.Fatal(err)
	}

	prc, ok := ps.getOrCreateRequestQueue(&message.Queue{})
	assert.True(t, ok)
	assert.NotNil(t, prc)
	prc1, ok := ps.getOrCreateRequestQueue(&message.Queue{})
	assert.False(t, ok)
	assert.Equal(t, prc, prc1)
}
