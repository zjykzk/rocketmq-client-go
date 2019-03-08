package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeTaskScheduler struct{}

func (s *fakeTaskScheduler) scheduleFuncAfter(f func(), delay time.Duration) error {
	f()
	return nil
}

func newTestCallback() *pullCallback {
	return &pullCallback{
		request:        &pullRequest{messageQueue: &message.Queue{}, processQueue: &processQueue{}},
		logger:         log.Std,
		pullService:    &fakePullRequestDispatcher{},
		consumeService: &fakeConsumerService{},
		offsetStorer:   &fakeOffsetStorer{},
		sched:          &fakeTaskScheduler{},
	}
}

func TestCallbackError(t *testing.T) {
	cb := newTestCallback()
	pullService := cb.pullService.(*fakePullRequestDispatcher)
	cb.onError(errors.New("bad"))
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, PullTimeDelayWhenException, pullService.delay)
}

func TestCallbackFound(t *testing.T) {
	testOnFound(t, newTestCallback(), &PullResult{})
}

func testOnFound(t *testing.T, cb *pullCallback, pr *PullResult) {
	pr.NextBeginOffset = 11
	// no message
	pullService := cb.pullService.(*fakePullRequestDispatcher)
	cb.onFound(pr)
	assert.Equal(t, int64(11), cb.request.nextOffset)
	assert.True(t, pullService.runSubmitImmediately)
	pullService.runSubmitImmediately = false

	// submit consume request
	pr.Messages = []*message.Ext{{}}
	cb.onFound(pr)
	consumeService := cb.consumeService.(*fakeConsumerService)
	assert.True(t, consumeService.runConsumeRequest)
	assert.True(t, pullService.runSubmitImmediately)
	consumeService.runConsumeRequest = false
	pullService.runSubmitImmediately = false

	// submit pull request later
	pr.Messages = []*message.Ext{{}}
	cb.pullInterval = time.Second
	cb.onFound(pr)
	assert.True(t, pullService.runSubmitLater)
	assert.Equal(t, cb.pullInterval, pullService.delay)
}

func TestOnNoMessage(t *testing.T) {
	testOnNoMessage(t, newTestCallback(), &PullResult{})
}

func testOnNoMessage(t *testing.T, cb *pullCallback, pr *PullResult) {
	pr.NextBeginOffset = 22
	pullService := cb.pullService.(*fakePullRequestDispatcher)
	cb.onNoNewMessage(pr)
	assert.Equal(t, int64(22), cb.request.nextOffset)
	assert.True(t, pullService.runSubmitImmediately)
	pullService.runSubmitImmediately = false
	assert.True(t, cb.offsetStorer.(*fakeOffsetStorer).runUpdate)
}

func TestOnOffsetIllegal(t *testing.T) {
	testOnOffsetIllegal(t, newTestCallback(), &PullResult{})
}

func testOnOffsetIllegal(t *testing.T, cb *pullCallback, pr *PullResult) {
	pr.NextBeginOffset = 33
	cb.onOffsetIllegal(pr)
	assert.Equal(t, int64(33), cb.request.nextOffset)
	assert.True(t, cb.request.processQueue.isDropped())
	offsetStorer := cb.offsetStorer.(*fakeOffsetStorer)
	assert.Equal(t, int64(33), offsetStorer.offset)
	assert.True(t, offsetStorer.runPersistOne)
	assert.True(t, cb.consumeService.(*fakeConsumerService).runDropAndRemoveProcessQueue)
}

func TestOnSuc(t *testing.T) {
	cb, pr, req := newTestCallback(), &PullResult{}, &rpc.PullResponse{}
	cb.processPullResponse = func(*rpc.PullResponse) *PullResult { return pr }
	// on found
	pr.Status = Found
	cb.onSuc(req)

	pr.Status = NoNewMessage
	cb.onSuc(req)

	pr.Status = NoMatchedMessage
	cb.onSuc(req)

	pr.Status = OffsetIllegal
	cb.onSuc(req)
}
