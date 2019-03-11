package consumer

import (
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
)

type pullCallback struct {
	request             *pullRequest
	sub                 *client.SubscribeData
	processPullResponse func(*rpc.PullResponse) *PullResult
	pullService         pullRequestDispatcher
	pullInterval        time.Duration
	consumeService      consumeService
	offsetStorer        offsetStorer
	sched               taskScheduler

	logger log.Logger
}

func (cb *pullCallback) run(resp *rpc.PullResponse, err error) {
	if err == nil {
		cb.onSuc(resp)
		return
	}

	cb.onError(err)
}

func (cb *pullCallback) onError(err error) {
	if !strings.HasPrefix(cb.request.messageQueue.Topic, rocketmq.RetryGroupTopicPrefix) {
		cb.logger.Warnf("pull %s error:%s", cb.request.String(), err)
	}

	cb.pullService.submitRequestLater(cb.request, PullTimeDelayWhenException)
}

func (cb *pullCallback) onSuc(resp *rpc.PullResponse) {
	pr := cb.processPullResponse(resp)
	switch pr.Status {
	case Found:
		cb.onFound(pr)
	case NoNewMessage:
		fallthrough
	case NoMatchedMessage:
		cb.onNoNewMessage(pr)
	case OffsetIllegal:
		cb.onOffsetIllegal(pr)
	default:
		panic("[BUG] cannot process the status:" + pr.Status.String())
	}
}

func (cb *pullCallback) onFound(pr *PullResult) {
	reqOffset := cb.updateNextOffset(pr.NextBeginOffset)
	cb.statiRT()

	req := cb.request
	if len(pr.Messages) <= 0 {
		cb.pullService.submitRequestImmediately(req)
		return
	}

	cb.statiTPS()
	cb.consumeService.submitConsumeRequest(pr.Messages, req.processQueue, req.messageQueue)
	if cb.pullInterval <= 0 {
		cb.pullService.submitRequestImmediately(req)
	} else {
		cb.pullService.submitRequestLater(req, cb.pullInterval)
	}

	cb.detectBug(reqOffset, pr)
}

func (cb *pullCallback) updateNextOffset(nextOffset int64) (reqOffset int64) {
	reqOffset = cb.request.nextOffset
	cb.request.nextOffset = nextOffset
	return
}

func (cb *pullCallback) statiRT() {
	// TODO
}

func (cb *pullCallback) statiTPS() {
	// TODO
}

func (cb *pullCallback) detectBug(reqOffset int64, pr *PullResult) {
	firstOffset := pr.Messages[0].QueueOffset
	if pr.NextBeginOffset < reqOffset || firstOffset < reqOffset {
		cb.logger.Warnf(
			"[BUG] pull message result maybe data wrong, nextOffset:%d, firstOffset:%d, reqOffset:%d",
			pr.NextBeginOffset, firstOffset, reqOffset,
		)
	}
}

func (cb *pullCallback) onNoNewMessage(pr *PullResult) {
	cb.updateNextOffset(pr.NextBeginOffset)
	cb.pullService.submitRequestImmediately(cb.request)
	cb.trySaveConsumedOffset()
}

func (cb *pullCallback) trySaveConsumedOffset() {
	req := cb.request
	if req.processQueue.messageCount() == 0 {
		cb.offsetStorer.updateOffsetIfGreater(req.messageQueue, req.nextOffset)
	}
}

func (cb *pullCallback) onOffsetIllegal(pr *PullResult) {
	cb.updateNextOffset(pr.NextBeginOffset)
	req := cb.request
	req.processQueue.drop()

	_ = cb.sched.scheduleFuncAfter(func() {
		cb.offsetStorer.updateOffset(req.messageQueue, pr.NextBeginOffset)
		cb.offsetStorer.persistOne(req.messageQueue)
		cb.consumeService.dropAndRemoveProcessQueue(req.messageQueue)
	}, 10*time.Second) // IGNORE ERROR
}
