package consumer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

const (
	defaultRequestBufferSize = 10
)

type pullRequest struct {
	group         string
	messageQueue  *message.Queue
	processQueue  *processQueue
	nextOffset    int64
	isLockedFirst bool
}

func (r *pullRequest) String() string {
	return fmt.Sprintf("group:%s,message queue:%s,nextoffset:%d", r.group, r.messageQueue, r.nextOffset)
}

type messagePuller interface {
	pull(r *pullRequest)
}

type pullServiceConfig struct {
	messagePuller     messagePuller
	requestBufferSize int
	logger            log.Logger
}

type pullService struct {
	pullServiceConfig
	queuesOfMessageQueue sync.Map
	sched                *scheduler

	logger log.Logger

	exitChan chan struct{}
}

func newPullService(conf pullServiceConfig) (*pullService, error) {
	if conf.messagePuller == nil {
		return nil, errors.New("new pull service error:empty message puller")
	}

	if conf.logger == nil {
		return nil, errors.New("new pull service error:empty logger")
	}

	if conf.requestBufferSize < defaultRequestBufferSize {
		conf.requestBufferSize = defaultRequestBufferSize
	}

	return &pullService{
		pullServiceConfig: conf,
		sched:             newScheduler(1),
		logger:            conf.logger,
		exitChan:          make(chan struct{}),
	}, nil
}

func (ps *pullService) submitRequestLater(r *pullRequest, delay time.Duration) {
	ps.sched.scheduleFuncAfter(func() { ps.submitRequestImmediately(r) }, delay)
}

func (ps *pullService) submitRequestImmediately(r *pullRequest) {
	q, created := ps.getOrCreateRequestQueue(r.messageQueue)
	if created {
		ps.startPulling(q)
	}
	select {
	case q <- r:
	default:
		ps.logger.Warnf("pull queue is full, ignore the pull request %+v", r)
	}
}

func (ps *pullService) getOrCreateRequestQueue(q *message.Queue) (chan *pullRequest, bool) {
	key := q.HashKey()
	qr, ok := ps.queuesOfMessageQueue.Load(key)
	if ok {
		return qr.(chan *pullRequest), false
	}

	qr, ok = ps.queuesOfMessageQueue.LoadOrStore(key, make(chan *pullRequest, 10))
	return qr.(chan *pullRequest), !ok
}

func (ps *pullService) startPulling(q chan *pullRequest) {
	go func() {
		for {
			select {
			case r := <-q:
				ps.messagePuller.pull(r)
			case <-ps.exitChan:
				return
			}
		}
	}()
}

func (ps *pullService) shutdown() {
	ps.logger.Info("shutdown pull service START")
	close(ps.exitChan)
	ps.logger.Info("shutdown pull service END")
}
