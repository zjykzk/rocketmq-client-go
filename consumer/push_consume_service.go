package consumer

import (
	"errors"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

const (
	defaultPullExpiredInterval = time.Second * 120
)

type messageSendBack interface {
	SendBack(m *message.Ext, delayLevel int, broker string) error
}

type baseConsumeService struct {
	group                  string
	messageModel           Model
	messageSendBack        messageSendBack
	offseter               offsetStorer
	oldMessageQueueRemover func(*message.Queue) bool

	processQueues       sync.Map
	pullExpiredInterval time.Duration

	scheduler *scheduler

	wg       sync.WaitGroup
	exitChan chan struct{}
	logger   log.Logger
}

type consumeServiceConfig struct {
	group                  string
	schedWorkerCount       int
	messageModel           Model
	messageSendBack        messageSendBack
	offseter               offsetStorer
	oldMessageQueueRemover func(*message.Queue) bool
	logger                 log.Logger
}

func newConsumeService(conf consumeServiceConfig) (*baseConsumeService, error) {
	if conf.group == "" {
		return nil, errors.New("new consumer service error:empty group")
	}

	if conf.logger == nil {
		return nil, errors.New("new consumer service error:empty logger")
	}

	if conf.messageSendBack == nil {
		return nil, errors.New("new consumer service error:empty sendback")
	}

	if conf.offseter == nil {
		return nil, errors.New("new consumer service error:empty offseter")
	}

	if conf.schedWorkerCount <= 0 {
		conf.schedWorkerCount = 2
	}

	c := &baseConsumeService{
		group:                  conf.group,
		messageModel:           conf.messageModel,
		messageSendBack:        conf.messageSendBack,
		scheduler:              newScheduler(conf.schedWorkerCount),
		offseter:               conf.offseter,
		oldMessageQueueRemover: conf.oldMessageQueueRemover,
		pullExpiredInterval:    defaultPullExpiredInterval,

		exitChan: make(chan struct{}),
		logger:   conf.logger,
	}

	if c.oldMessageQueueRemover == nil {
		c.oldMessageQueueRemover = c.dropAndRemoveProcessQueue
	}

	return c, nil
}

func (cs *baseConsumeService) resetRetryTopic(messages []*message.Ext) {
	retryTopic := retryTopic(cs.group)
	for _, m := range messages {
		if retryTopic == m.GetProperty(message.PropertyRetryTopic) {
			m.Topic = retryTopic
		}
	}
}

func (cs *baseConsumeService) startFunc(f func(), period time.Duration) {
	cs.wg.Add(1)
	go func() {
		ticker := time.NewTicker(period)
		for {
			select {
			case <-cs.exitChan:
				ticker.Stop()
				cs.wg.Done()
				return
			case <-ticker.C:
				f()
			}
		}
	}()
}

func (cs *baseConsumeService) start() {
	cs.startFunc(cs.dropExpiredProcessQueues, time.Second*10)
}

func (cs *baseConsumeService) shutdown() {
	cs.logger.Info("shutdown consume sevice START")
	close(cs.exitChan)
	cs.wg.Wait()
	cs.scheduler.shutdown()
	cs.logger.Info("shutdown consume sevice END")
}

func (cs *baseConsumeService) messageQueues() (mqs []message.Queue) {
	cs.processQueues.Range(func(k, _ interface{}) bool {
		mqs = append(mqs, k.(message.Queue))
		return true
	})
	return
}

func (cs *baseConsumeService) dropAndRemoveProcessQueue(mq *message.Queue) bool {
	v, ok := cs.processQueues.Load(*mq)
	if !ok {
		return false
	}
	cs.offseter.persistOne(mq)
	cs.offseter.removeOffset(mq)

	pq := (*processQueue)(unsafe.Pointer(reflect.ValueOf(v).Pointer()))
	pq.drop()
	cs.processQueues.Delete(*mq)
	return true
}

func (cs *baseConsumeService) dropExpiredProcessQueues() {
	cs.processQueues.Range(func(k, v interface{}) bool {
		pq := (*processQueue)(unsafe.Pointer(reflect.ValueOf(v).Pointer()))
		if !pq.isPullExpired(cs.pullExpiredInterval) {
			return true // next
		}

		mq := k.(message.Queue)
		if cs.oldMessageQueueRemover(&mq) {
			cs.processQueues.Delete(k)
		}
		return true
	})
}
