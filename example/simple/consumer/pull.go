package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type messageQueueChanger struct {
	consumer *consumer.PullConsumer
	logger   log.Logger

	workers sync.Map
	offset  sync.Map
}

func (qc *messageQueueChanger) Change(topic string, all, divided []*message.Queue) {
	qc.workers.Range(func(_, v interface{}) bool {
		v.(*worker).stop()
		return true
	})

	fmt.Printf("new messages:%+v\n", divided)
	for _, q := range divided {
		qc.launchWorkerIfNot(q)
	}
}

func (qc *messageQueueChanger) launchWorkerIfNot(q *message.Queue) {
	w := &worker{offset: &qc.offset, queue: q, exitChan: make(chan struct{}), consumer: qc.consumer}
	qc.workers.Store(*q, w)
	go w.start()
}

func (qc *messageQueueChanger) shutdown() {
	qc.workers.Range(func(_, v interface{}) bool {
		v.(*worker).stop()
		return true
	})
}

type worker struct {
	state    int32
	offset   *sync.Map
	queue    *message.Queue
	exitChan chan struct{}
	consumer *consumer.PullConsumer
}

func (w *worker) getOffset(q message.Queue) int64 {
	v, ok := w.offset.Load(q)
	if !ok {
		return 0
	}
	return v.(int64)
}

func (w *worker) updateOffset(q message.Queue, offset int64) {
	w.offset.Store(q, offset)
}

func (w *worker) start() {
	for {
		select {
		case <-w.exitChan:
			return
		default:
		}

		q := w.queue
		pr, err := w.consumer.PullSyncBlockIfNotFound(q, tags, w.getOffset(*q), 32)
		if err != nil {
			fmt.Printf("pull error:%v\n", err)
			continue
		}
		w.updateOffset(*q, pr.NextBeginOffset)
		fmt.Printf("pull %s count:%d\n", q, len(pr.Messages))
	}
}

func (w *worker) stop() {
	if atomic.CompareAndSwapInt32(&w.state, 0, 1) {
		close(w.exitChan)
	}
}

func runPull() {
	logger, err := newLogger("pull.log")
	if err != nil {
		fmt.Printf("new logger of pull.loge error:%s", err)
		return
	}

	c := consumer.NewPullConsumer(group, strings.Split(namesrvAddrs, ","), logger)
	qc := &messageQueueChanger{consumer: c, logger: logger}
	c.UnitName = "pull"

	c.Register([]string{topic}, qc)

	err = c.Start()
	if err != nil {
		fmt.Printf("start consumer error:%v", err)
		return
	}

	waitQuitSignal(func() { qc.shutdown(); c.Shutdown() })
}
