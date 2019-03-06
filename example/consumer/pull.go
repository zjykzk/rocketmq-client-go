package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type messageQueueChanger struct {
	consumer *consumer.PullConsumer
	logger   log.Logger

	offset sync.Map
}

func (qc *messageQueueChanger) Change(topic string, all, divided []*message.Queue) {
	fmt.Printf("all queues:%v, divided:%v\n", all, divided)
	c := qc.consumer
	for _, q := range divided {

		for {
			pr, err := c.PullSync(q, tags, qc.getOffset(*q), 32)
			if err != nil {
				qc.logger.Errorf("pull error:%v", err)
				break
			}
			qc.updateOffset(*q, pr.NextBeginOffset)
			fmt.Printf("pull %s count:%d\n", q, len(pr.Messages))
			if pr.Status == consumer.NoNewMessage {
				break
			}
		}
	}
}

func (qc *messageQueueChanger) getOffset(q message.Queue) int64 {
	v, ok := qc.offset.Load(q)
	if !ok {
		return 0
	}
	return v.(int64)
}

func (qc *messageQueueChanger) updateOffset(q message.Queue, offset int64) {
	qc.offset.Store(q, offset)
}

func runPull() {
	logger, err := newLogger("pull.log")
	if err != nil {
		fmt.Printf("new logger of pull.loge error:%s", err)
		return
	}

	c := consumer.NewPullConsumer("example-group", strings.Split(namesrvAddrs, ","), logger)

	c.Register([]string{topic}, &messageQueueChanger{
		consumer: c,
		logger:   logger,
	})

	err = c.Start()
	if err != nil {
		fmt.Printf("start consumer error:%v", err)
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-signalChan:
		c.Shutdown()
	}
}
