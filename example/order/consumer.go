package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type simpleConsumer struct {
	consumeTimes int
}

func (c *simpleConsumer) Consume(msgs []*message.Ext, ctx *consumer.OrderlyContext) consumer.ConsumeOrderlyStatus {
	fmt.Printf("receive new message:%s\n", msgs)

	c.consumeTimes++
	switch {
	case c.consumeTimes%2 == 0:
		ctx.SupsendCurrentQueueTime = 3 * time.Second
		return consumer.SuspendCurrentQueueMoment
	default:
		return consumer.OrderlySuccess
	}
}

func runConsumer() {
	logger, err := newLogger("consumer.log")
	if err != nil {
		fmt.Printf("new logger of consumer.log error:%s\n", err)
		return
	}

	c, err := consumer.NewOrderlyConsumer(
		group, strings.Split(namesrvAddrs, ","), &simpleConsumer{}, logger,
	)

	if err != nil {
		fmt.Printf("new orderly consumer error:%s\n", err)
		return
	}

	c.Subscribe(topic, strings.Replace(tags, ",", "||", -1))
	c.FromWhere = consumer.ConsumeFromFirstOffset

	err = c.Start()
	if err != nil {
		fmt.Printf("start consuming error:%s\n", err)
		return
	}

	waitQuitSignal(c.Shutdown)
}
