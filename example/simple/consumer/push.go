package main

import (
	"fmt"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type simpleConsumer struct{}

func (c *simpleConsumer) Consume(msgs []*message.Ext, ctx *consumer.ConcurrentlyContext) consumer.ConsumeConcurrentlyStatus {
	fmt.Printf("receive new message:%s\n", msgs)
	return consumer.ConcurrentlySuccess
}

func runPush() {
	logger, err := newLogger("push.log")
	if err != nil {
		fmt.Printf("new logger of push.log error:%s", err)
		return
	}

	c, err := consumer.NewConcurrentlyConsumer(
		group, strings.Split(namesrvAddrs, ","), &simpleConsumer{}, logger,
	)
	if err != nil {
		fmt.Printf("create concurrent consumer error:%s", err)
		return
	}
	c.UnitName = "push"

	c.Subscribe(topic, tags)
	c.FromWhere = consumer.ConsumeFromFirstOffset

	err = c.Start()
	if err != nil {
		fmt.Printf("start consumer error:%v", err)
		return
	}

	waitQuitSignal(c.Shutdown)
}
