package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/producer"
)

type messageQueueSelector struct{}

func (s messageQueueSelector) Select(mqs []*message.Queue, m *message.Message, arg interface{}) *message.Queue {
	orderID := arg.(int)
	return mqs[orderID%len(mqs)]
}

func runProucer() {
	logger, err := newLogger("producer.log")
	if err != nil {
		fmt.Printf("new logger of producer.loge error:%s\n", err)
		return
	}

	p := producer.New(group, strings.Split(namesrvAddrs, ","), logger)
	err = p.Start()
	if err != nil {
		fmt.Printf("start producer error:%s\n", err)
		return
	}

	defer p.Shutdown()

	tagList := strings.Split(tags, ",")
	getTag := func(i int) string {
		l := len(tagList)
		if l == 0 {
			return ""
		}
		return tagList[i%len(tagList)]
	}

	for orderID := 0; orderID < 100; orderID++ {
		m := &message.Message{Topic: topic, Body: []byte("test order:" + strconv.Itoa(orderID))}
		m.SetTags(getTag(orderID))

		_, err := p.SendSyncWithSelector(m, messageQueueSelector{}, orderID)
		if err != nil {
			fmt.Printf("send with selector error:%s\n", err)
			continue
		}
	}

	println("DONE")
}
