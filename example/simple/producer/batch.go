package main

import (
	"fmt"
	"time"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/producer"
)

func sendBatch(p *producer.Producer) (*producer.SendResult, error) {
	data := message.Data{Body: []byte(time.Now().String())}
	m := &message.Batch{Topic: topic, Datas: []message.Data{data}}

	r, err := p.SendBatchSync(m)

	if err == nil {
		fmt.Printf("sub message id:%s\n", message.GetUniqID(m.Datas[0].Properties))
	}

	return r, err
}
