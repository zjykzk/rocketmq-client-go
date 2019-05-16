package main

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/producer"
)

func sendSimple(p *producer.Producer) (*producer.SendResult, error) {
	now := time.Now()
	m := &message.Message{Topic: topic, Body: []byte(now.String())}
	m.SetTags("simple")

	return p.SendSync(m)
}
