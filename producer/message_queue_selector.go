package producer

import "github.com/zjykzk/rocketmq-client-go/message"

// MessageQueueSelector select the message queue
type MessageQueueSelector interface {
	Select(mqs []*message.Queue, m *message.Message, arg interface{}) *message.Queue
}
