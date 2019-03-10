package consumer

import "github.com/zjykzk/rocketmq-client-go/message"

type consumeService interface {
	start()
	shutdown()

	messageQueues(topic string) []message.Queue
	dropAndRemoveProcessQueue(*message.Queue) bool
	insertNewMessageQueue(*message.Queue) (*processQueue, bool)
	flowControl(*processQueue) bool
	check(*processQueue) error
	submitConsumeRequest([]*message.Ext, *processQueue, *message.Queue)
}