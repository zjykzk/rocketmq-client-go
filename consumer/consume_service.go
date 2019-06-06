package consumer

import (
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type consumeService interface {
	messageQueuesOfTopic(topic string) []message.Queue
	dropAndRemoveProcessQueue(*message.Queue) bool
	insertNewMessageQueue(*message.Queue) (*processQueue, bool)
	flowControl(*processQueue) bool
	check(*processQueue) error
	submitConsumeRequest([]*message.Ext, *processQueue, *message.Queue)
	dropAndClear(mq *message.Queue) error
	removeProcessQueue(mq *message.Queue)
	consumeMessageDirectly(msg *message.Ext, broker string) client.ConsumeMessageDirectlyResult
	properties() map[string]string
}
