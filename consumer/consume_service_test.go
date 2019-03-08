package consumer

import "github.com/zjykzk/rocketmq-client-go/message"

type fakeConsumerService struct {
	queues    []message.Queue
	runInsert bool

	insertRet bool
	pt        *processQueue

	removeRet bool

	flowControllRet bool
	checkRet        error

	runConsumeRequest            bool
	runDropAndRemoveProcessQueue bool
}

func (m *fakeConsumerService) messageQueues() []message.Queue {
	return m.queues
}

func (m *fakeConsumerService) dropAndRemoveProcessQueue(mq *message.Queue) bool {
	m.runDropAndRemoveProcessQueue = true
	nqs := make([]message.Queue, 0, len(m.queues))
	for _, q := range m.queues {
		if q != *mq {
			nqs = append(nqs, q)
		}
	}
	m.queues = nqs
	return m.removeRet
}

func (m *fakeConsumerService) insertNewMessageQueue(mq *message.Queue) (*processQueue, bool) {
	m.runInsert = true
	m.queues = append(m.queues, *mq)
	return m.pt, m.insertRet
}

func (m *fakeConsumerService) flowControl(*processQueue) bool {
	return m.flowControllRet
}

func (m *fakeConsumerService) check(*processQueue) error {
	return m.checkRet
}

func (m *fakeConsumerService) submitConsumeRequest([]*message.Ext, *processQueue, *message.Queue) {
	m.runConsumeRequest = true
	return
}
