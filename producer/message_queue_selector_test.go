package producer

import "github.com/zjykzk/rocketmq-client-go/message"

type fakeMessageQueueSelector struct {
	selectRet *message.Queue
}

func (s *fakeMessageQueueSelector) Select(mqs []*message.Queue, m *message.Message, arg interface{}) *message.Queue {
	return s.selectRet
}
