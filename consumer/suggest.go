package consumer

import (
	"sync"

	"github.com/zjykzk/rocketmq-client-go/message"
)

type brokerSuggester struct {
	sync.RWMutex
	table map[string]int32 // key: message queue's hask key, value: broker id
}

func (s *brokerSuggester) get(q *message.Queue) (id int32, exist bool) {
	s.RLock()
	id, exist = s.table[q.HashKey()]
	s.RUnlock()
	return
}

func (s *brokerSuggester) put(q *message.Queue, brokerID int32) (prev int32) {
	k := q.HashKey()
	s.Lock()
	prev = s.table[k]
	s.table[k] = brokerID
	s.Unlock()
	return prev
}
