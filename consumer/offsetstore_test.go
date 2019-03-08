package consumer

import (
	"sync"

	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeOffsetStorer struct {
	runUpdate     bool
	offset        int64
	readOffsetErr error
	runReadOffset bool
	readType      int

	runPersistOne bool

	sync.Mutex
}

func (m *fakeOffsetStorer) persist() error {
	return nil
}

func (m *fakeOffsetStorer) updateQueues(...*message.Queue) {
	return
}

func (m *fakeOffsetStorer) updateOffset(_ *message.Queue, offset int64) {
	m.Lock()
	m.offset = offset
	m.runUpdate = true
	m.Unlock()
}

func (m *fakeOffsetStorer) updateOffsetIfGreater(_ *message.Queue, offset int64) {
	m.Lock()
	m.offset = offset
	m.runUpdate = true
	m.Unlock()
}

func (m *fakeOffsetStorer) persistOne(_ *message.Queue) {
	m.runPersistOne = true
}

func (m *fakeOffsetStorer) removeOffset(_ *message.Queue) (int64, bool) {
	return m.offset, false
}

func (m *fakeOffsetStorer) readOffset(q *message.Queue, readType int) (offset int64, err error) {
	m.Lock()
	m.runReadOffset = true
	m.readType = readType
	m.Unlock()
	return m.offset, m.readOffsetErr
}
