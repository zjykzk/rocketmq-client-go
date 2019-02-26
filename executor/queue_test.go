package executor

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type counter struct {
	sync.Mutex
	c int
}

func (c *counter) Run() {
	c.Lock()
	c.c++
	c.Unlock()
}

func TestLinkedQueue(t *testing.T) {
	var q BlockQueue = NewLinkedBlockingQueue()
	c := &counter{}

	q.Put(c)
	assert.Equal(t, 1, q.Size())
	q.Put(c)
	assert.Equal(t, 2, q.Size())
	cc, _ := q.Take()
	assert.Equal(t, c, cc.(*counter))
	assert.Equal(t, 1, q.Size())
	cc, _ = q.Take()
	assert.Equal(t, c, cc.(*counter))
	assert.Equal(t, 0, q.Size())

	wg := sync.WaitGroup{}
	n := 1
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			q.Put(c)
		}()
	}

	go func() {
		for r, _ := q.Take(); r != nil; {
			r.Run()
			wg.Done()
			r, _ = q.Take()
		}
	}()

	wg.Wait()
	assert.Equal(t, n, c.c)
	assert.Equal(t, 0, q.Size())
	go func() {
		wg.Add(1)
		q.Put(nil)
	}()
}
