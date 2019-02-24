package executor

import (
	"sync"
)

// BlockQueue thread-safed queue
type BlockQueue interface {
	// Put the task to the head, waiting until success or error happends
	Put(r Runnable) error
	// Take the task from the head, waiting until has elements
	Take() (Runnable, error)
	// Size the size of queue
	Size() int
	// IsFull return true if queue is full
	IsFull() bool
}

type node struct {
	r          Runnable
	prev, next *node
}

// linkedBlockingQueue based on linked nodes
type linkedBlockingQueue struct {
	notEmpty sync.Cond
	q        node
	size     int
}

// NewLinkedBlockingQueue returns the link based blocking queue
func NewLinkedBlockingQueue() BlockQueue {
	q := &linkedBlockingQueue{
		notEmpty: sync.Cond{L: &sync.Mutex{}},
	}
	q.q.prev = &q.q
	q.q.next = &q.q
	return q
}

// Put adds one task to the last
func (lq *linkedBlockingQueue) Put(r Runnable) error {
	n := &node{r: r}
	lq.notEmpty.L.Lock()
	n.prev, n.next = lq.q.prev, &lq.q
	lq.q.prev.next, lq.q.prev = n, n
	lq.size++
	lq.notEmpty.L.Unlock()
	lq.notEmpty.Signal()

	return nil
}

// Take remove one from head and returns
func (lq *linkedBlockingQueue) Take() (r Runnable, err error) {
	lq.notEmpty.L.Lock()
	for lq.size == 0 {
		lq.notEmpty.Wait()
	}
	n := lq.q.next
	r = n.r
	n.prev.next = n.next
	n.next.prev = n.prev
	lq.size--
	lq.notEmpty.L.Unlock()
	return
}

// Size returns the size of queue
func (lq *linkedBlockingQueue) Size() (s int) {
	lq.notEmpty.L.Lock()
	s = lq.size
	lq.notEmpty.L.Unlock()
	return
}

func (lq *linkedBlockingQueue) IsFull() bool {
	return false
}
