package consumer

import (
	"container/heap"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeRunnable struct {
	id int
}

func (m *fakeRunnable) run() {}

func TestDelayedWorkQueue(t *testing.T) {
	q := newDelayedWorkQueue()
	defer q.shutdown()

	timeout := time.Now().Add(time.Hour)
	heap.Push(q, &scheduledTask{time: timeout, sequenceNumber: 2})
	heap.Push(q, &scheduledTask{time: timeout, sequenceNumber: 1})

	task := heap.Pop(q).(*scheduledTask)
	assert.Equal(t, int64(1), task.sequenceNumber)

	task = heap.Pop(q).(*scheduledTask)
	assert.Equal(t, int64(2), task.sequenceNumber)

	assert.Equal(t, 0, q.Len())

	q.offer(&fakeRunnable{id: 1}, time.Millisecond*10)
	q.offer(&fakeRunnable{id: 2}, time.Millisecond*1)
	q.offer(&fakeRunnable{id: 3}, time.Millisecond*20)

	task, _ = q.take()
	assert.Equal(t, 2, task.task.(*fakeRunnable).id)

	waitTake, waitOffer := make(chan struct{}), make(chan struct{})
	// insert first
	go func() {
		<-waitOffer
		close(waitTake)
		task, _ = q.take()
		assert.Equal(t, 4, task.task.(*fakeRunnable).id)
	}()

	q.offer(&fakeRunnable{id: 4}, time.Millisecond*5)
	fmt.Printf("offert %+v\n", q.tasks())
	close(waitOffer)
	<-waitTake

	task1, _ := q.take()
	assert.Equal(t, 1, task1.task.(*fakeRunnable).id)
	fmt.Printf("take %+v\n", q.tasks())
	task3, _ := q.take()
	assert.Equal(t, 3, task3.task.(*fakeRunnable).id)

	go func() {
		q.offer(&fakeRunnable{id: -1}, time.Millisecond*10)
	}()
	task2, _ := q.take()
	assert.Equal(t, -1, task2.task.(*fakeRunnable).id)

	// order
	offer := func(count int) {
		for i := 0; i < count; i++ {
			q.offer(&fakeRunnable{id: i}, time.Millisecond)
		}
	}

	take := func(count int) []*scheduledTask {
		tasks := make([]*scheduledTask, 0, count)
		for i := 0; i < count; i++ {
			task, c := q.take()
			assert.False(t, c)
			tasks = append(tasks, task)
		}
		return tasks
	}

	count := 10000
	go offer(count)

	tasks := take(count)
	for i := 1; i < count; i++ {
		assert.True(t, tasks[i-1].time.Before(tasks[i].time), "%s, %s", tasks[i-1].time, tasks[i].time)
	}
}

func TestSched(t *testing.T) {
	s := newScheduler(1)
	waitChan := make(chan struct{})
	s.scheduleFuncAfter(func() { waitChan <- struct{}{} }, time.Millisecond)
	<-waitChan

	s.shutdown()
	assert.Equal(t, errShutdown, s.scheduleFuncAfter(func() {}, time.Millisecond))
}
