package consumer

import (
	"container/heap"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockRunnable struct {
	id int
}

func (m *mockRunnable) run() {}

func TestDelayedWorkQueue(t *testing.T) {
	q := newDelayedWorkQueue()
	defer q.shutdown()

	timeout := time.Now().Add(time.Hour)
	heap.Push(q, &scheduledTask{time: timeout, sequenceNumber: 2})
	heap.Push(q, &scheduledTask{time: timeout, sequenceNumber: 1})

	task := heap.Pop(q).(*scheduledTask)
	assert.Equal(t, 1, task.sequenceNumber)

	task = heap.Pop(q).(*scheduledTask)
	assert.Equal(t, 2, task.sequenceNumber)

	assert.Equal(t, 0, q.Len())

	q.offer(&mockRunnable{id: 1}, time.Millisecond*10)
	q.offer(&mockRunnable{id: 2}, time.Millisecond*1)
	q.offer(&mockRunnable{id: 3}, time.Millisecond*20)

	task, _ = q.take()
	assert.Equal(t, 2, task.task.(*mockRunnable).id)

	waitChan := make(chan struct{})
	// insert first
	go func() {
		close(waitChan)
		task, _ = q.take()
		assert.Equal(t, 4, task.task.(*mockRunnable).id)
	}()

	<-waitChan
	time.Sleep(time.Millisecond)
	q.offer(&mockRunnable{id: 4}, time.Millisecond*5)
	fmt.Printf("offert %+v\n", q.queue)

	task, _ = q.take()
	assert.Equal(t, 1, task.task.(*mockRunnable).id)
	fmt.Printf("take %+v\n", q.queue)
	task, _ = q.take()
	assert.Equal(t, 3, task.task.(*mockRunnable).id)

	go func() {
		q.offer(&mockRunnable{id: -1}, time.Millisecond*10)
	}()
	task, _ = q.take()
	assert.Equal(t, -1, task.task.(*mockRunnable).id)

	offer := func(count int) {
		for i := 0; i < count; i++ {
			q.offer(&mockRunnable{id: i}, time.Duration(i%7)*time.Millisecond*20)
		}
	}

	tasks := make([]*scheduledTask, 0, 1024)
	taker := func(count int) {
		for i := 0; i < count; i++ {
			task, c := q.take()
			assert.False(t, c)
			tasks = append(tasks, task)
		}
	}

	count := 10000
	go offer(count)

	taker(count)
	for i := 1; i < count; i++ {
		if !tasks[i-1].time.Before(tasks[i].time) {
			t.Log(
				tasks[i-1], "delay ", tasks[i-1].task.(*mockRunnable).id%7,
				tasks[i], "delay ", tasks[i].task.(*mockRunnable).id%7,
			)
		}
		assert.True(t, tasks[i-1].time.Before(tasks[i].time))
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
