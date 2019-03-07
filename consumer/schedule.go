package consumer

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type runnable interface {
	run()
}

type scheduledTask struct {
	time           time.Time
	sequenceNumber int64
	task           runnable
}

func (t *scheduledTask) run() {
	t.task.run()
}

func (t *scheduledTask) String() string {
	return fmt.Sprintf("time:%s, seq number:%d, task:%v", t.time, t.sequenceNumber, t.task)
}

func (t *scheduledTask) getDelay() time.Duration {
	return t.time.Sub(time.Now())
}

type delayedWorkQueue struct {
	sequencer int64

	queue []*scheduledTask
	lock  sync.Mutex

	newLastest chan struct{}
	readyTasks chan *scheduledTask

	exitChan chan struct{}
	wg       sync.WaitGroup
}

func newDelayedWorkQueue() *delayedWorkQueue {
	q := &delayedWorkQueue{
		queue:      make([]*scheduledTask, 0, 16),
		readyTasks: make(chan *scheduledTask, 16),
		newLastest: make(chan struct{}, 1),
		exitChan:   make(chan struct{}),
	}
	q.signal()
	return q
}

func (q *delayedWorkQueue) Len() int {
	return len(q.queue)
}

func (q *delayedWorkQueue) Less(i, j int) bool {
	ti, tj := q.queue[i], q.queue[j]
	diff := ti.time.Sub(tj.time)
	if diff < 0 {
		return true
	}
	if diff > 0 {
		return false
	}

	if ti.sequenceNumber < tj.sequenceNumber {
		return true
	}
	return false
}

func (q *delayedWorkQueue) Swap(i, j int) {
	q.queue[i], q.queue[j] = q.queue[j], q.queue[i]
}

func (q *delayedWorkQueue) Push(x interface{}) {
	q.queue = append(q.queue, x.(*scheduledTask))
}

func (q *delayedWorkQueue) Pop() interface{} {
	l := q.Len() - 1
	r := q.queue[l]
	q.queue = q.queue[:l]
	return r
}

func (q *delayedWorkQueue) take() (t *scheduledTask, closed bool) {
	select {
	case t = <-q.readyTasks:
	case <-q.exitChan:
		closed = true
	}
	return
}
func (q *delayedWorkQueue) readyQueue() <-chan *scheduledTask {
	return q.readyTasks
}

func (q *delayedWorkQueue) offer(r runnable, timeout time.Duration) {
	t := &scheduledTask{
		task:           r,
		time:           time.Now().Add(timeout),
		sequenceNumber: atomic.AddInt64(&q.sequencer, 1),
	}
	q.lock.Lock()
	heap.Push(q, t)
	if t == q.queue[0] {
		select {
		case q.newLastest <- struct{}{}:
		default:
		}
	}
	q.lock.Unlock()
}

func (q *delayedWorkQueue) signal() {
	q.wg.Add(1)

	go func() {
		timer := time.NewTimer(time.Hour * 100000) // looooooooooong time
		for {
			select {
			case <-q.newLastest: // new task with shortest delay inserted
				var delay time.Duration
				q.lock.Lock()
				if len(q.queue) > 0 { // case: the fresh task is consumed at timer timeout
					delay = q.queue[0].getDelay()
				}
				q.lock.Unlock()

				timer.Reset(delay)
			case <-timer.C:
				tasks, delay := q.takeReadysAndNextDelay()

				for _, t := range tasks {
					q.readyTasks <- t
				}

				if delay > 0 {
					timer.Reset(delay)
				}
			case <-q.exitChan:
				timer.Stop()
				q.wg.Done()
				return
			}
		}
	}()
}

func (q *delayedWorkQueue) takeReadysAndNextDelay() ([]*scheduledTask, time.Duration) {
	delay, tasks := time.Duration(0), make([]*scheduledTask, 0, 8)

	q.lock.Lock()
	for len(q.queue) > 0 {
		t := q.queue[0]
		if delay = t.getDelay(); delay > 0 {
			break
		}
		tasks = append(tasks, t)
		heap.Pop(q)
	}
	q.lock.Unlock()

	return tasks, delay
}

func (q *delayedWorkQueue) tasks() []*scheduledTask {
	q.lock.Lock()
	r := make([]*scheduledTask, len(q.queue))
	copy(r, q.queue)
	q.lock.Unlock()

	return r
}

func (q *delayedWorkQueue) shutdown() {
	close(q.exitChan)
	q.wg.Wait()
}

var (
	errShutdown = errors.New("scheduler shutdown")
)

type scheduler struct {
	queue *delayedWorkQueue

	workerCount int

	stopped  int32
	exitChan chan struct{}
	wg       sync.WaitGroup
}

func newScheduler(workerCount int) *scheduler {
	s := &scheduler{
		queue:       newDelayedWorkQueue(),
		workerCount: workerCount,
		exitChan:    make(chan struct{}),
	}
	s.startWorking()
	return s
}

func (s *scheduler) scheduleFuncAfter(f func(), delay time.Duration) error {
	if atomic.LoadInt32(&s.stopped) > 0 {
		return errShutdown
	}

	s.queue.offer(runnableFunc(f), delay)

	return nil
}

func (s *scheduler) startWorking() {
	s.wg.Add(s.workerCount)
	for i := 0; i < s.workerCount; i++ {
		go func() {
			workCh := s.queue.readyQueue()
			for {
				select {
				case t := <-workCh:
					t.run()
				case <-s.exitChan:
					s.wg.Done()
					return
				}
			}
		}()
	}
}

func (s *scheduler) shutdown() {
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		return
	}
	close(s.exitChan)
	s.wg.Wait()
	s.queue.shutdown()
}

type runnableFunc func()

func (r runnableFunc) run() {
	r()
}
