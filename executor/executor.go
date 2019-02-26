package executor

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	countBits = 29
	capacity  = (1 << countBits) - 1

	running int32 = (iota - 1) << 29
	shutdown
	stop
)

// Executor executes the submitted tasks
type Executor interface {
	Execute(r Runnable) error
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan Runnable
}

// GoroutinePoolExecutor executor with pooled goroutie
type GoroutinePoolExecutor struct {
	corePoolSize, maxPoolSize, ctl int32

	name           string
	workerChanPool sync.Pool
	queue          BlockQueue

	locker       sync.RWMutex
	hasWorker    *sync.Cond
	ready        []*workerChan
	pendingCount sync.WaitGroup

	wg          sync.WaitGroup
	stopCh      chan struct{}
	maxIdleTime time.Duration
}

var workerChanCap = func() int {
	// Use blocking workerChan if GOMAXPROCS=1.
	// This immediately switches Serve to WorkerFunc, which results
	// in higher performance (under go1.5 at least).
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}

	// Use non-blocking workerChan if GOMAXPROCS>1,
	// since otherwise the Serve caller (Acceptor) may lag accepting
	// new connections if WorkerFunc is CPU-bound.
	return 1
}()

// NewPoolExecutor creates GoroutinePoolExecutor
func NewPoolExecutor(
	name string,
	corePoolSize, maxPoolSize int32,
	keepLiveTime time.Duration,
	queue BlockQueue,
) (*GoroutinePoolExecutor, error) {
	if corePoolSize > maxPoolSize {
		return nil, errors.New("corePoolSize bigger maximumPoolSize")
	}

	if queue == nil {
		return nil, errors.New("empgy queue")
	}

	ex := &GoroutinePoolExecutor{
		name:         name,
		corePoolSize: corePoolSize,
		maxPoolSize:  maxPoolSize,
		maxIdleTime:  keepLiveTime,
		queue:        queue,
		ready:        make([]*workerChan, 0, 32),
		stopCh:       make(chan struct{}),
		workerChanPool: sync.Pool{
			New: func() interface{} {
				return &workerChan{ch: make(chan Runnable, workerChanCap)}
			},
		},
	}

	ex.hasWorker = sync.NewCond(&ex.locker)

	ex.start()

	return ex, nil
}

func (e *GoroutinePoolExecutor) start() {
	e.setState(running)
	e.wgWrap(func() {
		idleWorkers := make([]*workerChan, 0, 128)
		select {
		case <-e.stopCh:
			return
		default:
			e.cleanIdle(&idleWorkers)
		}
	})

	e.wgWrap(func() {
		e.startFeedFromQueue()
	})

}

func (e *GoroutinePoolExecutor) cleanIdle(idleWorkers *[]*workerChan) {
	now := time.Now()
	e.locker.Lock()
	ready := e.ready
	n, l, maxIdleTime := 0, len(ready), e.maxIdleTime
	for n < l && now.Sub(e.ready[n].lastUseTime) > maxIdleTime {
		n++
	}

	if n > 0 {
		*idleWorkers = append((*idleWorkers)[:0], ready[:n]...)
		m := copy(ready, ready[n:])
		for i := m; i < l; i++ {
			ready[i] = nil
		}
		e.ready = ready[:m]
	}
	e.locker.Unlock()

	tmp := *idleWorkers
	for i, w := range tmp {
		w.ch <- nil
		tmp[i] = nil
	}
}

func (e *GoroutinePoolExecutor) startFeedFromQueue() {
	for {
		r, err := e.queue.Take()
		if err != nil || r == nil {
			break
		}

		e.locker.Lock()
		for len(e.ready) == 0 {
			e.hasWorker.Wait()
		}
		n := len(e.ready) - 1
		wch := e.ready[n]
		e.ready[n] = nil
		e.ready = e.ready[:n]
		e.locker.Unlock()

		wch.ch <- r
	}
}

// Execute run one task
func (e *GoroutinePoolExecutor) Execute(r Runnable) error {
	if r == nil {
		return errBadRunnable
	}

	if e.state() != running {
		return errNotRunning
	}

	createWorker := false
	workerCount := int32(e.workerCountOf())
	switch {
	case workerCount < e.corePoolSize:
		createWorker = true
	case workerCount < e.maxPoolSize && e.queue.IsFull():
		createWorker = true
	}

	if !createWorker {
		if e.queue.IsFull() {
			return errQueueIsFull
		}

		e.pendingCount.Add(1)
		e.queue.Put(r)
		return nil
	}

	e.pendingCount.Add(1)
	w, isNew := e.getWorkerCh()
	w.ch <- r

	if isNew {
		e.wgWrap(func() {
			e.startWorker(w)
			e.workerChanPool.Put(w)
		})
	}

	return nil
}

func (e *GoroutinePoolExecutor) getWorkerCh() (w *workerChan, isNew bool) {
	e.locker.Lock()
	ready := e.ready
	n := len(ready) - 1
	if n >= 0 {
		w = ready[n]
		ready[n] = nil
		e.ready = ready[:n]
	} else {
		e.ctl++
	}
	e.locker.Unlock()

	if w != nil {
		return
	}

	isNew = true
	w = e.workerChanPool.Get().(*workerChan)
	return
}

func (e *GoroutinePoolExecutor) startWorker(wch *workerChan) {
	var (
		r  Runnable
		ok bool
	)

	for {
		select {
		case r, ok = <-wch.ch:
		case <-e.stopCh:
			ok = false
		}

		if !ok {
			break
		}

		r.Run()

		e.pendingCount.Done()

		wch.lastUseTime = time.Now()
		e.locker.Lock()
		e.ready = append(e.ready, wch)
		e.locker.Unlock()
		e.hasWorker.Signal()
	}

	atomic.AddInt32(&e.ctl, -1)
}

// Shutdown shutdown the executor
func (e *GoroutinePoolExecutor) Shutdown() {
	e.setState(shutdown)

	e.queue.Put(nil)
	e.pendingCount.Wait() // wait task to be finished
	e.shutdownWorker()
	close(e.stopCh)

	e.wg.Wait()
	e.setState(stop)
}

func (e *GoroutinePoolExecutor) shutdownWorker() {
	e.locker.RLock()
	ready := e.ready
	e.locker.RUnlock()
	for _, w := range ready {
		close(w.ch)
	}
}

func (e *GoroutinePoolExecutor) workerCountOf() int {
	c := atomic.LoadInt32(&e.ctl)
	return int(c & capacity)
}

// WorkerCount returns the worker count in the executor
func (e *GoroutinePoolExecutor) WorkerCount() int {
	return e.workerCountOf()
}

func (e *GoroutinePoolExecutor) state() int32 {
	c := atomic.LoadInt32(&e.ctl)
	return c &^ capacity
}

func (e *GoroutinePoolExecutor) setState(s int32) {
	for {
		c := atomic.LoadInt32(&e.ctl)
		nc := c&capacity | s
		if atomic.CompareAndSwapInt32(&e.ctl, c, nc) {
			break
		}
	}
}

// ReadyCount returns the count of goroutine ready to run task
func (e *GoroutinePoolExecutor) ReadyCount() int {
	e.locker.RLock()
	c := len(e.ready)
	e.locker.RUnlock()
	return c
}

func (e *GoroutinePoolExecutor) wgWrap(f func()) {
	e.wg.Add(1)
	go func() {
		f()
		e.wg.Done()
	}()
}

func (e *GoroutinePoolExecutor) String() string {
	e.locker.RLock()
	l := len(e.ready)
	e.locker.RUnlock()
	return fmt.Sprintf(
		"GoroutinePoolExecutor:[name=%s,workerCount=%d, idleWorker=%d,corePoolSize=%d, maxPoolSize=%d,queue size=%d,queue is full=%t]",
		e.name, e.workerCountOf(), l, e.corePoolSize, e.maxPoolSize, e.queue.Size(), e.queue.IsFull(),
	)
}
