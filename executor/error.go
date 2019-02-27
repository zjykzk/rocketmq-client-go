package executor

import "errors"

var (
	errQueueIsFull        = errors.New("queue is full")
	errBadRunnable        = errors.New("bad runnable")
	errNotRunning         = errors.New("not running")
	errBadCoreAndMaxLimit = errors.New("bad core & max count limit")
	errEmptyQueue         = errors.New("empty queue")
)
