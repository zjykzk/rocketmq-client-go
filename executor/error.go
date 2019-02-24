package executor

import "errors"

var (
	errQueueIsFull = errors.New("queue is full")
	errBadRunnable = errors.New("bad runnable")
	errNotRunning  = errors.New("not running")
)
