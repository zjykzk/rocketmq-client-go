package consumer

import "time"

type timeoutLocker struct {
	sem chan struct{}
}

func newTimeoutLocker() *timeoutLocker {
	return &timeoutLocker{
		sem: make(chan struct{}, 1),
	}
}

func (l *timeoutLocker) tryLock(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	select {
	case l.sem <- struct{}{}:
		timer.Stop()
		return true
	case <-timer.C:
		return false
	}
}

func (l *timeoutLocker) unlock() {
	<-l.sem
}
