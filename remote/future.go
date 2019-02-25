package remote

import (
	"fmt"
	"sync"
	"time"
)

var (
	// DONOT CALL ME OUT OF THE FILE!
	futurePool = sync.Pool{
		New: func() interface{} { return &responseFuture{response: make(chan *Command, 1)} },
	}
)

// responseFuture waitable response
type responseFuture struct {
	response  chan *Command
	err       error
	startTime time.Time
	timeout   time.Duration
	id        int64
	ctx       *ChannelContext
}

// get return the reponse command
func (f *responseFuture) get() (*Command, error) {
	return <-f.response, f.err
}

func (f *responseFuture) put(resp *Command) {
	f.response <- resp
}

func (f *responseFuture) release() {
	futurePool.Put(f)
}

func (f *responseFuture) String() string {
	return fmt.Sprintf("id:%d, start:%s, timeout:%s, ctx:%s", f.id, f.startTime, f.timeout, f.ctx)
}

func newFuture(timeout time.Duration, id int64, ctx *ChannelContext) *responseFuture {
	r := futurePool.Get().(*responseFuture)
	r.timeout = timeout
	r.id = id
	r.startTime = time.Now()
	r.ctx = ctx
	return r
}
