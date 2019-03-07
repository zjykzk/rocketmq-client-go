package remote

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
)

type fakeRequestProcessor struct {
	processRet bool
	runProcess bool
}

func (f *fakeRequestProcessor) process(*ChannelContext, *Command) bool {
	f.runProcess = true
	return f.processRet
}

type fakeConn struct {
	writeErr error
}

func (c *fakeConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (c *fakeConn) Write(b []byte) (n int, err error)  { return 0, c.writeErr }
func (c *fakeConn) Close() error                       { return nil }
func (c *fakeConn) LocalAddr() net.Addr                { return fakeAddr{} }
func (c *fakeConn) RemoteAddr() net.Addr               { return fakeAddr{} }
func (c *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeAddr struct{}

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return "fake addr" }

type fakeEncoder struct {
	encodeRet []byte
	encodeErr error
}

func (e *fakeEncoder) Encode(*Command) ([]byte, error) {
	return e.encodeRet, e.encodeErr
}

func newTestClient() (*client, *fakeRequestProcessor) {
	requestProcessor := &fakeRequestProcessor{}
	rc, err := NewClient(ClientConfig{}, requestProcessor.process, log.Std)
	if err != nil {
		panic(err)
	}
	return rc.(*client), requestProcessor
}

func TestOnMessage(t *testing.T) {
	c, requestProcessor := newTestClient()
	ctx, cmd := &ChannelContext{Conn: &fakeConn{}}, &Command{}

	// process by reuest processor
	requestProcessor.processRet = true
	c.OnMessage(ctx, cmd)
	assert.True(t, requestProcessor.runProcess)
	requestProcessor.processRet = false

	// put the response cmd
	f := c.putFuture(time.Second, cmd.ID(), ctx)
	c.OnMessage(ctx, cmd)

	cmd1, err := f.get()
	assert.Nil(t, err)
	assert.Equal(t, cmd, cmd1)

	// callback
	runCallback := make(chan struct{})
	f = c.putFuture(time.Second, cmd.ID(), ctx)
	f.callback = func(*Command, error) { close(runCallback) }
	c.OnMessage(ctx, cmd)
	<-runCallback
}

func fakeChannel(h Handler) *channel {
	return &channel{
		ctx:           ChannelContext{Conn: &fakeConn{}},
		ChannelConfig: ChannelConfig{Encoder: &fakeEncoder{}, logger: log.Std, Handler: h},
	}
}

func TestRequestAsync(t *testing.T) {
	c, _ := newTestClient()

	cmd := &Command{}
	callback := func(*Command, error) {}

	// bad channel
	err := c.RequestAsync("bad addr", cmd, time.Second, callback)
	assert.NotNil(t, err)

	// send failed
	ch := fakeChannel(c)
	c.channels["fake"] = ch
	conn := ch.ctx.Conn.(*fakeConn)
	conn.writeErr = errors.New("bad write")
	err = c.RequestAsync("fake", cmd, time.Second, callback)
	assert.NotNil(t, err)
	conn.writeErr = nil

	// OK
	err = c.RequestAsync("fake", cmd, time.Second, callback)
	assert.Nil(t, err)
}
