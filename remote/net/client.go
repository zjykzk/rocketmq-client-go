package net

import (
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go/log"
)

// Request request data
type Request interface {
	ID() int64
}

// Response response data
type Response interface {
	ID() int64
}

// Channel the tcp channel
type Channel interface {
	SendSync([]byte) error
}

// ErrorResponseCreater creates some errors
type ErrorResponseCreater interface {
	// creates connection error response.
	ConnError(id int64, err error) Response
	// creates timeout error response.
	TimeoutError(id int64) Response
	// creates connection closed error response.
	ConnClosed(id int64) Response
}

// Client exchange the message with server
type Client struct {
	// the processor for the request from the server
	RequestProcessor func(*ChannelContext, Response) bool

	chanLocker sync.RWMutex
	channels   map[string]*channel // key: addr

	futureLocker    sync.RWMutex
	responseFutures map[int64]*responseFuture

	decoder      Decoder
	encoder      Encoder
	packetReader PacketReader

	conf *Config

	exitChan chan struct{}
	wg       sync.WaitGroup

	logger log.Logger
}

// NewClient create the client
func NewClient(
	encoder Encoder, packetReader PacketReader, decoder Decoder, conf *Config, logger log.Logger,
) *Client {
	return &Client{
		channels:        make(map[string]*channel),
		responseFutures: make(map[int64]*responseFuture),
		encoder:         encoder,
		decoder:         decoder,
		packetReader:    packetReader,
		conf:            conf,
		logger:          logger,
	}
}

func (c *Client) getChannel(addr string) (ch *channel, err error) {
	c.chanLocker.RLock()
	ch, ok := c.channels[addr]
	c.chanLocker.RUnlock()

	if !ok || ch.getState() != StateConnected {
		c.chanLocker.Lock()
		ch, ok = c.channels[addr]
		if !ok {
			var err error
			c.logger.Infof("new channel to %s\n", addr)
			ch, err = newChannel(addr, c.encoder, c.packetReader, c.decoder, c, c.conf, c.logger)
			if err != nil {
				c.chanLocker.Unlock()
				return nil, err
			}
			c.channels[addr] = ch
		}
		c.chanLocker.Unlock()
	}

	return
}

// RequestSync request sync
func (c *Client) RequestSync(addr string, req Request, timeout time.Duration) (Response, error) {
	ch, err := c.getChannel(addr)
	if err != nil {
		return nil, err
	}

	future := c.putFuture(timeout, req.ID(), &ch.ctx)
	if err := ch.SendSync(req); err != nil {
		c.logger.Errorf("send message [%d] sync error:%v", req.ID(), err)
		return nil, err
	}
	c.logger.Debugf("send message [%d] ok, %s", req.ID(), addr)
	r, err := future.get()
	future.release()
	return r, err
}

// RequestOneway send the request and ignore the response
func (c *Client) RequestOneway(addr string, req Request) error {
	ch, err := c.getChannel(addr)
	if err != nil {
		return err
	}

	if err := ch.SendSync(req); err != nil {
		c.logger.Errorf("send message [%d] sync error:%v", req.ID(), err)
		return err
	}
	return nil
}

func (c *Client) putFuture(timeout time.Duration, id int64, ctx *ChannelContext) *responseFuture {
	f := newFuture(timeout, id, ctx)
	c.futureLocker.Lock()
	c.responseFutures[id] = f
	c.futureLocker.Unlock()
	return f
}

// OnActive callback when connected
func (c *Client) OnActive(ctx *ChannelContext) {
	c.logger.Infof("channel active:%s", ctx)
}

// OnDeactive callback when disconnected
func (c *Client) OnDeactive(ctx *ChannelContext) {
	c.logger.Infof("channel deactive:%s", ctx)
	c.clearChan(ctx, errConnDeactive)
}

// OnError callback when errors occurs
func (c *Client) OnError(ctx *ChannelContext, err error) {
	c.logger.Errorf("channel error:%s %s\n", ctx, err)
	c.clearChan(ctx, err)
}

// OnClose callback when closed
func (c *Client) OnClose(ctx *ChannelContext) {
	c.logger.Error("channel closed " + ctx.String())
	c.clearChan(ctx, errConnClosed)
}

func (c *Client) clearChan(ctx *ChannelContext, err error) {
	c.chanLocker.Lock()
	ch, ok := c.channels[ctx.Address]
	ok = ok && ctx == &ch.ctx
	if ok {
		delete(c.channels, ctx.Address)
	}
	c.chanLocker.Unlock()

	if ch != nil && &ch.ctx != ctx {
		c.logger.Errorf("wrong delete the connection, old:%s, new:%s", ctx, &ch.ctx)
	}

	if !ok {
		return
	}

	removedFutures := c.getFutures(func(f *responseFuture) bool { return f.ctx == ctx })
	c.removeFuturesOnError(removedFutures, err)
}

// OnMessage callback when received message
func (c *Client) OnMessage(ctx *ChannelContext, o interface{}) {
	resp := o.(Response)
	id := resp.ID()
	c.logger.Debugf("receive message [%d] of connection %s", id, ctx.String())
	if c.RequestProcessor != nil && c.RequestProcessor(ctx, resp) {
		c.logger.Debugf("[%d] processed by the default processor", id)
		return
	}

	c.futureLocker.Lock()
	f, ok := c.responseFutures[id]
	if ok {
		delete(c.responseFutures, f.id)
	}
	c.futureLocker.Unlock()

	if ok {
		f.put(resp)
	} else {
		c.logger.Errorf("message [%d] LOST: %v", id, o)
	}
	c.logger.Debugf("[%d] processed by response future", id)
}

// Start client's work
func (c *Client) Start() {
	c.exitChan = make(chan struct{})
	c.wg.Add(1)
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				removedFutures := c.getFutures(
					func(f *responseFuture) bool { return time.Since(f.startTime) > f.timeout },
				)
				c.removeFuturesOnError(removedFutures, errTimeout)
			case <-c.exitChan:
				c.wg.Done()
				ticker.Stop()
				return
			}
		}
	}()
}

// thread-safe
func (c *Client) getFutures(filter func(*responseFuture) bool) []*responseFuture {
	var removedFutures []*responseFuture
	c.futureLocker.RLock()
	for _, f := range c.responseFutures {
		if filter(f) {
			removedFutures = append(removedFutures, f)
		}
	}
	c.futureLocker.RUnlock()
	return removedFutures
}

// thread-safe
func (c *Client) removeFuturesOnError(futures []*responseFuture, err error) {
	for _, f := range futures {
		c.futureLocker.Lock()
		_, ok := c.responseFutures[f.id]
		if ok {
			delete(c.responseFutures, f.id)
		}
		c.futureLocker.Unlock()

		if !ok {
			continue
		}

		c.logger.Errorf(
			"message [%d], start %s, now %s, timeout:%s, error:%s",
			f.id, f.startTime, f.timeout, time.Now(), err,
		)

		f.err = err
		f.response <- nil
	}
}

// Shutdown client's work
func (c *Client) Shutdown() {
	c.logger.Info("shutdown net client")
	close(c.exitChan)
	for _, ch := range c.channels {
		ch.close()
		c.OnClose(&ch.ctx)
	}
	c.wg.Wait()
	c.logger.Info("shutdown net client END")
}
