package remote

import (
	"errors"
	"sync"
	"time"

	"github.com/zjykzk/rocketmq-client-go/executor"
	"github.com/zjykzk/rocketmq-client-go/log"
)

// Client exchange the message with server
type Client interface {
	RequestSync(addr string, cmd *Command, timeout time.Duration) (*Command, error)
	RequestAsync(addr string, cmd *Command, timeout time.Duration, callback func(*Command, error)) error
	RequestOneway(addr string, cmd *Command) error
	Start() error
	Shutdown()
}

type client struct {
	requestProcessor func(*ChannelContext, *Command) bool

	chanLocker sync.RWMutex
	channels   map[string]*channel // key: addr

	futureLocker    sync.RWMutex
	responseFutures map[int64]*responseFuture

	decoder      Decoder
	encoder      Encoder
	packetReader PacketReader

	executor *executor.GoroutinePoolExecutor

	conf ClientConfig

	exitChan chan struct{}
	wg       sync.WaitGroup

	logger log.Logger
}

// ClientConfig timeout configuration
type ClientConfig struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DialTimeout  time.Duration
}

// NewClient create the client
func NewClient(
	conf ClientConfig, rp func(*ChannelContext, *Command) bool, logger log.Logger,
) (
	Client, error,
) {
	if logger == nil {
		return nil, errors.New("new remote client error:empty logger")
	}

	executor, err := executor.NewPoolExecutor(
		"remote-client", 8, 8, time.Hour, executor.NewLinkedBlockingQueue(),
	)
	if err != nil {
		return nil, err
	}

	c := &client{
		requestProcessor: rp,
		conf:             conf,
		encoder:          EncoderFunc(encode),
		decoder:          DecoderFunc(decode),
		packetReader:     PacketReaderFunc(ReadPacket),
		executor:         executor,
		logger:           logger,
		channels:         make(map[string]*channel, 16),
		responseFutures:  make(map[int64]*responseFuture, 256),
	}
	return c, nil
}

// RequestSync request the command sync
func (c *client) RequestSync(addr string, cmd *Command, timeout time.Duration) (
	*Command, error,
) {
	ch, err := c.getChannel(addr)
	if err != nil {
		return nil, err
	}

	future := c.putFuture(timeout, cmd.ID(), &ch.ctx)
	if err := ch.SendSync(cmd); err != nil {
		c.logger.Errorf("send message [%d] sync error:%v", cmd.ID(), err)
		return nil, err
	}
	c.logger.Debugf("send message [%d] ok, %s", cmd.ID(), addr)
	r, err := future.get()
	future.release()
	return r, err
}

// RequestAsync request the command async
func (c *client) RequestAsync(
	addr string, cmd *Command, timeout time.Duration, callback func(*Command, error),
) error {
	ch, err := c.getChannel(addr)
	if err != nil {
		return err
	}

	c.putCallbackFuture(timeout, cmd.ID(), &ch.ctx, callback)
	if err := ch.SendSync(cmd); err != nil {
		c.logger.Errorf("send message [%d] sync error:%v", cmd.ID(), err)
		return err
	}
	return nil
}

func (c *client) putFuture(timeout time.Duration, id int64, ctx *ChannelContext) *responseFuture {
	return c.putCallbackFuture(timeout, id, ctx, nil)
}

func (c *client) putCallbackFuture(
	timeout time.Duration, id int64, ctx *ChannelContext, callback func(*Command, error),
) *responseFuture {
	f := newFuture(timeout, id, ctx, callback)
	c.futureLocker.Lock()
	c.responseFutures[id] = f
	c.futureLocker.Unlock()
	return f
}
func (c *client) RequestOneway(addr string, cmd *Command) error {
	ch, err := c.getChannel(addr)
	if err != nil {
		return err
	}

	if err := ch.SendSync(cmd); err != nil {
		c.logger.Errorf("send message [%d] sync error:%v", cmd.ID(), err)
		return err
	}
	return nil
}

// OnActive callback when connected
func (c *client) OnActive(ctx *ChannelContext) {
	c.logger.Infof("channel active:%s", ctx)
}

// OnDeactive callback when disconnected
func (c *client) OnDeactive(ctx *ChannelContext) {
	c.logger.Infof("channel deactive:%s", ctx)
	c.clearChan(ctx, errConnDeactive)
}

// OnError callback when errors occurs
func (c *client) OnError(ctx *ChannelContext, err error) {
	c.logger.Errorf("channel error:%s %s\n", ctx, err)
	c.clearChan(ctx, err)
}

// OnClose callback when closed
func (c *client) OnClose(ctx *ChannelContext) {
	c.logger.Error("channel closed " + ctx.String())
	c.clearChan(ctx, errConnClosed)
}

func (c *client) clearChan(ctx *ChannelContext, err error) {
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
func (c *client) OnMessage(ctx *ChannelContext, cmd *Command) {
	id := cmd.ID()
	c.logger.Debugf("receive message [%d] of connection %s", id, ctx.String())
	if c.requestProcessor != nil && c.requestProcessor(ctx, cmd) {
		c.logger.Debugf("[%d] processed by the default processor", id)
		return
	}

	c.futureLocker.Lock()
	f, ok := c.responseFutures[id]
	if ok {
		delete(c.responseFutures, f.id)
	}
	c.futureLocker.Unlock()

	c.logger.Debugf("[%d] processed by response future", id)
	if !ok {
		c.logger.Errorf("message [%d] LOST: %v", id, cmd)
		return
	}

	c.processResponseFuture(f, cmd)
}

func (c *client) processResponseFuture(f *responseFuture, cmd *Command) {
	if f.callback != nil {
		c.executor.Execute(callback{cmd: cmd, err: f.err, f: f.callback})
	} else {
		f.put(cmd)
	}
}

type callback struct {
	cmd *Command
	err error
	f   func(*Command, error)
}

func (c callback) Run() {
	c.f(c.cmd, c.err)
}

// Start client's work
func (c *client) Start() error {
	c.exitChan = make(chan struct{})
	c.wg.Add(1)
	go c.cleanExpiredFuturesPeriod()
	return nil
}

func (c *client) cleanExpiredFuturesPeriod() {
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
}

// thread-safe
func (c *client) getFutures(filter func(*responseFuture) bool) []*responseFuture {
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
func (c *client) removeFuturesOnError(futures []*responseFuture, err error) {
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
		c.processResponseFuture(f, nil)
	}
}

// Shutdown client's work
func (c *client) Shutdown() {
	c.logger.Info("shutdown remote client START")
	close(c.exitChan)
	for _, ch := range c.channels {
		ch.close()
		c.OnClose(&ch.ctx)
	}
	c.wg.Wait()
	c.executor.Shutdown()
	c.logger.Info("shutdown remote client END")
}

func (c *client) getChannel(addr string) (ch *channel, err error) {
	c.chanLocker.RLock()
	ch, ok := c.channels[addr]
	c.chanLocker.RUnlock()

	if !ok || ch.getState() != StateConnected {
		c.chanLocker.Lock()
		ch, ok = c.channels[addr]
		if !ok {
			var err error
			c.logger.Infof("new channel to %s\n", addr)
			ch, err = newChannel(addr, ChannelConfig{
				ClientConfig: c.conf,
				Encoder:      c.encoder,
				PacketReader: c.packetReader,
				Decoder:      c.decoder,
				Handler:      c,
				logger:       c.logger,
			})
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
