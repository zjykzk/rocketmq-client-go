package remote

import (
	"errors"
	"time"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/remote/net"
)

// Client exchange the message with server
type Client interface {
	RequestSync(addr string, cmd *Command, timeout time.Duration) (*Command, error)
	RequestOneway(addr string, cmd *Command) error
	Start() error
	Shutdown()
}

type client struct {
	client           *net.Client
	requestProcessor func(*net.ChannelContext, *Command) bool
}

// ErrBadNamesrvAddrs bad name server address
var ErrBadNamesrvAddrs = errors.New("bad name server address")

// NewClient create the client
func NewClient(
	conf *net.Config, rp func(*net.ChannelContext, *Command) bool, logger log.Logger,
) Client {
	c := &client{requestProcessor: rp}
	c.client = net.NewClient(
		net.EncoderFunc(Encode),
		net.PacketReaderFunc(ReadPacket),
		net.DecoderFunc(Decode),
		conf,
		logger,
	)
	c.client.RequestProcessor = c.processRequest
	return c
}

// RequestSync request the command sync
func (c *client) RequestSync(addr string, cmd *Command, timeout time.Duration) (
	*Command, error,
) {
	resp, err := c.client.RequestSync(addr, cmd, timeout)
	if err != nil {
		return nil, err
	}
	return resp.(*Command), err
}

func (c *client) RequestOneway(addr string, cmd *Command) error {
	return c.client.RequestOneway(addr, cmd)
}

func (c *client) processRequest(ctx *net.ChannelContext, resp net.Response) bool {
	return c.requestProcessor(ctx, resp.(*Command))
}

// Start start the client
func (c *client) Start() error {
	c.client.Start()
	return nil
}

// Shutdown shutdown the client
func (c *client) Shutdown() {
	c.client.Shutdown()
}
