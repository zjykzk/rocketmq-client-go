package client

import (
	"errors"
	"time"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
)

// SendMessageSync send message to the broker
func (c *MqClient) SendMessageSync(
	broker string, data []byte, header *rpc.SendHeader, timeout time.Duration,
) (
	*rpc.SendResponse, error,
) {
	addr := c.GetMasterBrokerAddr(broker)
	if addr == "" {
		c.logger.Errorf("cannot find broker:%s", broker)
		return nil, errors.New("cannot find broker")
	}

	return rpc.SendMessageSync(c.Client, addr, data, header, timeout)
}
