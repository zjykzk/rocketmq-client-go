package client

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
)

// GetConsumerIDs get the client id from the broker wraper
func (c *MQClient) GetConsumerIDs(addr, group string, to time.Duration) (ids []string, err error) {
	return rpc.GetConsumerIDs(c.Client, addr, group, to)
}
