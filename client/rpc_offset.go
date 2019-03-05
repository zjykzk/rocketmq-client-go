package client

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
)

// QueryConsumerOffset query cosnume offset wraper
func (c *MqClient) QueryConsumerOffset(addr, topic, group string, queueID int, to time.Duration) (
	int64, *rpc.Error,
) {
	return rpc.QueryConsumerOffset(c.Client, addr, topic, group, queueID, to)
}

// UpdateConsumerOffsetOneway updates the offset of the consumer queue wraper
func (c *MqClient) UpdateConsumerOffsetOneway(
	addr, topic, group string, queueID int, offset int64,
) error {
	return rpc.UpdateConsumerOffsetOneway(c.Client, addr, topic, group, queueID, offset)
}

// UpdateConsumerOffset updates the offset of the consumer queue wraper
func (c *MqClient) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	return rpc.UpdateConsumerOffset(c.Client, addr, topic, group, queueID, offset, to)
}
