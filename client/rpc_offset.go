package client

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/message"
)

// QueryConsumerOffset query cosnume offset wraper
func (c *MQClient) QueryConsumerOffset(addr, topic, group string, queueID int, timeout time.Duration) (
	int64, *rpc.Error,
) {
	return rpc.QueryConsumerOffset(c.Client, addr, topic, group, queueID, timeout)
}

// UpdateConsumerOffsetOneway updates the offset of the consumer queue wraper
func (c *MQClient) UpdateConsumerOffsetOneway(
	addr, topic, group string, queueID int, offset int64,
) error {
	return rpc.UpdateConsumerOffsetOneway(c.Client, addr, topic, group, queueID, offset)
}

// UpdateConsumerOffset updates the offset of the consumer queue wraper
func (c *MQClient) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, timeout time.Duration,
) error {
	return rpc.UpdateConsumerOffset(c.Client, addr, topic, group, queueID, offset, timeout)
}

// ResetConsumeOffset requests the broker to reset the offsets of the specified topic, the offsets' owner
// is specified by the group
func (c *MQClient) ResetConsumeOffset(
	addr, topic, group string, timestamp time.Time, isForce bool, timeout time.Duration,
) (
	map[message.Queue]int64, error,
) {
	data, err := rpc.ResetConsumeOffset(c.Client, addr, topic, group, timestamp, isForce, timeout)
	if err != nil {
		return nil, err
	}

	return parseResetOffsetRequest(string(data))
}
