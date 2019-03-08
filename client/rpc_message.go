package client

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/message"
)

// SendMessageSync send message to the broker
func (c *MQClient) SendMessageSync(
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

// PullMessageSync pull message sync
func (c *MQClient) PullMessageSync(addr string, header *rpc.PullHeader, to time.Duration) (
	pr *rpc.PullResponse, err error,
) {
	return rpc.PullMessageSync(c.Client, addr, header, to)
}

type queryMessageByIDHeader int64

func (h queryMessageByIDHeader) ToMap() map[string]string {
	return map[string]string{
		"offset": strconv.FormatInt(int64(h), 10),
	}
}

// QueryMessageByOffset querys the message by message id
func (c *MQClient) QueryMessageByOffset(addr string, offset int64, to time.Duration) (
	*message.Ext, error,
) {
	return rpc.ViewMessageByOffset(c.Client, addr, offset, to)
}

// SendBack send back the message
func (c *MQClient) SendBack(addr string, h *rpc.SendBackHeader, to time.Duration) (err error) {
	return rpc.SendBack(c.Client, addr, h, to)
}

type maxOffsetHeader struct {
	topic   string
	queueID uint8
}

func (h *maxOffsetHeader) ToMap() map[string]string {
	return map[string]string{
		"topic":   h.topic,
		"queueId": strconv.FormatInt(int64(h.queueID), 10),
	}
}

type maxOffsetResponse struct {
	Offset int64 `json:"offset"`
}

// MaxOffset returns the max offset in the consume queue
func (c *MQClient) MaxOffset(addr, topic string, queueID uint8, to time.Duration) (
	int64, *rpc.Error,
) {
	return rpc.MaxOffset(c.Client, addr, topic, queueID, to)
}

// SearchOffsetByTimestamp returns the offset of the specified message queue and the timestamp
func (c *MQClient) SearchOffsetByTimestamp(
	addr, topic string, queueID uint8, timestamp time.Time, to time.Duration,
) (
	int64, *rpc.Error,
) {
	return rpc.SearchOffsetByTimestamp(c.Client, addr, topic, queueID, timestamp, to)
}

// RegisterFilter register the filter to the broker
func (c *MQClient) RegisterFilter(group string, subData *SubscribeData) error {
	router := c.routersOfTopic.Get(subData.Topic)
	if router == nil {
		return nil
	}

	brokers := router.Brokers
	count := len(brokers)
	if count <= 0 {
		return nil
	}

	broker := brokers[rand.Intn(count)]
	return rpc.RegisterFilter(
		c.Client, broker.SelectAddress(), group, c.clientID, (*rpc.SubscribeData)(subData), time.Second*3,
	)
}

// PullMessageAsync pull message async
func (c *MQClient) PullMessageAsync(
	addr string, header *rpc.PullHeader, to time.Duration, callback func(*rpc.PullResponse, error),
) error {
	return rpc.PullMessageAsync(c.Client, addr, header, to, callback)
}
