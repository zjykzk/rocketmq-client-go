package consumer

import (
	"errors"
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type fakeMQClient struct {
	brokderAddr            string
	updateTopicRouterCount int

	sendBackAddr string
	pullCount    int

	maxOffset    int64
	maxOffsetErr *rpc.Error

	searchOffsetByTimestampRet int64
	searchOffsetByTimestampErr *rpc.Error

	getConsumerIDsErr error
	clientIDs         []string

	findBrokerAddrErr error
	findBrokerAddrRet client.FindBrokerResult

	pullHeader   *rpc.PullHeader
	pullAsyncErr error
}

func (c *fakeMQClient) RegisterConsumer(co client.Consumer) error { return nil }
func (c *fakeMQClient) UnregisterConsumer(group string)           {}
func (c *fakeMQClient) SendHeartbeat()                            {}
func (c *fakeMQClient) FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (
	*client.FindBrokerResult, error,
) {
	return &c.findBrokerAddrRet, c.findBrokerAddrErr
}

func (c *fakeMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	switch c.updateTopicRouterCount++; c.updateTopicRouterCount {
	case 1:
		return errors.New("fake update topic router info")
	default:
		c.brokderAddr = "ok"
	}
	return nil
}

func (c *fakeMQClient) GetConsumerIDs(addr, group string, to time.Duration) ([]string, error) {
	return c.clientIDs, c.getConsumerIDsErr
}
func (c *fakeMQClient) PullMessageSync(
	addr string, header *rpc.PullHeader, to time.Duration,
) (*rpc.PullResponse, error) {
	pr := &rpc.PullResponse{
		NextBeginOffset: 2,
		MinOffset:       1,
		MaxOffset:       3,
		Messages: []*message.Ext{
			&message.Ext{
				Message: message.Message{Properties: map[string]string{message.PropertyTags: "t1"}},
			},
			&message.Ext{
				Message: message.Message{Properties: map[string]string{message.PropertyTags: "t2"}},
			},
		},
		SuggestBrokerID: 123,
	}
	switch c.pullCount++; c.pullCount {
	case 1:
		return nil, errors.New("fake pull error")
	case 2:
		pr.Code = rpc.Success
	case 3:
		pr.Code = rpc.PullNotFound
	case 4:
		pr.Code = rpc.PullRetryImmediately
	case 5:
		pr.Code = rpc.PullOffsetMoved
	}
	return pr, nil
}
func (c *fakeMQClient) PullMessageAsync(
	addr string, header *rpc.PullHeader, to time.Duration, callback func(*rpc.PullResponse, error),
) error {
	c.pullHeader = header
	return c.pullAsyncErr
}
func (c *fakeMQClient) SendBack(addr string, h *rpc.SendBackHeader, to time.Duration) error {
	c.sendBackAddr = addr
	return nil
}

func (c *fakeMQClient) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	return nil
}

func (c *fakeMQClient) UpdateConsumerOffsetOneway(
	addr, topic, group string, queueID int, offset int64,
) error {
	return nil
}

func (c *fakeMQClient) QueryConsumerOffset(
	addr, topic, group string, queueID int, to time.Duration,
) (
	int64, *rpc.Error,
) {
	return 0, nil
}

func (c *fakeMQClient) MaxOffset(addr, topic string, queueID uint8, to time.Duration) (
	int64, *rpc.Error,
) {
	return c.maxOffset, c.maxOffsetErr
}
func (c *fakeMQClient) SearchOffsetByTimestamp(addr, topic string, queueID uint8, timestamp time.Time, to time.Duration) (
	int64, *rpc.Error,
) {
	return c.searchOffsetByTimestampRet, c.searchOffsetByTimestampErr
}
func (c *fakeMQClient) RegisterFilter(group string, subData *client.SubscribeData) error {
	return nil
}
