package consumer

import (
	"errors"
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/message"
)

type mockMQClient struct {
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
}

func (c *mockMQClient) Start() error                              { return nil }
func (c *mockMQClient) Shutdown()                                 {}
func (c *mockMQClient) RegisterConsumer(co client.Consumer) error { return nil }
func (c *mockMQClient) UnregisterConsumer(group string)           {}
func (c *mockMQClient) SendHeartbeat()                            {}
func (c *mockMQClient) FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (
	*client.FindBrokerResult, error,
) {
	if c.brokderAddr != "" {
		return &client.FindBrokerResult{
			Addr:    c.brokderAddr,
			IsSlave: true,
		}, nil
	}
	return nil, errors.New("mock find broker addr error")
}

func (c *mockMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	switch c.updateTopicRouterCount++; c.updateTopicRouterCount {
	case 1:
		return errors.New("mock update topic router info")
	default:
		c.brokderAddr = "ok"
	}
	return nil
}

func (c *mockMQClient) GetConsumerIDs(addr, group string, to time.Duration) ([]string, error) {
	return c.clientIDs, c.getConsumerIDsErr
}
func (c *mockMQClient) PullMessageSync(
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
		return nil, errors.New("mock pull error")
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
func (c *mockMQClient) SendBack(addr string, h *rpc.SendBackHeader, to time.Duration) error {
	c.sendBackAddr = addr
	return nil
}

func (c *mockMQClient) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	return nil
}

func (c *mockMQClient) UpdateConsumerOffsetOneway(
	addr, topic, group string, queueID int, offset int64,
) error {
	return nil
}

func (c *mockMQClient) QueryConsumerOffset(
	addr, topic, group string, queueID int, to time.Duration,
) (
	int64, *rpc.Error,
) {
	return 0, nil
}

func (c *mockMQClient) MaxOffset(addr, topic string, queueID uint8, to time.Duration) (
	int64, *rpc.Error,
) {
	return c.maxOffset, c.maxOffsetErr
}
func (c *mockMQClient) SearchOffsetByTimestamp(addr, topic string, queueID uint8, timestamp time.Time, to time.Duration) (
	int64, *rpc.Error,
) {
	return c.searchOffsetByTimestampRet, c.searchOffsetByTimestampErr
}
func (c *mockMQClient) RegisterFilter(group string, subData *client.SubscribeData) error {
	return nil
}
