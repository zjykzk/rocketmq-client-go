package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
)

func TestPullConsumer(t *testing.T) {
	logger := &log.MockLogger{}
	namesrvAddrs := []string{"10.200.20.54:9988", "10.200.20.25:9988"}
	c := NewPullConsumer("test-senddlt", namesrvAddrs, logger)
	c.Start()
	c.rpc = &mockConsumerRPC{}
	c.client = &mockMQClient{}

	defer c.Shutdown()

	t.Run("sendback", func(t *testing.T) {
		testSendback(c, t)
	})

	t.Run("pullsync", func(t *testing.T) {
		testPullSync(c, t)
	})
}

type mockMQClient struct {
	*client.EmptyMQClient
	brokderAddr            string
	updateTopicRouterCount int
}

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

type mockConsumerRPC struct {
	sendBackAddr string
	pullCount    int

	maxOffset    int64
	maxOffsetErr *remote.RPCError

	searchOffsetByTimestampRet int64
	searchOffsetByTimestampErr *remote.RPCError

	getConsumerIDsErr error
	clientIDs         []string
}

func (r *mockConsumerRPC) GetConsumerIDs(addr, group string, to time.Duration) ([]string, error) {
	return r.clientIDs, r.getConsumerIDsErr
}
func (r *mockConsumerRPC) PullMessageSync(
	addr string, header *remote.PullHeader, to time.Duration,
) (*remote.PullResponse, error) {
	pr := &remote.PullResponse{
		NextBeginOffset: 2,
		MinOffset:       1,
		MaxOffset:       3,
		Messages: []*message.MessageExt{
			&message.MessageExt{
				Message: message.Message{Properties: map[string]string{message.PropertyTags: "t1"}},
			},
			&message.MessageExt{
				Message: message.Message{Properties: map[string]string{message.PropertyTags: "t2"}},
			},
		},
		SuggestBrokerID: 123,
	}
	switch r.pullCount++; r.pullCount {
	case 1:
		return nil, errors.New("mock pull error")
	case 2:
		pr.Code = remote.Success
	case 3:
		pr.Code = remote.PullNotFound
	case 4:
		pr.Code = remote.PullRetryImmediately
	case 5:
		pr.Code = remote.PullOffsetMoved
	}
	return pr, nil
}
func (r *mockConsumerRPC) SendBack(addr string, h *remote.SendBackHeader, to time.Duration) error {
	r.sendBackAddr = addr
	return nil
}

func (r *mockConsumerRPC) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	return nil
}

func (r *mockConsumerRPC) UpdateConsumerOffsetOneway(
	addr, topic, group string, queueID int, offset int64,
) error {
	return nil
}

func (r *mockConsumerRPC) QueryConsumerOffset(
	addr, topic, group string, queueID int, to time.Duration,
) (
	int64, error,
) {
	return 0, nil
}

func (r *mockConsumerRPC) MaxOffset(addr, topic string, queueID uint8, to time.Duration) (
	int64, *remote.RPCError,
) {
	return r.maxOffset, r.maxOffsetErr
}
func (r *mockConsumerRPC) SearchOffsetByTimestamp(addr, broker, topic string, queueID uint8, timestamp time.Time, to time.Duration) (
	int64, *remote.RPCError,
) {
	return r.searchOffsetByTimestampRet, r.searchOffsetByTimestampErr
}

func testSendback(c *PullConsumer, t *testing.T) {
	msgID := "bad message"
	assert.NotNil(t, c.SendBack(&message.MessageExt{MsgID: msgID}, -1, "", ""))

	msgID = "0AC8145700002A9F00000000006425A2"
	msg := &message.MessageExt{MsgID: msgID}
	err := c.SendBack(msg, -1, "", "mock addr")
	assert.Nil(t, err)
}

func testPullSync(c *PullConsumer, t *testing.T) {
	q := &message.Queue{}
	pr, err := c.PullSync(q, "", 0, 10)
	assert.Equal(t, "mock find broker addr error", err.Error())
	pr, err = c.PullSync(q, "", 0, 10)
	assert.Equal(t, "mock pull error", err.Error())
	pr, err = c.PullSync(q, "", 0, 10)
	assert.Nil(t, err)
	id, exist := c.brokerSuggester.get(q)
	assert.True(t, exist)
	assert.Equal(t, int32(123), id)

	pr, err = c.PullSync(q, "", 0, 10)
	assert.Equal(t, NoNewMessage, pr.Status)

	pr, err = c.PullSync(q, "t2", 0, 10)
	assert.Equal(t, NoMatchedMessage, pr.Status)

	pr, err = c.PullSync(q, "t1||t2", 0, 10)
	assert.Equal(t, OffsetIllegal, pr.Status)

	pr, err = c.PullSync(q, "t1", 0, 10)
	assert.Equal(t, 1, len(pr.Messages))

	pr, err = c.PullSync(q, "t2", 0, 10)
	assert.Equal(t, 1, len(pr.Messages))

	pr, err = c.PullSync(q, "t1||t2", 0, 10)
	assert.Equal(t, 2, len(pr.Messages))
}

func TestMessageQueueChanged(t *testing.T) {
	qs1 := []*message.Queue{
		{
			BrokerName: "b1",
		},
		{
			BrokerName: "b12",
		},
	}
	qs2 := []*message.Queue{
		{
			BrokerName: "b1",
		},
	}
	assert.True(t, messageQueueChanged(qs1, qs2))

	qs1 = []*message.Queue{
		{
			BrokerName: "b1",
		},
	}
	qs2 = []*message.Queue{
		{
			BrokerName: "b1",
		},
		{
			BrokerName: "b12",
		},
	}
	assert.True(t, messageQueueChanged(qs1, qs2))

	qs1 = []*message.Queue{
		{
			BrokerName: "b1",
		},
		{
			BrokerName: "b1",
		},
	}
	qs2 = []*message.Queue{
		{
			BrokerName: "b1",
		},
		{
			BrokerName: "b1",
		},
	}
	assert.False(t, messageQueueChanged(qs1, qs2))
}
