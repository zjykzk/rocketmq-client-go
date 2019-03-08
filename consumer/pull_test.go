package consumer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestPullConsumer(t *testing.T) {
	logger := log.Std
	namesrvAddrs := []string{"10.200.20.54:9988", "10.200.20.25:9988"}
	c := NewPullConsumer("test-senddlt", namesrvAddrs, logger)
	c.Start()
	c.client = &fakeMQClient{}

	defer c.Shutdown()

	t.Run("sendback", func(t *testing.T) {
		testSendback(c, t)
	})

	t.Run("pullsync", func(t *testing.T) {
		testPullSync(c, t)
	})
}

func testSendback(c *PullConsumer, t *testing.T) {
	// bad id
	msgID := "bad message"
	assert.NotNil(t, c.SendBack(&message.Ext{MsgID: msgID}, -1, "", ""))

	// ok id
	msgID = "0AC8145700002A9F00000000006425A2"
	msg := &message.Ext{MsgID: msgID}
	err := c.SendBack(msg, -1, "", "")
	assert.Nil(t, err)

	// bad broker
	client := c.client.(*fakeMQClient)
	client.findBrokerAddrErr = errors.New("find broker failed")
	err = c.SendBack(msg, -1, "", "bad broker")
	assert.NotNil(t, err)
	client.findBrokerAddrErr = nil

	// ok broker
	err = c.SendBack(msg, -1, "", "bad broker")
	assert.Nil(t, err)
}

func testPullSync(c *PullConsumer, t *testing.T) {
	q := &message.Queue{}

	client := c.client.(*fakeMQClient)
	client.findBrokerAddrErr = errors.New("find broker failed")
	pr, err := c.PullSync(q, "", 0, 10)
	assert.Equal(t, client.findBrokerAddrErr, err)
	client.findBrokerAddrErr = nil

	pr, err = c.PullSync(q, "", 0, 10)
	assert.Equal(t, "fake pull error", err.Error())
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

	qs1 = []*message.Queue{
		{
			BrokerName: "b2",
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
			BrokerName: "b2",
		},
	}
	assert.False(t, messageQueueChanged(qs1, qs2))
}
