package route

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoute(t *testing.T) {
	t0 := &TopicRouter{
		OrderTopicConf: "OrderTopicConf",
		Queues: []*TopicQueue{
			{
				BrokerName: "bn",
				ReadCount:  2,
				WriteCount: 2,
				Perm:       3,
				SyncFlag:   5,
			},
		},
		Brokers: []*Broker{
			{
				Cluster: "c",
				Name:    "n",
			},
		},
	}

	assert.True(t, t0.Equal(t0))
	t1 := &TopicRouter{
		OrderTopicConf: "OrderTopicConf1",
		Queues: []*TopicQueue{
			{
				BrokerName: "bn",
				ReadCount:  2,
				WriteCount: 2,
				Perm:       3,
				SyncFlag:   5,
			},
		},
		Brokers: []*Broker{
			{
				Cluster: "c",
				Name:    "n",
			},
		},
	}
	assert.False(t, t0.Equal(t1))
}
