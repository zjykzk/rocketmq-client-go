package client

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// DeleteTopicInNamesrv delete topic in the broker
func (c *MQClient) DeleteTopicInNamesrv(addr, topic string, to time.Duration) error {
	return rpc.DeleteTopicInNamesrv(c.Client, addr, topic, to)
}

// GetBrokerClusterInfo get the cluster info from the namesrv
func (c *MQClient) GetBrokerClusterInfo(addr string, to time.Duration) (*route.ClusterInfo, error) {
	return rpc.GetBrokerClusterInfo(c.Client, addr, to)
}
