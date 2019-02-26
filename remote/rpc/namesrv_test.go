package rpc

import (
	"testing"
	"time"

	"github.com/zjykzk/rocketmq-client-go/log"
)

func testGetTopicRouteInfo(t *testing.T) {
	rpc := NewRPC(NewClient(&ClientConfig{}, nil, &log.MockLogger{}))
	//topic, err := getTopicRouteInfo("localhost:9876", c, "Perform_topic_1", time.Second*10)
	topic, err := rpc.GetTopicRouteInfo("10.200.20.70:9876", "Perform_topic_1", time.Second*10)
	t.Log(topic, err)
}
