package rpc

import (
	"testing"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

func testGetTopicRouteInfo(t *testing.T) {
	//topic, err := getTopicRouteInfo("localhost:9876", c, "Perform_topic_1", time.Second*10)
	topic, err := GetTopicRouteInfo(&remote.MockClient{}, "10.200.20.70:9876", "Perform_topic_1", time.Second*10)
	t.Log(topic, err)
}
