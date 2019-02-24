package remote

import (
	"os"
	"testing"
	"time"

	"github.com/qiniu/log.v1"
	"github.com/zjykzk/rocketmq-client-go/remote/net"
)

func testGetTopicRouteInfo(t *testing.T) {
	rpc := NewRPC(NewClient(&net.Config{}, nil, log.New(os.Stderr, "", log.Ldefault)))
	//topic, err := getTopicRouteInfo("localhost:9876", c, "Perform_topic_1", time.Second*10)
	topic, err := rpc.GetTopicRouteInfo("10.200.20.70:9876", "Perform_topic_1", time.Second*10)
	t.Log(topic, err)
}
