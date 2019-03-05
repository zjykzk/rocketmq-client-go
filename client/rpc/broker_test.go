package rpc

import (
	"testing"
	"time"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/remote"
)

func TestBroker(t *testing.T) {
	t.Run("create topic", testCreateTopic)
	t.Run("delete topic", testDeleteTopic)
}

func testCreateTopic(t *testing.T) {
	client := remote.NewClient(remote.ClientConfig{}, nil, &log.MockLogger{})
	err := CreateOrUpdateTopic(client, "localhost:10909", &CreateOrUpdateTopicHeader{
		Topic:           "test_create_topic",
		DefaultTopic:    "default_topic",
		ReadQueueNums:   4,
		WriteQueueNums:  4,
		Perm:            0,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    0,
		Order:           false,
	}, time.Millisecond*100)
	t.Log(err)
}

func testDeleteTopic(t *testing.T) {
	client := remote.NewClient(remote.ClientConfig{}, nil, &log.MockLogger{})
	err := DeleteTopicInBroker(client, "localhost:10909", "test_create_topic", time.Millisecond*100)
	t.Log(err)
}
