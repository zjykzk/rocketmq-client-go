package rpc

import (
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

// CreateOrUpdateTopicHeader create topic params
type CreateOrUpdateTopicHeader struct {
	Topic           string
	DefaultTopic    string
	ReadQueueNums   int32
	WriteQueueNums  int32
	Perm            int32
	TopicFilterType string
	TopicSysFlag    int32
	Order           bool
}

// ToMap serializes to the map
func (h *CreateOrUpdateTopicHeader) ToMap() map[string]string {
	return map[string]string{
		"topic":           h.Topic,
		"defaultTopic":    h.DefaultTopic,
		"readQueueNums":   strconv.FormatInt(int64(h.ReadQueueNums), 10),
		"writeQueueNums":  strconv.FormatInt(int64(h.WriteQueueNums), 10),
		"perm":            strconv.FormatInt(int64(h.Perm), 10),
		"topicFilterType": h.TopicFilterType,
		"topicSysFlag":    strconv.FormatInt(int64(h.TopicSysFlag), 10),
		"order":           strconv.FormatBool(h.Order),
	}
}

// CreateOrUpdateTopic create topic from broker
func CreateOrUpdateTopic(client remote.Client, addr string, header *CreateOrUpdateTopicHeader, to time.Duration) (
	err error,
) {
	cmd, err := client.RequestSync(addr, remote.NewCommand(UpdateAndCreateTopic, header), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
}

type deleteTopicHeader string

func (h deleteTopicHeader) ToMap() map[string]string {
	return map[string]string{"topic": string(h)}
}

// DeleteTopicInBroker delete topic in the broker
func DeleteTopicInBroker(client remote.Client, addr, topic string, to time.Duration) (err error) {
	h := deleteTopicHeader(topic)
	cmd, err := client.RequestSync(addr, remote.NewCommand(DeleteTopicInBrokerCode, h), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
}
