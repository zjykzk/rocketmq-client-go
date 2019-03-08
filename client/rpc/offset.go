package rpc

import (
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

type queryConsumerOffsetRequestHeader struct {
	group   string
	topic   string
	queueID int
}

func (qo *queryConsumerOffsetRequestHeader) ToMap() map[string]string {
	return map[string]string{
		"consumerGroup": qo.group,
		"topic":         qo.topic,
		"queueId":       strconv.Itoa(qo.queueID),
	}
}

// QueryConsumerOffset returns the offset of the specified topic and group
func QueryConsumerOffset(
	client remote.Client, addr, topic, group string, queueID int, to time.Duration,
) (
	int64, *Error,
) {
	h := &queryConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
	}
	cmd, err := client.RequestSync(addr, remote.NewCommand(queryConsumerOffset, h), to)
	if err != nil {
		return 0, requestError(err)
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	offset, err := strconv.ParseInt(cmd.ExtFields["offset"], 10, 64)
	if err != nil {
		return 0, dataError(err)
	}
	return offset, nil
}

type updateConsumerOffsetRequestHeader struct {
	group   string
	topic   string
	queueID int
	offset  int64
}

func (uo *updateConsumerOffsetRequestHeader) ToMap() map[string]string {
	return map[string]string{
		"consumerGroup": uo.group,
		"topic":         uo.topic,
		"queueId":       strconv.Itoa(uo.queueID),
		"commitOffset":  strconv.FormatInt(uo.offset, 10),
	}
}

// UpdateConsumerOffset updates the offset of the consumer queue
func UpdateConsumerOffset(
	client remote.Client, addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	h := &updateConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
		offset:  offset,
	}
	_, err := client.RequestSync(addr, remote.NewCommand(updateConsumerOffset, h), to)
	return err
}

// UpdateConsumerOffsetOneway updates the offset of the consumer queue
func UpdateConsumerOffsetOneway(
	client remote.Client, addr, topic, group string, queueID int, offset int64,
) error {
	h := &updateConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
		offset:  offset,
	}
	return client.RequestOneway(addr, remote.NewCommand(updateConsumerOffset, h))
}
