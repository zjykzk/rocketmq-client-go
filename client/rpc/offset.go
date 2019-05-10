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
		return 0, brokerError(cmd)
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
	cmd, err := client.RequestSync(addr, remote.NewCommand(updateConsumerOffset, h), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		return brokerError(cmd)
	}

	return nil
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
	err := client.RequestOneway(addr, remote.NewCommand(updateConsumerOffset, h))
	if err != nil {
		return requestError(err)
	}
	return nil
}

type resetConsumeOffsetHeader struct {
	topic     string
	group     string
	timestamp int64
	isForce   bool
}

func (h *resetConsumeOffsetHeader) ToMap() map[string]string {
	return map[string]string{
		"topic":     h.topic,
		"group":     h.group,
		"timestamp": strconv.FormatInt(h.timestamp, 10),
		"isForce":   strconv.FormatBool(h.isForce),
	}
}

// ResetConsumeOffset requests broker to reset the offsets of the specified topic, the offsets' owner
// is specified by the group
func ResetConsumeOffset(
	client remote.Client, addr, topic, group string, timestamp time.Time, isForce bool, timeout time.Duration,
) (
	[]byte, error,
) {
	h := &resetConsumeOffsetHeader{
		topic:     topic,
		group:     group,
		timestamp: timestamp.UnixNano() / int64(time.Millisecond),
		isForce:   isForce,
	}

	cmd, err := client.RequestSync(addr, remote.NewCommand(invokeBrokerToResetOffset, h), timeout)
	if err != nil {
		return nil, requestError(err)
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	return cmd.Body, nil
}
