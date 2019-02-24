package remote

import (
	"encoding/json"
	"strconv"
	"time"
)

// BrokerRuntimeInfo returns the broker runtime information.
func (r *RPC) BrokerRuntimeInfo(addr string, timeout time.Duration) (
	map[string]string, error,
) {
	cmd, err := r.client.RequestSync(addr,
		NewCommand(GetBrokerRuntimeInfo, nil),
		timeout)

	if err != nil {
		return nil, err
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	var ret map[string]map[string]string

	err = json.Unmarshal(cmd.Body, &ret)

	if err != nil {
		return nil, err
	}

	return ret["table"], nil
}

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
func (r *RPC) CreateOrUpdateTopic(addr string, header *CreateOrUpdateTopicHeader, to time.Duration) (
	err error,
) {
	cmd, err := r.client.RequestSync(
		addr,
		NewCommand(UpdateAndCreateTopic, header),
		to)
	if err != nil {
		return
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
func (r *RPC) DeleteTopicInBroker(addr, topic string, to time.Duration) (err error) {
	h := deleteTopicHeader(topic)
	cmd, err := r.client.RequestSync(addr, NewCommand(DeleteTopicInBroker, h), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
}

type getConsumerIDsHeader string

func (h getConsumerIDsHeader) ToMap() map[string]string {
	return map[string]string{"consumerGroup": string(h)}
}

// GetConsumerIDs get the client id from the broker
func (r *RPC) GetConsumerIDs(addr, group string, to time.Duration) (
	ids []string, err error,
) {
	g := getConsumerIDsHeader(group)
	cmd, err := r.client.RequestSync(addr, NewCommand(GetConsumerListByGroup, g), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
		return
	}

	if len(cmd.Body) == 0 {
		return
	}

	rp := &struct {
		IDs []string `json:"consumerIdList"`
	}{}
	err = json.Unmarshal(cmd.Body, rp)
	if err == nil {
		ids = rp.IDs
	}
	return
}

type unregisterClientHeader struct {
	clientID      string
	producerGroup string
	consumerGroup string
}

func (uh *unregisterClientHeader) ToMap() map[string]string {
	ret := map[string]string{
		"clientID": uh.clientID,
	}

	if uh.producerGroup != "" {
		ret["producerGroup"] = uh.producerGroup
	}

	if uh.consumerGroup != "" {
		ret["consumerGroup"] = uh.consumerGroup
	}

	return ret
}

// UnregisterClient unregister the producer/consumer group from broker
func (r *RPC) UnregisterClient(addr, clientID, pGroup, cGroup string, to time.Duration) (
	err error,
) {
	h := &unregisterClientHeader{
		clientID:      clientID,
		producerGroup: pGroup,
		consumerGroup: cGroup,
	}
	cmd, err := r.client.RequestSync(addr, NewCommand(UnregisterClient, h), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
}

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
func (r *RPC) QueryConsumerOffset(addr, topic, group string, queueID int, to time.Duration) (
	offset int64, err error,
) {
	h := &queryConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
	}
	cmd, err := r.client.RequestSync(addr, NewCommand(QueryConsumerOffset, h), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	offset, err = strconv.ParseInt(cmd.ExtFields["offset"], 10, 64)
	return
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
func (r *RPC) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	h := &updateConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
		offset:  offset,
	}
	_, err := r.client.RequestSync(addr, NewCommand(UpdateConsumerOffset, h), to)
	return err
}

// UpdateConsumerOffsetOneway updates the offset of the consumer queue
func (r *RPC) UpdateConsumerOffsetOneway(addr, topic, group string, queueID int, offset int64) error {
	h := &updateConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
		offset:  offset,
	}
	return r.client.RequestOneway(addr, NewCommand(UpdateConsumerOffset, h))
}
