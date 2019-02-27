package rpc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

// BrokerRuntimeInfo returns the broker runtime information.
func (r *RPC) BrokerRuntimeInfo(addr string, timeout time.Duration) (
	map[string]string, error,
) {
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(GetBrokerRuntimeInfo, nil), timeout)

	if err != nil {
		return nil, remote.RequestError(err)
	}

	if cmd.Code != Success {
		return nil, remote.BrokerError(cmd)
	}

	var ret map[string]map[string]string

	err = json.Unmarshal(cmd.Body, &ret)

	if err != nil {
		return nil, remote.DataError(err)
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
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(UpdateAndCreateTopic, header), to)
	if err != nil {
		return remote.RequestError(err)
	}

	if cmd.Code != Success {
		err = remote.BrokerError(cmd)
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
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(DeleteTopicInBroker, h), to)
	if err != nil {
		return remote.RequestError(err)
	}

	if cmd.Code != Success {
		err = remote.BrokerError(cmd)
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
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(GetConsumerListByGroup, g), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = remote.BrokerError(cmd)
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
	} else {
		err = remote.DataError(err)
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
	int64, *remote.RPCError,
) {
	h := &queryConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
	}
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(QueryConsumerOffset, h), to)
	if err != nil {
		return 0, remote.RequestError(err)
	}

	if cmd.Code != Success {
		err = remote.BrokerError(cmd)
	}
	offset, err := strconv.ParseInt(cmd.ExtFields["offset"], 10, 64)
	if err != nil {
		return 0, remote.DataError(err)
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
func (r *RPC) UpdateConsumerOffset(
	addr, topic, group string, queueID int, offset int64, to time.Duration,
) error {
	h := &updateConsumerOffsetRequestHeader{
		group:   group,
		topic:   topic,
		queueID: queueID,
		offset:  offset,
	}
	_, err := r.client.RequestSync(addr, remote.NewCommand(UpdateConsumerOffset, h), to)
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
	return r.client.RequestOneway(addr, remote.NewCommand(UpdateConsumerOffset, h))
}

// HeartbeatRequest heartbeat request data
type HeartbeatRequest struct {
	ClientID  string      `json:"clientID"`
	Producers []Producer  `json:"producerDataSet"`
	Consumers []*Consumer `json:"consumerDataSet"`
}

func (h *HeartbeatRequest) String() string {
	return fmt.Sprintf("HeartbeatRequest [ClientID="+h.ClientID+
		",Producers=%v,Consumers=%v]", h.Producers, h.Consumers)
}

// Producer producer's data in the heartbeat
type Producer struct {
	Group string `json:"groupName"`
}

func (p *Producer) String() string {
	return "Producer [group=" + p.Group + "]"
}

// Consumer consumer's data in the heartbeat
type Consumer struct {
	Group        string  `json:"groupName"`
	Type         string  `json:"consumeType"`
	Model        string  `json:"messageModel"`
	FromWhere    string  `json:"consumeFromWhere"`
	Subscription []*Data `json:"subscriptionDataSet"`
	UnitMode     bool    `json:"unitMode"`
}

// Data subscription information
type Data struct {
	Topic   string   `json:"topic"`
	Expr    string   `json:"subString"`
	Typ     string   `json:"expressionType"`
	Tags    []string `json:"tagsSet"`
	Codes   []uint32 `json:"codeSet"`
	Version int64    `json:"subVersion"`
}

func (c *Consumer) String() string {
	return fmt.Sprintf("ConsumerData [groupName="+c.Group+
		", consumeType="+c.Type+
		", messageModel="+c.Model+
		", consumeFromWhere="+c.FromWhere+
		", unitMode=%t"+
		", subscriptionDataSet=%v]", c.UnitMode, c.Subscription)
}

// SendHeartbeat send the heartbeat to the broker
func SendHeartbeat(client remote.Client, addr string, heartbeat *HeartbeatRequest, to time.Duration) (
	version int16, err error,
) {
	data, err := json.Marshal(heartbeat)
	if err != nil {
		return
	}

	cmd, err := client.RequestSync(addr, remote.NewCommandWithBody(HeartBeat, nil, data), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = remote.BrokerError(cmd)
	}

	version = cmd.Version
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
func UnregisterClient(
	client remote.Client, addr, clientID, pGroup, cGroup string, to time.Duration,
) (
	err error,
) {
	h := &unregisterClientHeader{
		clientID:      clientID,
		producerGroup: pGroup,
		consumerGroup: cGroup,
	}
	cmd, err := client.RequestSync(addr, remote.NewCommand(UnregisterClientCode, h), to)
	if err != nil {
		return remote.RequestError(err)
	}

	if cmd.Code != Success {
		err = remote.BrokerError(cmd)
	}
	return
}
