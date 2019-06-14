package rpc

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

// BrokerRuntimeInfo returns the broker runtime information.
func BrokerRuntimeInfo(client remote.Client, addr string, timeout time.Duration) (
	map[string]string, error,
) {
	cmd, err := client.RequestSync(addr, remote.NewCommand(getBrokerRuntimeInfo, nil), timeout)

	if err != nil {
		return nil, requestError(err)
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	var ret map[string]map[string]string

	err = json.Unmarshal(cmd.Body, &ret)

	if err != nil {
		return nil, dataError(err)
	}

	return ret["table"], nil
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
	cmd, err := client.RequestSync(addr, remote.NewCommand(unregisterClient, h), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
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
	Group        string           `json:"groupName"`
	Type         string           `json:"consumeType"`
	Model        string           `json:"messageModel"`
	FromWhere    string           `json:"consumeFromWhere"`
	Subscription []*SubscribeData `json:"subscriptionDataSet"`
	UnitMode     bool             `json:"unitMode"`
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

	cmd, err := client.RequestSync(addr, remote.NewCommandWithBody(heartBeat, nil, data), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}

	version = cmd.Version
	return
}
