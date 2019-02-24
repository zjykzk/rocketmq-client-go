package client

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
)

type rpc interface {
	GetTopicRouteInfo(addr string, topic string, to time.Duration) (*route.TopicRouter, error)
	UnregisterClient(addr, clientID, pGroup, cGroup string, to time.Duration) error
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

func (c *Consumer) String() string {
	return fmt.Sprintf("ConsumerData [groupName="+c.Group+
		", consumeType="+c.Type+
		", messageModel="+c.Model+
		", consumeFromWhere="+c.FromWhere+
		", unitMode=%t"+
		", subscriptionDataSet=%v]", c.UnitMode, c.Subscription)
}

// SendHeartbeat send the heartbeat to the broker
func sendHeartbeat(
	client remote.Client, addr string, heartbeat *HeartbeatRequest, to time.Duration,
) (
	version int16, err error,
) {
	data, err := json.Marshal(heartbeat)
	if err != nil {
		return
	}

	cmd, err := client.RequestSync(
		addr, remote.NewCommandWithBody(remote.HeartBeat, nil, data), to,
	)
	if err != nil {
		return
	}

	if cmd.Code != remote.Success {
		err = remote.BrokerError(cmd)
	}

	version = cmd.Version
	return
}
