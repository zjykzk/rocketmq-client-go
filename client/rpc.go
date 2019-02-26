package client

import (
	"fmt"
	"time"

	"github.com/zjykzk/rocketmq-client-go/route"
)

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

type rpc interface {
	GetTopicRouteInfo(addr string, topic string, to time.Duration) (*route.TopicRouter, error)
	SendHeartbeat(addr string, heartbeat *HeartbeatRequest, to time.Duration) (int64, error)
	UnregisterClient(addr, clientID, pGroup, cGroup string, to time.Duration) error
}
