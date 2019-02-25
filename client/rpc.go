package client

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
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

func unregisterClient(
	client remote.Client, addr, clientID, pGroup, cGroup string, to time.Duration,
) error {
	h := &unregisterClientHeader{
		clientID:      clientID,
		producerGroup: pGroup,
		consumerGroup: cGroup,
	}
	cmd, err := client.RequestSync(addr, remote.NewCommand(remote.UnregisterClient, h), to)
	if err != nil {
		return err
	}

	if cmd.Code != remote.Success {
		err = remote.BrokerError(cmd)
	}
	return err
}

type getTopicRouteInfoHeader string

func (h getTopicRouteInfoHeader) ToMap() map[string]string {
	return map[string]string{"topic": string(h)}
}

func getTopicRouteInfo(
	client remote.Client, addr string, topic string, to time.Duration,
) (
	router *route.TopicRouter, err error,
) {
	h := getTopicRouteInfoHeader(topic)
	cmd, err := client.RequestSync(addr, remote.NewCommand(remote.GetRouteintoByTopic, h), to)
	if err != nil {
		return
	}

	if cmd.Code != remote.Success {
		err = remote.BrokerError(cmd)
		return
	}

	if len(cmd.Body) == 0 {
		return
	}

	router = &route.TopicRouter{}
	bodyjson := strings.Replace(string(cmd.Body), ",0:", ",\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1) // fastJson key is string todo todo
	bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
	if err = json.Unmarshal([]byte(bodyjson), router); err != nil {
		return
	}
	return
}
