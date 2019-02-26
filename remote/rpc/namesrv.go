package rpc

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go/route"
)

// DeleteTopicInNamesrv delete topic in the broker
func (r *RPC) DeleteTopicInNamesrv(addr, topic string, to time.Duration) (err error) {
	h := deleteTopicHeader(topic)
	cmd, err := r.client.RequestSync(addr, NewCommand(DeleteTopicInNamesrv, h), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
}

// GetBrokerClusterInfo get the cluster info from the namesrv
func (r *RPC) GetBrokerClusterInfo(addr string, to time.Duration) (*route.ClusterInfo, error) {
	cmd, err := r.client.RequestSync(addr, NewCommand(GetBrokerClusterInfo, nil), to)
	if err != nil {
		return nil, err
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	info := &route.ClusterInfo{}
	if len(cmd.Body) == 0 {
		return info, nil
	}

	bodyjson := strings.Replace(string(cmd.Body), ",0:", ",\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1) // fastJson key is string todo todo
	bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
	err = json.Unmarshal([]byte(bodyjson), info)
	return info, err
}

type getTopicRouteInfoHeader string

func (h getTopicRouteInfoHeader) ToMap() map[string]string {
	return map[string]string{"topic": string(h)}
}

// GetTopicRouteInfo returns the topic information.
func (r *RPC) GetTopicRouteInfo(addr string, topic string, to time.Duration) (
	router *route.TopicRouter, err error,
) {
	h := getTopicRouteInfoHeader(topic)
	cmd, err := r.client.RequestSync(addr, command.NewCommand(command.GetRouteintoByTopic, h), to)
	if err != nil {
		return
	}

	if cmd.Code != command.Success {
		err = brokerError(cmd)
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
