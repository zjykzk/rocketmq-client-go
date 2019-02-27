package rpc

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// DeleteTopicInNamesrv delete topic in the broker
func (r *RPC) DeleteTopicInNamesrv(addr, topic string, to time.Duration) (err error) {
	h := deleteTopicHeader(topic)
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(DeleteTopicInNamesrv, h), to)
	if err != nil {
		return remote.RequestError(err)
	}

	if cmd.Code != Success {
		return remote.BrokerError(cmd)
	}
	return
}

// GetBrokerClusterInfo get the cluster info from the namesrv
func (r *RPC) GetBrokerClusterInfo(addr string, to time.Duration) (*route.ClusterInfo, error) {
	cmd, err := r.client.RequestSync(addr, remote.NewCommand(GetBrokerClusterInfo, nil), to)
	if err != nil {
		return nil, err
	}

	if cmd.Code != Success {
		return nil, remote.BrokerError(cmd)
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
	if err != nil {
		err = remote.DataError(err)
	}
	return info, err
}

type getTopicRouteInfoHeader string

func (h getTopicRouteInfoHeader) ToMap() map[string]string {
	return map[string]string{"topic": string(h)}
}

// GetTopicRouteInfo returns the topic information.
func GetTopicRouteInfo(client remote.Client, addr string, topic string, to time.Duration) (
	router *route.TopicRouter, err *remote.RPCError,
) {
	h := getTopicRouteInfoHeader(topic)
	cmd, e := client.RequestSync(addr, remote.NewCommand(GetRouteintoByTopic, h), to)
	if e != nil {
		return nil, remote.RequestError(e)
	}

	if cmd.Code != Success {
		return nil, remote.BrokerError(cmd)
	}

	if len(cmd.Body) == 0 {
		return
	}

	router = &route.TopicRouter{}
	bodyjson := strings.Replace(string(cmd.Body), ",0:", ",\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1) // fastJson key is string todo todo
	bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
	if e = json.Unmarshal([]byte(bodyjson), router); e != nil {
		return nil, remote.DataError(e)
	}
	return
}
