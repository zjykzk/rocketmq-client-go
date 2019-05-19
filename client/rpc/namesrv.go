package rpc

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go/fastjson"
	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// DeleteTopicInNamesrv delete topic in the broker
func DeleteTopicInNamesrv(client remote.Client, addr, topic string, to time.Duration) (err error) {
	h := deleteTopicHeader(topic)
	cmd, err := client.RequestSync(addr, remote.NewCommand(deleteTopicInNamesrv, h), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		return brokerError(cmd)
	}
	return
}

// GetBrokerClusterInfo get the cluster info from the namesrv
func GetBrokerClusterInfo(client remote.Client, addr string, to time.Duration) (*route.ClusterInfo, error) {
	cmd, err := client.RequestSync(addr, remote.NewCommand(getBrokerClusterInfo, nil), to)
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

	info, err = parseClusterInfo(cmd.Body)
	if err != nil {
		err = dataError(err)
	}
	return info, err
}

func parseClusterInfo(d []byte) (*route.ClusterInfo, error) {
	m, err := fastjson.ParseObject(d)
	if err != nil {
		return nil, err
	}

	clusterInfo := &route.ClusterInfo{}
	for k, v := range m {
		switch k {
		case "brokerAddrTable":
			clusterInfo.BrokerAddr, err = parseBrokerAddrInCluster(v)
		case "clusterAddrTable":
			clusterInfo.ClusterAddr, err = parseClusterAddr(v)
		}
		if err != nil {
			return nil, err
		}
	}

	return clusterInfo, nil
}

func parseBrokerAddrInCluster(d []byte) (map[string]*route.Broker, error) {
	m, err := fastjson.ParseObject(d)
	if err != nil {
		return nil, err
	}

	brokerAddr := map[string]*route.Broker{}
	for k, v := range m {
		brokerAddr[string(k)], err = parseBroker(v)
		if err != nil {
			return nil, err
		}
	}
	return brokerAddr, nil
}

func parseClusterAddr(d []byte) (map[string][]string, error) {
	m, err := fastjson.ParseObject(d)
	if err != nil {
		return nil, err
	}

	clusterAddr := map[string][]string{}
	for k, v := range m {
		es, err := fastjson.ParseArray(v)
		if err != nil {
			return nil, err
		}
		ss := make([]string, len(es))
		for i, e := range es {
			ss[i] = string(e)
		}
		clusterAddr[string(k)] = ss
	}

	return clusterAddr, nil
}

type getTopicRouteInfoHeader string

func (h getTopicRouteInfoHeader) ToMap() map[string]string {
	return map[string]string{"topic": string(h)}
}

// GetTopicRouteInfo returns the topic information.
func GetTopicRouteInfo(client remote.Client, addr string, topic string, to time.Duration) (
	router *route.TopicRouter, err *Error,
) {
	h := getTopicRouteInfoHeader(topic)
	cmd, e := client.RequestSync(addr, remote.NewCommand(getRouteintoByTopic, h), to)
	if e != nil {
		return nil, requestError(e)
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	if len(cmd.Body) == 0 {
		return
	}

	router, e = parseTopicRouter(cmd.Body)
	if e != nil {
		return nil, dataError(e)
	}
	return
}

func parseTopicRouter(d []byte) (*route.TopicRouter, error) {
	m, err := fastjson.ParseObject(d)
	if err != nil {
		return nil, err
	}

	r := &route.TopicRouter{}
	for k, v := range m {
		switch k {
		case "orderTopicConf":
			r.OrderTopicConf = string(v)
		case "queueDatas":
			err = json.Unmarshal(v, &r.Queues)
			if err != nil {
				return nil, err
			}
		case "brokerDatas":
			ds, err := fastjson.ParseArray(v)
			if err != nil {
				return nil, err
			}

			brokers := make([]*route.Broker, len(ds))
			for i, d := range ds {
				brokers[i], err = parseBroker(d)
				if err != nil {
					return nil, err
				}
			}
			r.Brokers = brokers
		case "filterServerTable":
			err = json.Unmarshal(v, &r.FilterServer)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

func parseBroker(d []byte) (*route.Broker, error) {
	m, err := fastjson.ParseObject(d)
	if err != nil {
		return nil, err
	}
	broker := &route.Broker{
		Cluster:   string(m["cluster"]),
		Name:      string(m["brokerName"]),
		Addresses: map[int32]string{},
	}

	d, ok := m["brokerAddrs"]
	if !ok {
		return broker, nil
	}

	m, err = fastjson.ParseObject(m["brokerAddrs"])
	if err != nil {
		return nil, err
	}

	for k, v := range m {
		id, err := strconv.Atoi(string(k))
		if err != nil {
			return nil, err
		}

		broker.Addresses[int32(id)] = string(v)
	}

	return broker, nil
}
