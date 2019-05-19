package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/route"
)

func TestParseTopicRouter(t *testing.T) {
	r, err := parseTopicRouter([]byte("{}"))
	assert.Nil(t, err)
	assert.Equal(t, "", r.OrderTopicConf)
	assert.Nil(t, r.Queues)
	assert.Nil(t, r.Brokers)
	assert.Nil(t, r.FilterServer)

	r, err = parseTopicRouter([]byte(`{"orderTopicConf":"order \"topic conf","queueDatas":[{"perm":0,"readQueueNums":0,"topicSynFlag":0,"writeQueueNums":0},{"perm":0,"readQueueNums":0,"topicSynFlag":0,"writeQueueNums":0}]}`))
	assert.Nil(t, err)
	assert.Equal(t, "order \\\"topic conf", r.OrderTopicConf)
	assert.Equal(t, []*route.TopicQueue{{}, {}}, r.Queues)
	assert.Nil(t, r.Brokers)
	assert.Nil(t, r.FilterServer)

	r, err = parseTopicRouter([]byte(`{"brokerDatas":[{"brokerAddrs":{0:"addr0",1:"addr1",2:"addr2",3:"addr3"},"brokerName":"broker name","cluster":"cluster"}],"orderTopicConf":"order \"topic conf","queueDatas":[{"perm":0,"readQueueNums":0,"topicSynFlag":0,"writeQueueNums":0},{"perm":0,"readQueueNums":0,"topicSynFlag":0,"writeQueueNums":0}]}`))
	assert.Nil(t, err)
	assert.Equal(t, "order \\\"topic conf", r.OrderTopicConf)
	assert.Equal(t, []*route.TopicQueue{{}, {}}, r.Queues)
	assert.Equal(t, []*route.Broker{
		{Cluster: "cluster", Name: "broker name", Addresses: map[int32]string{0: "addr0", 1: "addr1", 2: "addr2", 3: "addr3"}},
	}, r.Brokers)
	assert.Nil(t, r.FilterServer)

	r, err = parseTopicRouter([]byte(`{"brokerDatas":[{"brokerAddrs":{0:"addr0",1:"addr1",2:"addr2",3:"addr3"},"brokerName":"broker name","cluster":"cluster"}],"filterServerTable":{"addr0":["server0","server1"],"addr1":[]},"orderTopicConf":"order \"topic conf","queueDatas":[{"perm":0,"readQueueNums":0,"topicSynFlag":0,"writeQueueNums":0},{"perm":0,"readQueueNums":0,"topicSynFlag":0,"writeQueueNums":0}]}
`))
	assert.Nil(t, err)
	assert.Equal(t, "order \\\"topic conf", r.OrderTopicConf)
	assert.Equal(t, []*route.TopicQueue{{}, {}}, r.Queues)
	assert.Equal(t, []*route.Broker{
		{Cluster: "cluster", Name: "broker name", Addresses: map[int32]string{0: "addr0", 1: "addr1", 2: "addr2", 3: "addr3"}},
	}, r.Brokers)
	assert.Equal(t, map[string][]string{"addr0": []string{"server0", "server1"}, "addr1": []string{}}, r.FilterServer)
}

func TestParseClusterInfo(t *testing.T) {
	r, err := parseClusterInfo([]byte("{}"))
	assert.Nil(t, err)
	assert.Nil(t, r.BrokerAddr)
	assert.Nil(t, r.ClusterAddr)

	r, err = parseClusterInfo([]byte(`{"brokerAddrTable":{"name1":{},"name0":{}}}`))
	assert.Nil(t, err)
	assert.Equal(t, route.Broker{Addresses: map[int32]string{}}, *r.BrokerAddr["name0"])
	assert.Equal(t, route.Broker{Addresses: map[int32]string{}}, *r.BrokerAddr["name1"])
	assert.Nil(t, r.ClusterAddr)

	r, err = parseClusterInfo([]byte(`{"brokerAddrTable":{"name1":{},"name0":{"brokerAddrs":{0:"addr0",1:"addr1",2:"addr2",3:"addr3"},"brokerName":"broker name","cluster":"cluster"}},"clusterAddrTable":{"cluster0":["1","2"]}}`))
	assert.Nil(t, err)
	assert.Equal(t, route.Broker{
		Cluster:   "cluster",
		Name:      "broker name",
		Addresses: map[int32]string{0: "addr0", 1: "addr1", 2: "addr2", 3: "addr3"},
	}, *r.BrokerAddr["name0"])
	assert.Equal(t, route.Broker{Addresses: map[int32]string{}}, *r.BrokerAddr["name1"])
	assert.Equal(t, map[string][]string{"cluster0": []string{"1", "2"}}, r.ClusterAddr)
}
