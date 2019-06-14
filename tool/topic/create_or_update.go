package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/admin"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/route"
)

var (
	namesrvAddrs string
	topic        string
	isCreate     bool
)

func init() {
	flag.StringVar(&namesrvAddrs, "n", "", "name server address")
	flag.StringVar(&topic, "t", "", "topic")
	flag.BoolVar(&isCreate, "c", true, "is create")
}

func main() {
	flag.Parse()

	if len(namesrvAddrs) == 0 {
		println("bad namesrvAddrs:" + namesrvAddrs)
		return
	}

	if len(topic) == 0 {
		println("bad topic:" + topic)
		return
	}

	logger := log.Std
	a := admin.New("tool-create-or-update", strings.Split(namesrvAddrs, ","), logger)
	a.Start()
	defer a.Shutdown()

	brokerCluster, err := a.GetBrokerClusterInfo()
	if err != nil {
		fmt.Printf("get broker cluseter info error:%s", err)
		return
	}

	if isCreate {
		for _, b := range brokerCluster.BrokerAddr {
			addr := b.Addresses[rocketmq.MasterID]
			if addr == "" {
				fmt.Println("no master broker")
				continue
			}
			err := a.CreateOrUpdateTopic(addr, topic, 2, route.PermWrite|route.PermRead)
			if err != nil {
				logger.Errorf("create topic in broker error:%s", err)
			}
		}
		return
	}

	for _, b := range brokerCluster.BrokerAddr {
		addr := b.Addresses[rocketmq.MasterID]
		if addr == "" {
			fmt.Println("no master broker")
			continue
		}

		err = a.DeleteTopicInBroker(addr, topic)
		if err != nil {
			logger.Errorf("delete topic in all broker error:%s", err)
		}
	}

	err = a.DeleteTopicInAllNamesrv(topic)
	if err != nil {
		logger.Errorf("delete topic in all namesrv error:%s", err)
	}

}
