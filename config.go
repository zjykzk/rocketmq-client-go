package rocketmq

import "time"

// Client the configuration of client(producer/consumer)
type Client struct {
	HeartbeatBrokerInterval       time.Duration
	PollNameServerInterval        time.Duration
	PersistConsumerOffsetInterval time.Duration
	NameServerAddrs               []string
	IsUnitMode                    bool
	UnitName                      string
	VipChannelEnabled             bool
	InstanceName                  string
	ClientIP                      string
	GroupName                     string
	ClientID                      string
}
