package client

import "time"

// Config the remote client configurations
type Config struct {
	HeartbeatBrokerInterval time.Duration
	PollNameServerInterval  time.Duration
	NameServerAddrs         []string
}
