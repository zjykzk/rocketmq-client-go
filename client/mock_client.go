package client

import "github.com/zjykzk/rocketmq-client-go/remote"

type EmptyMQClient struct{}

func (c *EmptyMQClient) Start() error { return nil }
func (c *EmptyMQClient) Shutdown()    {}

func (c *EmptyMQClient) RegisterProducer(p Producer) error                   { return nil }
func (c *EmptyMQClient) UnregisterProducer(group string)                     {}
func (c *EmptyMQClient) RegisterConsumer(co consumer) error                  { return nil }
func (c *EmptyMQClient) UnregisterConsumer(group string)                     {}
func (c *EmptyMQClient) RegisterAdmin(a admin) error                         { return nil }
func (c *EmptyMQClient) UnregisterAdmin(group string)                        {}
func (c *EmptyMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error { return nil }

func (c *EmptyMQClient) AdminCount() int    { return 0 }
func (c *EmptyMQClient) ConsumerCount() int { return 0 }
func (c *EmptyMQClient) ProducerCount() int { return 0 }

func (c *EmptyMQClient) GetMasterBrokerAddr(brokerName string) string { return "" }
func (c *EmptyMQClient) GetMasterBrokerAddrs() []string               { return nil }
func (c *EmptyMQClient) FindBrokerAddr(brokerName string, hintBrokerID int32, lock bool) (*FindBrokerResult, error) {
	return nil, nil
}
func (c *EmptyMQClient) FindAnyBrokerAddr(brokerName string) (*FindBrokerResult, error) {
	return nil, nil
}
func (c *EmptyMQClient) FindMasterBrokerAddr(brokerName string) (string, error) {
	return "", nil
}
func (c *EmptyMQClient) RemotingClient() remote.Client { return nil }
func (c *EmptyMQClient) SendHeartbeat()                {}
