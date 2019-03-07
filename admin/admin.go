package admin

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// Admin admin operations
type Admin struct {
	rocketmq.Client
	state    rocketmq.State
	exitChan chan int

	client mqClient

	Logger log.Logger
}

// NewAdmin create admin operator
func NewAdmin(namesrvAddrs []string, logger log.Logger) *Admin {
	a := &Admin{
		Client: rocketmq.Client{
			HeartbeatBrokerInterval:       30 * time.Second,
			PollNameServerInterval:        30 * time.Second,
			PersistConsumerOffsetInterval: 5 * time.Second,
			NameServerAddrs:               namesrvAddrs,
			GroupName:                     "admin_ext_group",
		},
	}
	a.Logger = logger
	return a
}

// Start admin work
func (a *Admin) Start() (err error) {
	a.Logger.Info("start admin")
	switch a.state {
	case rocketmq.StateRunning:
		a.Logger.Warn("admin is running")
		return errors.New("bad status:running")
	case rocketmq.StateStopped:
		a.Logger.Errorf("admin is stoped")
		return errors.New("bad status:stoped")
	case rocketmq.StateStartFailed:
		a.Logger.Errorf("admin start failed")
		return errors.New("bad status:start failed")
	case rocketmq.StateCreating:
	default:
		panic("BUG:unknow state:" + strconv.Itoa(int(a.state)))
	}

	a.state = rocketmq.StateStartFailed
	a.InstanceName = strconv.Itoa(os.Getpid())
	a.ClientIP, err = rocketmq.GetIPStr()
	if err != nil {
		a.Logger.Errorf("no ip")
		return
	}
	a.ClientID = client.BuildMQClientID(a.ClientIP, a.UnitName, a.InstanceName)
	a.client, err = client.New(
		&client.Config{
			HeartbeatBrokerInterval: a.HeartbeatBrokerInterval,
			PollNameServerInterval:  a.PollNameServerInterval,
			NameServerAddrs:         a.NameServerAddrs,
		}, a.ClientID, a.Logger)
	if err != nil {
		return
	}

	err = a.client.RegisterAdmin(a)
	if err != nil {
		a.state = rocketmq.StateCreating
		a.Logger.Errorf("register producer error:%s", err.Error())
		return
	}

	err = a.client.Start()
	if err == nil {
		a.state = rocketmq.StateRunning
	}
	return
}

// Shutdown admin work
func (a *Admin) Shutdown() error {
	a.Logger.Info("shutdown admin:" + a.GroupName)
	switch a.state {
	case rocketmq.StateRunning:
		a.client.UnregisterAdmin(a.GroupName)
		a.client.Shutdown()
		a.state = rocketmq.StateStopped
		return nil
	default:
		a.Logger.Warnf("shutdown admin under state:%s", a.state.String())
		return fmt.Errorf("shutdown admin at bad state:%d", a.state)
	}
}

// Group returns the GroupName of the producer
func (a *Admin) Group() string {
	return a.GroupName
}

// CreateOrUpdateTopic create a new topic
func (a *Admin) CreateOrUpdateTopic(addr, topic string, perm, queueCount int32) error {
	header := &rpc.CreateOrUpdateTopicHeader{
		Topic:           topic,
		ReadQueueNums:   queueCount,
		WriteQueueNums:  queueCount,
		DefaultTopic:    rocketmq.DefaultTopic,
		Perm:            perm,
		TopicFilterType: SingleTag.String(),
	}

	var (
		err error
	)
	for i := 0; i < 5; i++ {
		if err = a.client.CreateOrUpdateTopic(addr, header, 3*time.Second); err == nil {
			return nil
		}
	}

	return err
}

// DeleteTopicInBroker delete the topic in the broker
func (a *Admin) DeleteTopicInBroker(addr, topic string) (err error) {
	err = a.client.DeleteTopicInBroker(addr, topic, 3*time.Second)
	if err != nil {
		a.Logger.Errorf("delete topic %s in broker:%s error:%s", topic, addr, err)
		return
	}

	a.Logger.Debugf("DELETE topic %s suc at broker %s", topic, addr)
	return
}

// DeleteTopicInAllNamesrv delete the topic in the namesrv
func (a *Admin) DeleteTopicInAllNamesrv(topic string) (err error) {
	for _, addr := range a.NameServerAddrs {
		err = a.client.DeleteTopicInNamesrv(addr, topic, 3*time.Second)
		if err != nil {
			a.Logger.Errorf("delete topic %s in namesrv:%s error:%s", topic, addr, err)
			continue
		}
		a.Logger.Debugf("DELETE topic %s suc at namesrv %s", topic, addr)
	}
	return
}

// GetBrokerClusterInfo get broker cluster info
func (a *Admin) GetBrokerClusterInfo() (info *route.ClusterInfo, err error) {
	l := len(a.NameServerAddrs)
	for i, c := rand.Intn(l), l; c > 0; i, c = i+1, c-1 {
		addr := a.NameServerAddrs[i%l]
		info, err = a.client.GetBrokerClusterInfo(addr, 3*time.Second)
		if err == nil {
			return
		}

		a.Logger.Errorf("request broker cluster info from %s, error:%s", addr, err)
	}
	return
}

// QueryMessageByID querys the message by message id
func (a *Admin) QueryMessageByID(id string) (*message.Ext, error) {
	addr, offset, err := message.ParseMessageID(id)
	if err != nil {
		return nil, err
	}

	return a.client.QueryMessageByOffset(addr.String(), offset, 3*time.Second)
}

// MaxOffset fetches the max offset of the consume queue
func (a *Admin) MaxOffset(q *message.Queue) (int64, error) {
	addr, err := a.client.FindBrokerAddr(q.BrokerName, rocketmq.MasterID, false)
	if err != nil {
		err = a.client.UpdateTopicRouterInfoFromNamesrv(q.Topic)
		if err != nil {
			return -1, err
		}

		addr, err = a.client.FindBrokerAddr(q.BrokerName, rocketmq.MasterID, false)
		if err != nil {
			return -1, err
		}
	}

	return a.client.MaxOffset(addr.Addr, q.Topic, uint8(q.QueueID), 3*time.Second)
}

// GetConsumerIDs get the consumer ids from the broker
func (a *Admin) GetConsumerIDs(addr, group string) ([]string, error) {
	return a.client.GetConsumerIDs(addr, group, time.Second*3)
}

// TopicFilter details
type TopicFilter int8

func (f TopicFilter) String() string {
	switch f {
	case SingleTag:
		return "SINGLE_TAG"
	case MultiTag:
		return "MULTI_TAG"
	default:
		panic("BUG:unknow topic filter:" + strconv.Itoa(int(f)))
	}
}

// TopicFilter defination
const (
	SingleTag TopicFilter = iota
	MultiTag
)
