package consumer

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/zjykzk/rocketmq-client-go/admin"
	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/tool/command"
)

func init() {
	cmd := &offsetUpdater{}

	flags := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd.namesrvAddrs, "a", "", "namesrv addresses, split by comma")
	flags.StringVar(&cmd.topic, "t", "", "topic")
	flags.StringVar(&cmd.group, "g", "", "group")
	flags.StringVar(&cmd.broker, "b", "", "broker name")
	flags.IntVar(&cmd.queueID, "q", 0, "queue id")
	flags.Int64Var(&cmd.offset, "o", 0, "offset")
	cmd.flags = flags

	command.RegisterCommand(cmd)

	cmd1 := &offsetQuerier{}
	flags = flag.NewFlagSet(cmd1.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd1.namesrvAddrs, "a", "", "namesrv addresses, split by comma")
	flags.StringVar(&cmd1.topic, "t", "", "topic")
	flags.StringVar(&cmd1.group, "g", "", "group")
	flags.StringVar(&cmd1.broker, "b", "", "broker name")
	flags.IntVar(&cmd1.queueID, "q", 0, "queue id")
	cmd1.flags = flags

	command.RegisterCommand(cmd1)

	cmd2 := &offsetReseter{}
	flags = flag.NewFlagSet(cmd2.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd2.namesrvAddrs, "a", "", "namesrv addresses, split by comma")
	flags.StringVar(&cmd2.topic, "t", "", "topic")
	flags.StringVar(&cmd2.group, "g", "", "group")
	flags.StringVar(&cmd2.broker, "b", "", "broker name")
	flags.StringVar(&cmd2.timestamp, "s", "", "timestamp")
	flags.BoolVar(&cmd2.isForce, "f", false, "is force")
	cmd2.flags = flags

	command.RegisterCommand(cmd2)
}

type offsetUpdater struct {
	namesrvAddrs  string
	topic, broker string
	group         string
	queueID       int
	offset        int64
	flags         *flag.FlagSet
}

func (c *offsetUpdater) Name() string {
	return "updateOffset"
}

func (c *offsetUpdater) Desc() string {
	return "update the client's offset"
}

func (c *offsetUpdater) Run(args []string) {
	c.flags.Parse(args)
	if len(c.namesrvAddrs) == 0 {
		fmt.Println("empty namesrv address")
		return
	}

	if c.topic == "" {
		fmt.Println("empty topic")
		return
	}

	if c.broker == "" {
		fmt.Println("empty broker")
		return
	}

	if c.offset < 0 {
		fmt.Println("offset must positive")
		return
	}

	if c.group == "" {
		fmt.Println("empty group")
		return
	}
	logger := log.Std
	consumer := consumer.NewPullConsumer(c.group, strings.Split(c.namesrvAddrs, ","), logger)
	consumer.Start()

	err := consumer.UpdateOffset(
		&message.Queue{Topic: c.topic, BrokerName: c.broker, QueueID: uint8(c.queueID)}, c.offset, false,
	)

	if err != nil {
		logger.Error(err)
	}
}

func (c *offsetUpdater) Usage() {
	c.flags.Usage()
}

type offsetQuerier struct {
	namesrvAddrs  string
	topic, broker string
	group         string
	queueID       int
	flags         *flag.FlagSet
}

func (c *offsetQuerier) Name() string {
	return "queryOffset"
}

func (c *offsetQuerier) Desc() string {
	return "query specified consume queue's offset"
}

func (c *offsetQuerier) Run(args []string) {
	c.flags.Parse(args)
	if len(c.namesrvAddrs) == 0 {
		fmt.Println("empty namesrv address")
		return
	}

	if c.topic == "" {
		fmt.Println("empty topic")
		return
	}

	if c.broker == "" {
		fmt.Println("empty broker")
		return
	}

	if c.group == "" {
		fmt.Println("empty group")
		return
	}

	logger := log.Std
	consumer := consumer.NewPullConsumer(c.group, strings.Split(c.namesrvAddrs, ","), logger)
	consumer.Start()

	offset, err := consumer.QueryConsumerOffset(
		&message.Queue{Topic: c.topic, BrokerName: c.broker, QueueID: uint8(c.queueID)},
	)

	if err != nil {
		logger.Error(err)
	}
	fmt.Println(offset)
}

func (c *offsetQuerier) Usage() {
	c.flags.Usage()
}

type offsetReseter struct {
	namesrvAddrs  string
	topic, broker string
	group         string
	timestamp     string
	isForce       bool
	flags         *flag.FlagSet
}

func (c *offsetReseter) Name() string {
	return "resetOffset"
}

func (c *offsetReseter) Desc() string {
	return "reset client's consume-offset"
}

func (c *offsetReseter) Run(args []string) {
	c.flags.Parse(args)
	if len(c.namesrvAddrs) == 0 {
		fmt.Println("empty namesrv address")
		return
	}

	if c.topic == "" {
		fmt.Println("empty topic")
		return
	}

	if c.broker == "" {
		fmt.Println("empty broker")
		return
	}

	if c.group == "" {
		fmt.Println("empty group")
		return
	}

	if c.timestamp == "" {
		fmt.Println("empty timestamp")
		return
	}
	logger := log.Std
	a := admin.New("tool-offset", strings.Split(c.namesrvAddrs, ","), logger)
	a.Start()

	t, err := time.Parse("2006-01-02 15:04:05", c.timestamp)
	if err != nil {
		fmt.Printf("bad timestamp '%s', error:%s", c.timestamp, err)
		return
	}

	r, err := a.ResetConsumeOffset(c.broker, c.topic, c.group, t, c.isForce)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return
	}

	fmt.Printf("reset result:%v\n", r)
}

func (c *offsetReseter) Usage() {
	c.flags.Usage()
}
