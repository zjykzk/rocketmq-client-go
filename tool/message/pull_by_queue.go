package message

import (
	"flag"
	"fmt"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/tool/command"
)

type pullByQueue struct {
	queueID      int
	topic        string
	tags         string
	offset       int64
	broker       string
	maxCount     int
	namesrvAddrs string

	flags *flag.FlagSet
}

func init() {
	cmd := &pullByQueue{}
	flags := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
	flags.IntVar(&cmd.queueID, "q", -1, "queue id")
	flags.StringVar(&cmd.topic, "t", "", "topic")
	flags.StringVar(&cmd.tags, "T", "", "tags")
	flags.Int64Var(&cmd.offset, "o", -1, "offset")
	flags.StringVar(&cmd.broker, "b", "", "broker name")
	flags.IntVar(&cmd.maxCount, "m", -1, "max count")
	flags.StringVar(&cmd.namesrvAddrs, "n", "", "name servers")

	cmd.flags = flags

	command.RegisterCommand(cmd)
}

func (p *pullByQueue) Name() string {
	return "pullByQueue"
}

func (p *pullByQueue) Desc() string {
	return "pull the message from the specified queue"
}

func (p *pullByQueue) Run(args []string) {
	p.flags.Parse(args)

	if len(p.broker) == 0 {
		fmt.Println("empty broker:[" + p.broker + "]")
		p.Usage()
		return
	}

	if len(p.topic) == 0 {
		fmt.Println("empty topic: [" + p.topic + "]")
		p.Usage()
		return
	}

	if len(p.namesrvAddrs) == 0 {
		fmt.Println("empty namesrv: [" + p.namesrvAddrs + "]")
		p.Usage()
		return
	}

	logger := log.Std
	c := consumer.NewPullConsumer("test-group", strings.Split(p.namesrvAddrs, ","), logger)
	c.Start()

	pr, err := c.PullSync(
		&message.Queue{BrokerName: p.broker, QueueID: uint8(p.queueID), Topic: p.topic},
		p.tags, p.offset, p.maxCount)
	if err != nil {
		fmt.Printf("pull error:%s\n", err)
		return
	}
	fmt.Printf("message count:%d\n", len(pr.Messages))
	fmt.Printf("detail=%v\n", pr)
}

func (p *pullByQueue) Usage() {
	p.flags.Usage()
}
