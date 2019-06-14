package message

import (
	"flag"
	"fmt"

	"github.com/zjykzk/rocketmq-client-go/admin"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/tool/command"
)

type consumeDirectly struct {
	group    string
	clientID string
	offsetID string

	flags *flag.FlagSet
}

func init() {
	cmd := &consumeDirectly{}
	flags := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd.group, "g", "", "group")
	flags.StringVar(&cmd.clientID, "c", "", "client id")
	flags.StringVar(&cmd.offsetID, "i", "", "offset id")

	cmd.flags = flags

	command.RegisterCommand(cmd)
}

func (c *consumeDirectly) Name() string {
	return "consumeDirectly"
}

func (c *consumeDirectly) Desc() string {
	return "consume the message by specified client directly"
}

func (c *consumeDirectly) Run(args []string) {
	c.flags.Parse(args)

	if c.group == "" {
		println("empty group")
		return
	}

	if c.clientID == "" {
		println("empty client id")
		return
	}

	if c.offsetID == "" {
		println("empty offset id")
		return
	}

	logger := log.Std
	a := admin.New("too-consume-directly", []string{"fake.namesrv.com"}, logger)
	a.Start()

	r, err := a.ConsumeMessageDirectly(c.group, c.clientID, c.offsetID)
	if err != nil {
		fmt.Printf("Error:%s\n", err)
		return
	}
	fmt.Printf("consume message directly result:%#v\n", r)
}

func (c *consumeDirectly) Usage() {
	c.flags.Usage()
}
