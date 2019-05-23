package message

import (
	"flag"
	"fmt"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/producer"
	"github.com/zjykzk/rocketmq-client-go/tool/command"
)

type sendMsg struct {
	topic        string
	tags         string
	body         string
	namesrvAddrs string

	flags *flag.FlagSet
}

func init() {
	cmd := &sendMsg{}
	flags := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd.topic, "t", "", "topic")
	flags.StringVar(&cmd.tags, "T", "", "tags")
	flags.StringVar(&cmd.body, "b", "", "body")
	flags.StringVar(&cmd.namesrvAddrs, "n", "", "name servers")

	cmd.flags = flags

	command.RegisterCommand(cmd)
}

func (s *sendMsg) Name() string {
	return "sendMsg"
}

func (s *sendMsg) Desc() string {
	return "send the message"
}

func (s *sendMsg) Run(args []string) {
	s.flags.Parse(args)

	if len(s.topic) == 0 {
		fmt.Println("empty topic: [" + s.topic + "]")
		s.Usage()
		return
	}

	if len(s.tags) == 0 {
		fmt.Println("empty tags")
		s.Usage()
		return
	}

	if len(s.body) == 0 {
		fmt.Println("empty body")
		s.Usage()
		return
	}

	if len(s.namesrvAddrs) == 0 {
		fmt.Println("empty namesrv: [" + s.namesrvAddrs + "]")
		s.Usage()
		return
	}

	logger := log.Std
	p := producer.New("test-group", strings.Split(s.namesrvAddrs, ","), logger)
	p.Start()

	msg := &message.Message{Topic: s.topic, Body: []byte(s.body)}
	msg.SetTags(s.tags)
	sr, err := p.SendSync(msg)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return
	}
	fmt.Printf("send result:%v\n", sr)
}

func (s *sendMsg) Usage() {
	s.flags.Usage()
}
