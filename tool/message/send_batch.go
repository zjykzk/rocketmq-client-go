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

type sendBatch struct {
	topic        string
	tags         string
	bodys        string
	namesrvAddrs string

	flags *flag.FlagSet
}

func init() {
	cmd := &sendBatch{}
	flags := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd.topic, "t", "", "topic")
	flags.StringVar(&cmd.tags, "T", "", "tags")
	flags.StringVar(&cmd.bodys, "b", "", "bodys")
	flags.StringVar(&cmd.namesrvAddrs, "n", "", "name servers")

	cmd.flags = flags

	command.RegisterCommand(cmd)
}

func (s *sendBatch) Name() string {
	return "sendBatch"
}

func (s *sendBatch) Desc() string {
	return "send the message batchly"
}

func (s *sendBatch) Run(args []string) {
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

	if len(s.bodys) == 0 {
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
	p := producer.New(s.Name(), strings.Split(s.namesrvAddrs, ","), logger)
	p.Start()

	bodys := strings.Split(s.bodys, ",")
	datas := make([]message.Data, len(bodys))
	for i := range bodys {
		datas[i].Body, datas[i].Properties = []byte(bodys[i]), map[string]string{message.PropertyTags: s.tags}
	}

	batch := &message.Batch{Datas: datas, Topic: s.topic}
	sr, err := p.SendBatchSync(batch)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return
	}

	fmt.Println("message uniq id:")
	for _, d := range datas {
		fmt.Println(message.GetUniqID(d.Properties))
	}
	fmt.Printf("send result:%v\n", sr)
}

func (s *sendBatch) Usage() {
	s.flags.Usage()
}
