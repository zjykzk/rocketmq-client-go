package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/producer"
)

var (
	namesrvAddrs  string
	topic         string
	instanceCount int
	sendCount     int
)

func init() {
	flag.StringVar(&namesrvAddrs, "n", "", "name server address")
	flag.StringVar(&topic, "t", "", "topic")
	flag.IntVar(&sendCount, "c", 1, "the count of message to send")
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

	if sendCount <= 0 {
		sendCount = 1
	}

	fmt.Printf("namesrv:%s, topic:%s, send count:%d\n", namesrvAddrs, topic, sendCount)
	p := producer.NewProducer("test-group", strings.Split(namesrvAddrs, ","), log.Std)
	err := p.Start()
	if err != nil {
		fmt.Printf("start error:%v", err)
		return
	}

	for i := 0; i < sendCount; i++ {
		sendResult, err := p.SendSync(&message.Message{
			Topic:      topic,
			Body:       []byte(" test "),
			Properties: map[string]string{},
		})
		fmt.Printf("send result:%v, err:%v\n", sendResult, err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-signalChan:
		p.Shutdown()
	}
}
