package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/zjykzk/rocketmq-client-go/log"
)

var (
	group        string
	namesrvAddrs string
	tags         string
	topic        string
	isProducer   bool
)

func init() {
	flag.StringVar(&group, "g", "", "group name")
	flag.StringVar(&namesrvAddrs, "n", "", "name server address, split by comma")
	flag.StringVar(&topic, "t", "", "topic")
	flag.StringVar(&tags, "a", "", "tags, split by comma")
	flag.BoolVar(&isProducer, "p", true, "if true, run producer, otherwise consumer")
}

// consumer: go run -n 10.20.200.198:9988 -t=topic_name -p=false
// producer: go run -n 10.20.200.198:9988 -t=topic_name -p=true
func main() {
	flag.Parse()

	if group == "" {
		println("empty group")
		return
	}

	if namesrvAddrs == "" {
		println("bad namesrvAddrs:" + namesrvAddrs)
		return
	}

	if topic == "" {
		println("bad topic:" + topic)
		return
	}

	if isProducer {
		runProucer()
	} else {
		runConsumer()
	}
}

func newLogger(filename string) (log.Logger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		println("create file error", err.Error())
		return nil, err
	}

	logger := log.New(file, "", log.Ldefault)
	logger.Level = log.Ldebug

	return logger, nil
}

func waitQuitSignal(shutdown func()) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-signalChan:
		shutdown()
	}
}
