package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/zjykzk/rocketmq-client-go/log"
)

var (
	namesrvAddrs string
	group        string
	tags         string
	topic        string
	model        string
)

func init() {
	flag.StringVar(&namesrvAddrs, "n", "", "name server address")
	flag.StringVar(&model, "m", "pull", `"pull" or "push"`)
	flag.StringVar(&topic, "t", "", "topic")
	flag.StringVar(&group, "g", "", "group")
	flag.StringVar(&tags, "a", "", "tags")
}

// push consumer: go run -n 10.20.200.198:9988 -m=pull -t=topic_name
// pull consumer: go run -n 10.20.200.198:9988 -m=push -t=topic_name
func main() {
	flag.Parse()

	if namesrvAddrs == "" {
		println("bad namesrvAddrs:" + namesrvAddrs)
		return
	}

	if topic == "" {
		println("bad topic:" + topic)
		return
	}

	if model == "pull" {
		runPull()
	} else if model == "push" {
		runPush()
	} else {
		println("bad model:" + model)
		return
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
