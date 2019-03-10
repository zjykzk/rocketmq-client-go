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
	tags         string
	topic        string
	isPull       bool
)

func init() {
	flag.StringVar(&namesrvAddrs, "n", "", "name server address")
	flag.BoolVar(&isPull, "m", true, "model:pull or push")
	flag.StringVar(&topic, "t", "", "topic")
	flag.StringVar(&tags, "g", "", "tags")
}

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

	if isPull {
		runPull()
	} else {
		runPush()
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