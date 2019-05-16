package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/producer"
)

var (
	namesrvAddrs string
	topic        string
	group        string
	batch        bool
)

func init() {
	flag.StringVar(&namesrvAddrs, "n", "", "name server address")
	flag.StringVar(&topic, "t", "", "topic")
	flag.StringVar(&group, "g", "", "group")
	flag.BoolVar(&batch, "b", false, "is send batch")
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

	logger, err := newLogger("producer.log")
	if err != nil {
		fmt.Printf("new logger of producer.loge error:%s\n", err)
		return
	}

	p := producer.New(group, strings.Split(namesrvAddrs, ","), logger)
	err = p.Start()
	if err != nil {
		fmt.Printf("start producer error:%s\n", err)
		return
	}

	defer p.Shutdown()

	exitCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:

				var (
					r   *producer.SendResult
					err error
				)
				if batch {
					r, err = sendBatch(p)
				} else {
					r, err = sendSimple(p)
				}

				if err != nil {
					fmt.Printf("%s send sync error:%s\n", time.Now(), err)
					break
				}
				fmt.Printf("send suc:%s\n", r)
			case <-exitCh:
				return
			}
		}
	}()

	waitQuitSignal(func() { close(exitCh) })
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
