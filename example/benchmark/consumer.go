package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zjykzk/rocketmq-client-go/consumer"
	"github.com/zjykzk/rocketmq-client-go/log"
)

type statiBenchmarkConsumerSnapshot struct {
	receiveMessageTotal   int64
	born2ConsumerTotalRT  int64
	store2ConsumerTotalRT int64
	born2ConsumerMaxRT    int64
	store2ConsumerMaxRT   int64
	createdAt             time.Time
	next                  *statiBenchmarkConsumerSnapshot
}

type consumeSnapshots struct {
	sync.RWMutex
	head, tail, cur *statiBenchmarkConsumerSnapshot
	len             int
}

func (s *consumeSnapshots) takeSnapshot() {
	b := s.cur
	sn := new(statiBenchmarkConsumerSnapshot)
	sn.receiveMessageTotal = atomic.LoadInt64(&b.receiveMessageTotal)
	sn.born2ConsumerMaxRT = atomic.LoadInt64(&b.born2ConsumerMaxRT)
	sn.born2ConsumerTotalRT = atomic.LoadInt64(&b.born2ConsumerTotalRT)
	sn.store2ConsumerMaxRT = atomic.LoadInt64(&b.store2ConsumerMaxRT)
	sn.store2ConsumerTotalRT = atomic.LoadInt64(&b.store2ConsumerTotalRT)
	sn.createdAt = time.Now()

	s.Lock()
	if s.tail != nil {
		s.tail.next = sn
	}
	s.tail = sn
	if s.head == nil {
		s.head = s.tail
	}

	s.len++
	if s.len > 10 {
		s.head = s.head.next
		s.len--
	}
	s.Unlock()
}

func (s *consumeSnapshots) printStati() {
	s.RLock()
	if s.len < 10 {
		s.RUnlock()
		return
	}

	f, l := s.head, s.tail
	respSucCount := float64(l.receiveMessageTotal - f.receiveMessageTotal)
	consumeTps := respSucCount / l.createdAt.Sub(f.createdAt).Seconds()
	avgB2CRT := float64(l.born2ConsumerTotalRT-f.born2ConsumerTotalRT) / respSucCount
	avgS2CRT := float64(l.store2ConsumerTotalRT-f.store2ConsumerTotalRT) / respSucCount
	s.RUnlock()

	fmt.Printf(
		"Consume TPS: %d Average(B2C) RT: %7.3f Average(S2C) RT: %7.3f MAX(B2C) RT: %d MAX(S2C) RT: %d\n",
		int64(consumeTps), avgB2CRT, avgS2CRT, l.born2ConsumerMaxRT, l.store2ConsumerMaxRT,
	)
}

type bconsumer struct {
	topic          string
	groupPrefix    string
	nameSrv        string
	isPrefixEnable bool
	filterType     string
	expression     string
	testMinutes    int
	instanceCount  int

	c *consumer.PullConsumer

	flags *flag.FlagSet

	groupID string
}

func init() {
	c := &bconsumer{}
	flags := flag.NewFlagSet("consumer", flag.ExitOnError)
	c.flags = flags

	flags.StringVar(&c.topic, "t", "BenchmarkTest", "topic")
	flags.StringVar(&c.groupPrefix, "g", "benchmark_consumer", "group prefix")
	flags.StringVar(&c.nameSrv, "n", "", "namesrv address list, seperated by comma")
	flags.BoolVar(&c.isPrefixEnable, "p", true, "group prefix is enable")
	flags.StringVar(&c.filterType, "f", "", "filter type,options:TAG|SQL92, or empty")
	flags.StringVar(&c.expression, "e", "*", "expression")
	flags.IntVar(&c.testMinutes, "m", 10, "test minutes")
	flags.IntVar(&c.instanceCount, "i", 1, "instance count")

	registerCommand("consumer", c)
}

func (bc *bconsumer) consumeMsg(stati *statiBenchmarkConsumerSnapshot, exit chan struct{}) {
	// TODO
	//consumer.Subscribe(c.topic, c.expression, func(m *rocketmq.MessageExt) rocketmq.ConsumeStatus {
	//atomic.AddInt64(&stati.receiveMessageTotal, 1)
	//now := time.Now().UnixNano() / int64(time.Millisecond)
	//b2cRT := now - m.BornTimestamp
	//atomic.AddInt64(&stati.born2ConsumerTotalRT, b2cRT)
	//s2cRT := now - m.StoreTimestamp
	//atomic.AddInt64(&stati.store2ConsumerTotalRT, s2cRT)

	//for {
	//old := atomic.LoadInt64(&stati.born2ConsumerMaxRT)
	//if old >= b2cRT || atomic.CompareAndSwapInt64(&stati.born2ConsumerMaxRT, old, b2cRT) {
	//break
	//}
	//}

	//for {
	//old := atomic.LoadInt64(&stati.store2ConsumerMaxRT)
	//if old >= s2cRT || atomic.CompareAndSwapInt64(&stati.store2ConsumerMaxRT, old, s2cRT) {
	//break
	//}
	//}

	//return rocketmq.ConsumeSuccess
	//})
}

func (bc *bconsumer) run(args []string) {
	bc.flags.Parse(args)
	if bc.topic == "" {
		println("empty topic")
		bc.usage()
		return
	}

	if bc.groupPrefix == "" {
		println("empty group prefix")
		bc.usage()
		return
	}

	if bc.nameSrv == "" {
		println("empty name server")
		bc.usage()
		return
	}

	if bc.testMinutes <= 0 {
		println("test time must be positive integer")
		bc.usage()
		return
	}

	if bc.instanceCount <= 0 {
		println("thread count must be positive integer")
		bc.usage()
		return
	}

	groupID := bc.groupPrefix
	if bc.isPrefixEnable {
		groupID += fmt.Sprintf("_%d", time.Now().UnixNano()/int64(time.Millisecond)%100)
	}

	c := consumer.NewPullConsumer( // FIXME
		bc.groupID, strings.Split(bc.nameSrv, ","), log.Std,
	)

	err := c.Start()
	if err != nil {
		fmt.Printf("start cosnumer error:%s\n", err)
		return
	}
	defer c.Shutdown()
	bc.c = c

	stati := statiBenchmarkConsumerSnapshot{}
	snapshots := consumeSnapshots{cur: &stati}
	exitChan := make(chan struct{})

	wg := sync.WaitGroup{}

	go func() {
		wg.Add(1)
		bc.consumeMsg(&stati, exitChan)
		wg.Done()
	}()

	// snapshot
	go func() {
		wg.Add(1)
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				snapshots.takeSnapshot()
			case <-exitChan:
				ticker.Stop()
				return
			}
		}
	}()

	// print statistic
	go func() {
		wg.Add(1)
		defer wg.Done()
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-ticker.C:
				snapshots.printStati()
			case <-exitChan:
				ticker.Stop()
				return
			}
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-time.Tick(time.Minute * time.Duration(bc.testMinutes)):
	case <-signalChan:
	}

	println("Closed")
	close(exitChan)
	wg.Wait()
	snapshots.takeSnapshot()
	snapshots.printStati()
}

func (bc *bconsumer) usage() {
	bc.flags.Usage()
}
