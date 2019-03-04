package producer

import (
	"bytes"
	"math/rand"
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go/message"
)

var (
	latencyMax           = [...]time.Duration{50, 100, 550, 1000, 2000, 3000, 15000}      //millis
	notAvailableDuration = [...]time.Duration{0, 0, 30000, 60000, 120000, 180000, 600000} //millis
)

func init() {
	// fix the max latency and not available duration
	for i := range latencyMax {
		latencyMax[i] *= time.Millisecond
	}

	for i := range notAvailableDuration {
		notAvailableDuration[i] *= time.Millisecond
	}
}

type topicRouter interface {
	SelectOneQueue() *message.Queue
	NextQueueIndex() uint32
	MessageQueues() []*message.Queue
	WriteQueueCount(broker string) int
	SelectOneQueueNotOf(lastBroker string) *message.Queue
}

// MQFaultStrategy the strategy of fault
type MQFaultStrategy struct {
	sendLatencyFaultEnable bool
	faultLatency           *faultColl
}

// NewMQFaultStrategy creates on fault strategy
func NewMQFaultStrategy(sendEnable bool) *MQFaultStrategy {
	return &MQFaultStrategy{
		sendLatencyFaultEnable: sendEnable,
		faultLatency: &faultColl{
			coll:           make(map[string]*faultItem, 32),
			whereItemWorst: rand.Uint32(),
		},
	}
}

// SelectOneQueue select one message queue to send message
func (s *MQFaultStrategy) SelectOneQueue(tp topicRouter, exculdedBroker string) *message.Queue {
	if !s.sendLatencyFaultEnable {
		return tp.SelectOneQueueNotOf(exculdedBroker)
	}

	i, queues := tp.NextQueueIndex(), tp.MessageQueues()
	l := uint32(len(queues))
	for range queues {
		q := queues[i%l]
		i++

		if !s.faultLatency.Available(q.BrokerName) {
			continue
		}

		if "" == exculdedBroker || q.BrokerName != exculdedBroker {
			return q
		}
	}

	wc := tp.WriteQueueCount(exculdedBroker)
	if wc <= 0 {
		s.faultLatency.Remove(exculdedBroker)
		return tp.SelectOneQueueNotOf(exculdedBroker)
	}

	notBestBroker, ok := s.faultLatency.PickOneAtLeast()
	if !ok {
		return tp.SelectOneQueueNotOf(exculdedBroker)
	}

	q := tp.SelectOneQueue()
	return &message.Queue{
		BrokerName: notBestBroker,
		Topic:      q.Topic,
		QueueID:    uint8(tp.NextQueueIndex() % uint32(wc)),
	}
}

// UpdateFault update the latency
func (s *MQFaultStrategy) UpdateFault(broker string, latency time.Duration, isolation bool) {
	if !s.sendLatencyFaultEnable {
		return
	}

	newLatency := latency
	if isolation {
		newLatency = 30 * time.Second
	}

	var duration time.Duration
	for i := len(latencyMax) - 1; i >= 0; i-- {
		if newLatency >= latencyMax[i] {
			duration = notAvailableDuration[i]
			break
		}
	}

	s.faultLatency.UpdateFault(broker, latency, duration)
}

// Available returns true if the broker can server, false otherwise
func (s *MQFaultStrategy) Available(broker string) bool {
	return s.faultLatency.Available(broker)
}

func (s *MQFaultStrategy) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	buf.WriteString("MQFaultStrategy:[sendLatencyFaultEnable=")
	buf.WriteString(strconv.FormatBool(s.sendLatencyFaultEnable))
	buf.WriteString(",faultLatency=")
	buf.WriteString(s.faultLatency.String())
	buf.WriteByte(']')
	return string(buf.Bytes())
}
