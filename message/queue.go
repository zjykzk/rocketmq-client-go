package message

import (
	"sort"
	"strconv"
)

// Queue the consume queue in the broker
type Queue struct {
	Topic      string
	BrokerName string
	QueueID    uint8
}

func (q *Queue) String() string {
	return "Queue:[Topic=" + q.Topic +
		",Broker=" + q.BrokerName +
		",QueueID=" + strconv.Itoa(int(q.QueueID)) + "]"
}

// HashKey returns the hash key of the instance
func (q *Queue) HashKey() string {
	return q.BrokerName + "@" + strconv.Itoa(int(q.QueueID)) + "@" + q.Topic
}

type queueSorter []*Queue

func (s queueSorter) Len() int      { return len(s) }
func (s queueSorter) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s queueSorter) Less(i, j int) bool {
	si, sj := s[i], s[j]
	if si.Topic != sj.Topic {
		return si.Topic < sj.Topic
	}

	if si.BrokerName != sj.BrokerName {
		return si.BrokerName < sj.BrokerName
	}

	return si.QueueID < sj.QueueID
}

// SortQueue sort the consume queues
func SortQueue(queues []*Queue) {
	sort.Sort(queueSorter(queues))
}
