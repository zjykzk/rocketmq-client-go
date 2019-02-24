package route

import (
	"fmt"
	"sort"
)

// TopicQueue the topic queue information in the broker
type TopicQueue struct {
	BrokerName string `json:"brokerName"`
	ReadCount  int    `json:"readQueueNums"`
	WriteCount int    `json:"writeQueueNums"`
	Perm       int    `json:"perm"`
	SyncFlag   int    `json:"topicSynFlag"`
}

// Equal judge equals with other topic queue
func (tq *TopicQueue) Equal(o *TopicQueue) bool {
	if tq.BrokerName != o.BrokerName {
		return false
	}

	if tq.ReadCount != o.ReadCount {
		return false
	}

	if tq.WriteCount != o.WriteCount {
		return false
	}

	if tq.Perm != o.Perm {
		return false
	}

	return tq.SyncFlag == o.SyncFlag
}

func (tq *TopicQueue) String() string {
	return fmt.Sprintf(
		"TopicQueue [brokerName=%s,readCount=%d,writeCount=%d,perm=%d,topicSynFlag=%d]",
		tq.BrokerName, tq.ReadCount, tq.WriteCount, tq.Perm, tq.SyncFlag)
}

type topicQueueSorter []*TopicQueue

func (s topicQueueSorter) Len() int           { return len(s) }
func (s topicQueueSorter) Less(i, j int) bool { return s[i].BrokerName < s[j].BrokerName }
func (s topicQueueSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SortTopicQueue sort the queues of topic by broker name
func SortTopicQueue(q []*TopicQueue) {
	sort.Sort(topicQueueSorter(q))
}

const (
	PermInherit = 1 << iota
	PermWrite
	PermRead
	PermPriority
)

func PermToString(perm int) string {
	ret := [...]byte{'-', '-', '-'}

	if IsReadable(perm) {
		ret[0] = 'R'
	}

	if IsWritable(perm) {
		ret[1] = 'W'
	}

	if IsInherited(perm) {
		ret[2] = 'X'
	}

	return string(ret[:])
}

func IsReadable(perm int) bool {
	return perm&PermRead == PermRead
}

func IsWritable(perm int) bool {
	return perm&PermWrite == PermWrite
}

func IsInherited(perm int) bool {
	return perm&PermInherit == PermInherit
}
