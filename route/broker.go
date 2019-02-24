package route

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/zjykzk/rocketmq-client-go"
)

// Broker describes broker
type Broker struct {
	Cluster   string           `json:"cluster"`
	Name      string           `json:"brokerName"`
	Addresses map[int32]string `json:"brokerAddrs"`
}

// Equal compares with other broker
func (b *Broker) Equal(o *Broker) bool {
	if b.Cluster != o.Cluster {
		return false
	}

	if b.Name != o.Name {
		return false
	}

	if len(b.Addresses) != len(o.Addresses) {
		return false
	}

	for k, v := range b.Addresses {
		v1, ok := o.Addresses[k]
		if !ok {
			return false
		}

		if v1 != v {
			return false
		}
	}

	return true
}

// SelectAddress selects a (preferably master) broker address from the registered list.
// If the master's address cannot be found, a slave broker address is selected in a random manner.
func (b *Broker) SelectAddress() string {
	if addr, ok := b.Addresses[rocketmq.MasterID]; ok {
		return addr
	}
	l := len(b.Addresses)
	if l == 0 {
		return ""
	}

	n := rand.Intn(l)
	for _, addr := range b.Addresses {
		if n == 0 {
			return addr
		}
		n--
	}

	panic("IMPOSSIBLE")
}

func (b *Broker) String() string {
	return fmt.Sprintf("Broker [cluster=%s, name=%s, addresses=%v]", b.Cluster, b.Name, b.Addresses)
}

type brokerSorter []*Broker

func (s brokerSorter) Len() int           { return len(s) }
func (s brokerSorter) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s brokerSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SortBrokerData sort the Broker slice by broker name
func SortBrokerData(d []*Broker) {
	sort.Sort(brokerSorter(d))
}
