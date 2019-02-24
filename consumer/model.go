package consumer

import "strconv"

// Model consume message model type
type Model int8

func (m Model) String() string {
	if m < 0 || int(m) >= len(modelDescs) {
		panic("BUG:unknown model:" + strconv.Itoa(int(m)))
	}
	return modelDescs[m]
}

var modelDescs = []string{"BROADCASTING", "CLUSTERING"}

const (
	// BroadCasting consumed by all consumer
	BroadCasting Model = iota
	// Clustering consumed by different consumer
	Clustering
)
