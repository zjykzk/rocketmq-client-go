package route

import "testing"

func TestClusterInfo(t *testing.T) {
	c := &ClusterInfo{
		BrokerAddr:  map[string]*Broker{"a": &Broker{}},
		ClusterAddr: map[string][]string{"c": []string{"a", "b"}},
	}

	t.Logf("clusterinfo:[%s]", c.String())
}
