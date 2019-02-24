package route

import (
	"bytes"
	"fmt"
	"strings"
)

// ClusterInfo the cluster address information
type ClusterInfo struct {
	BrokerAddr  map[string]*Broker  `json:"brokerAddrTable"`  // key:broker name
	ClusterAddr map[string][]string `json:"clusterAddrTable"` // key:cluster name, value:broker name
}

func (c *ClusterInfo) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	buf.WriteString("BrokerAddr:[")
	if len(c.BrokerAddr) != 0 {
		for k, v := range c.BrokerAddr {
			buf.WriteString(fmt.Sprintf("%s=%s,", k, v.String()))
		}
	}
	buf.WriteString("],ClusterAddr:[")
	if len(c.ClusterAddr) > 0 {
		for k, v := range c.ClusterAddr {
			buf.WriteString(fmt.Sprintf("%s=%s", k, strings.Join(v, ",")))
		}
	}
	buf.WriteByte(']')

	return string(buf.Bytes())
}
