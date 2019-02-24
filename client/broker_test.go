package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBroker(t *testing.T) {
	ba := &brokerAddrTable{table: make(map[string]map[int32]string)}
	ba.put("test", map[int32]string{0: "addr"})

	addrs := ba.brokerAddrs("test")
	assert.Equal(t, []brokerAddr{{brokerID: 0, addr: "addr"}}, addrs)

	addr, exist := ba.anyOneAddrOf("test")
	assert.True(t, exist)
	assert.Equal(t, brokerAddr{brokerID: 0, addr: "addr"}, addr)

	addr, exist = ba.anyOneAddrOf("no exist")
	assert.False(t, exist)

	assert.Equal(t, []string{"test"}, ba.brokerNames())

	ba.put("test1", map[int32]string{0: "addr"})
	assert.Equal(t, 2, ba.size())
}

func TestVersion(t *testing.T) {
	vt := brokerVersionTable{table: make(map[string]map[string]int32)}

	vt.put("testv", "test addr", 1)
	assert.Equal(t, 1, vt.get("testv", "test addr"))
	assert.Equal(t, 0, vt.get("not exist", "test addr"))
	assert.Equal(t, 0, vt.get("testv", "not exist"))
}
