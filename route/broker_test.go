package route

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBroker(t *testing.T) {
	b := &Broker{Cluster: "c", Name: "n", Addresses: map[int32]string{0: "addr"}}
	assert.True(t, b.Equal(b))
	o := &Broker{Cluster: "c", Name: "n", Addresses: map[int32]string{0: "addr"}}
	assert.True(t, b.Equal(o))

	b.Addresses[2] = "slave"
	assert.Equal(t, "addr", b.SelectAddress())

	delete(b.Addresses, 0)
	assert.Equal(t, "slave", b.SelectAddress())

	assert.False(t, b.Equal(o))

	bs := []*Broker{&Broker{Name: "b"}, &Broker{Name: "a"}}
	SortBrokerData(bs)
	assert.Equal(t, "a", bs[0].Name)
	assert.Equal(t, "b", bs[1].Name)
}
