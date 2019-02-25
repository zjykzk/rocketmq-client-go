package latency

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFaultItem(t *testing.T) {
	f1 := &faultItem{name: "f1", latencyMillis: 1, availableTimeMillis: 20}
	f2 := &faultItem{name: "f2", latencyMillis: 1, availableTimeMillis: 20}
	assert.False(t, f1.less(f2))
	assert.False(t, f2.less(f1))

	now := unixMillis()
	f1.availableTimeMillis, f2.availableTimeMillis = now, now
	f2.latencyMillis = 2
	assert.True(t, f1.less(f2))
	assert.False(t, f2.less(f1))

	now = unixMillis()
	f1.availableTimeMillis, f2.availableTimeMillis = now, now
	f2.latencyMillis = 1
	f2.availableTimeMillis += 21
	assert.True(t, f1.less(f2))
	assert.False(t, f2.less(f1))

	now = unixMillis()
	f1.availableTimeMillis, f2.availableTimeMillis = now-1, now-1
	f2.availableTimeMillis -= 19
	assert.False(t, f1.less(f2))
	assert.True(t, f2.less(f1))

	now = unixMillis()
	f1.availableTimeMillis, f2.availableTimeMillis = now+1, now+1
	f2.latencyMillis = 100
	f2.availableTimeMillis -= 100
	r12 := f1.less(f2)
	r21 := f2.less(f1)
	assert.False(t, r12)
	assert.True(t, r21)
}

func TestFaultColl(t *testing.T) {
	fc := &faultColl{
		coll: make(map[string]*faultItem),
	}

	assert.True(t, fc.Available("not exist"))

	n, ok := fc.PickOneAtLeast()
	assert.False(t, ok)

	fc.UpdateFault("f1", 0, 0)
	assert.True(t, fc.Available("f1"))

	n, ok = fc.PickOneAtLeast()
	assert.True(t, ok)
	assert.Equal(t, "f1", n)

	fc.UpdateFault("f2", 0, 1000)
	assert.False(t, fc.Available("f2"))

	fc.UpdateFault("f2", 0, 0)
	c := 10
	for n, ok := fc.PickOneAtLeast(); ok; n, ok = fc.PickOneAtLeast() {
		assert.True(t, ok)
		if n == "f2" {
			break
		}
		c--
		if c <= 0 {
			t.Log("canot not detect the 'f2'")
			break
		}
	}

	assert.True(t, fc.Remove("f1"))
	n, ok = fc.PickOneAtLeast()
	assert.True(t, ok)
	assert.Equal(t, "f2", n)
	t.Log(fc)
}
