package producer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFaultItem(t *testing.T) {
	// equal
	now := time.Now()
	f1 := &faultItem{name: "f1", latency: 1, availableTime: now}
	f2 := &faultItem{name: "f2", latency: 1, availableTime: now}
	assert.False(t, f1.less(f2))
	assert.False(t, f2.less(f1))

	// latency less
	f1.availableTime, f2.availableTime = now, now
	f2.latency = 2
	assert.True(t, f1.less(f2))
	assert.False(t, f2.less(f1))

	// latency first, then available time
	now = time.Now()
	f1.availableTime, f2.availableTime = now, now
	f2.latency = 1
	f2.availableTime = f2.availableTime.Add(time.Second)
	assert.True(t, f1.less(f2))
	assert.False(t, f2.less(f1))

	// latency first, then available time
	now = time.Now()
	f1.availableTime, f2.availableTime = now.Add(-time.Second), now.Add(-time.Second)
	f2.availableTime = f2.availableTime.Add(-19 * time.Second)
	assert.False(t, f1.less(f2))
	assert.True(t, f2.less(f1))

	now = time.Now()
	f1.availableTime, f2.availableTime = now.Add(time.Second), now.Add(time.Second)
	f2.latency = 100
	f2.availableTime = f2.availableTime.Add(-100 * time.Second)
	assert.False(t, f1.less(f2))
	assert.True(t, f2.less(f1))
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
