package producer

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type faultItem struct {
	name                string
	latencyMillis       int64
	availableTimeMillis int64
}

func (i *faultItem) String() string {
	return fmt.Sprintf("faultItem:[name=%s,latencyMillis=%v,availableTimeMillis=%v]",
		i.name, i.latencyMillis, i.availableTimeMillis)
}

func (i *faultItem) available() bool {
	return unixMillis() >= i.availableTimeMillis
}

func unixMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (i *faultItem) less(o *faultItem) bool {
	if i.available() != o.available() {
		if i.available() {
			return true
		}

		return false
	}

	if i.latencyMillis < o.latencyMillis {
		return true
	} else if i.latencyMillis > o.latencyMillis {
		return false
	}

	switch {
	case i.latencyMillis < o.latencyMillis:
		return true
	case i.latencyMillis > o.latencyMillis:
		return false
	case i.availableTimeMillis < o.availableTimeMillis:
		return true
	case i.availableTimeMillis > o.availableTimeMillis:
		return false
	default:
		return false
	}
}

type faultColl struct {
	sync.RWMutex
	coll           map[string]*faultItem
	whereItemWorst uint32
}

func (fc *faultColl) UpdateFault(name string, latencyMillis, notAvailableDurationMillis int64) {
	fc.Lock()
	fc.coll[name] = &faultItem{
		name:                name,
		latencyMillis:       latencyMillis,
		availableTimeMillis: unixMillis() + notAvailableDurationMillis,
	}
	fc.Unlock()
}

func (fc *faultColl) Available(name string) bool {
	fc.RLock()
	i, ok := fc.coll[name]
	fc.RUnlock()
	if !ok {
		return true
	}
	return i.available()
}

func (fc *faultColl) Remove(name string) bool {
	fc.Lock()
	_, ok := fc.coll[name]
	if ok {
		delete(fc.coll, name)
	}
	fc.Unlock()
	return ok
}

func (fc *faultColl) PickOneAtLeast() (string, bool) {
	fc.RLock()
	l := len(fc.coll)
	is, i := make([]*faultItem, l), 0
	for _, v := range fc.coll {
		is[i] = v
		i++
	}
	fc.RUnlock()

	if l <= 0 {
		return "", false
	}

	if l == 1 {
		return is[0].name, true
	}

	// shuffle
	for i := l; i > 1; i-- {
		j := rand.Intn(i)
		is[j], is[i-1] = is[i-1], is[j]
	}

	sort.Sort(faultItemSorter(is))

	return is[atomic.AddUint32(&fc.whereItemWorst, 1)%uint32(l>>1)].name, true
}

func (fc *faultColl) String() string {
	fc.RLock()
	l := len(fc.coll)
	is, i := make([]*faultItem, l), 0
	for _, v := range fc.coll {
		is[i] = v
		i++
	}
	fc.RUnlock()

	buf := bytes.NewBuffer(make([]byte, 0, 256))
	buf.WriteString("faultColl:[")
	for _, v := range is {
		buf.WriteString(v.name)
		buf.WriteByte('=')
		buf.WriteString(v.String())
		buf.WriteByte(',')
	}
	buf.WriteByte(']')

	return string(buf.Bytes())
}

type faultItemSorter []*faultItem

func (s faultItemSorter) Len() int           { return len(s) }
func (s faultItemSorter) Less(i, j int) bool { return s[i].less(s[j]) }
func (s faultItemSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
