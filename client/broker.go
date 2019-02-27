package client

import (
	"sync"
)

type brokerAddrTable struct {
	sync.RWMutex
	table map[string]map[int32]string // brokerName->[brokerID->broker address], NOTE: donot operate it outside
}

type brokerAddr struct {
	brokerID int32
	addr     string
}

func (a *brokerAddrTable) put(name string, addrs map[int32]string) (old map[int32]string) {
	a.Lock()
	old = a.table[name]
	a.table[name] = addrs
	a.Unlock()
	return
}

func (a *brokerAddrTable) get(name string, brokerID int32) string {
	a.RLock()
	v := a.table[name]
	a.RUnlock()
	return v[brokerID]
}

func (a *brokerAddrTable) getByBrokerID(brokerID int32) []string {
	a.RLock()
	addrs := make([]string, 0, len(a.table))
	for _, v := range a.table {
		id, ok := v[brokerID]
		if ok {
			addrs = append(addrs, id)
		}
	}
	a.RUnlock()
	return addrs
}

func (a *brokerAddrTable) deleteBroker(name string) {
	a.Lock()
	delete(a.table, name)
	a.Unlock()
}

func (a *brokerAddrTable) deleteAddr(name string, brokerID int32) {
	a.Lock()
	addr, ok := a.table[name]
	if ok {
		delete(addr, brokerID)
	}
	a.Unlock()
}

func (a *brokerAddrTable) size() int {
	a.RLock()
	s := len(a.table)
	a.RUnlock()

	return s
}

func (a *brokerAddrTable) brokerNames() []string {
	a.RLock()
	names := make([]string, 0, len(a.table))
	for k := range a.table {
		names = append(names, k)
	}
	a.RUnlock()
	return names
}

func (a *brokerAddrTable) brokerAddrs(name string) []brokerAddr {
	var ret []brokerAddr
	a.RLock()
	addrs, ok := a.table[name]
	if ok {
		ret = make([]brokerAddr, len(addrs))
		i := 0
		for id, addr := range addrs {
			r := &ret[i]
			r.brokerID, r.addr = id, addr
			i++
		}
	}
	a.RUnlock()
	return ret
}

func (a *brokerAddrTable) anyOneAddrOf(brokerName string) (addr brokerAddr, exist bool) {
	a.RLock()
	addrs, ok := a.table[brokerName]
	if ok {
		for id, addr0 := range addrs {
			addr.brokerID, addr.addr = id, addr0
			exist = true
			break
		}
	}
	a.RUnlock()
	return
}

type brokerVersionTable struct {
	sync.RWMutex
	table map[string]map[string]int32 // brokerName->[broker address->version]
}

func (v *brokerVersionTable) put(name, addr string, version int32) {
	v.Lock()
	vs, ok := v.table[name]
	if !ok {
		vs = make(map[string]int32, 2)
		v.table[name] = vs
	}
	vs[addr] = int32(version)
	v.Unlock()
}

func (v *brokerVersionTable) get(name, addr string) (r int32) {
	v.RLock()
	vs, ok := v.table[name]
	if ok {
		r = vs[addr]
	}
	v.RUnlock()
	return
}
