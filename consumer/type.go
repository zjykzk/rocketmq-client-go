package consumer

import "strconv"

// Type consume type
type Type int8

func (t Type) String() string {
	if t < 0 || int(t) >= len(typeDescs) {
		panic("BUG:unknown type:" + strconv.Itoa(int(t)))
	}
	return typeDescs[t]
}

var typeDescs = []string{"CONSUME_ACTIVELY", "CONSUME_PASSIVELY"}

const (
	// Pull cosume message by pulling
	Pull Type = iota
	// Push cosume message by pushing
	Push
)
