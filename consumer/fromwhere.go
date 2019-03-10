package consumer

import "strconv"

type fromWhere int

func (f fromWhere) String() string {
	if f < 0 || int(f) >= len(fromWhereDescs) {

		panic("BUG: unknow from where:" + strconv.Itoa(int(f)))
	}
	return fromWhereDescs[f]
}

var fromWhereDescs = []string{
	"CONSUME_FROM_LAST_OFFSET",
	"CONSUME_FROM_FIRST_OFFSET",
	"CONSUME_FROM_TIMESTAMP",
}

const (
	// ConsumeFromLastOffset from the offset stored, if not found from the maxoffset
	ConsumeFromLastOffset fromWhere = iota
	// ConsumeFromFirstOffset from the offset stored, if not found from the zeor
	ConsumeFromFirstOffset
	// ConsumeFromTimestamp from the offset specified by the timestamp
	ConsumeFromTimestamp
)
