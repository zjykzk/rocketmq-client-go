package client

import (
	"errors"
	"strconv"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/message"
)

// ResetOffsetRequest the request of resetting offset from the broker
type ResetOffsetRequest struct {
	Offsets map[message.Queue]int64
}

// the data has following format:
// {"offsetTable":{{"brokerName":"broker","queueId":1,"topic":"topic"}:1,{"queueId":0}:1}}
func parseResetOffsetRequest(d string) (r ResetOffsetRequest, err error) {
	d = strings.TrimSpace(d)
	if len(d) < 2 {
		err = errors.New("bad format")
		return
	}

	d = d[1:] // skip first '{'
	s := strings.IndexByte(d, '{')
	if s == -1 {
		return
	}
	s++
	d = d[s:] // skip second '{'

	for {
		of, adv, ok := nextQueue(d)
		if !ok {
			return
		}
		d = d[adv:]

		q, ok := parseQueue(of)
		if !ok {
			return
		}

		if r.Offsets == nil {
			r.Offsets = make(map[message.Queue]int64)
		}

		r.Offsets[q], adv = readFirstInt64(d)
		d = d[adv:]
	}
}

func nextQueue(d string) (string, int, bool) {
	s := strings.IndexByte(d, '{')
	if s == -1 {
		return "", 0, false
	}

	e := strings.IndexByte(d[s+1:], '}')
	if e == -1 {
		return "", 0, false
	}

	e = s + 1 + e + 1

	return d[s:e], e, true
}

func parseQueue(of string) (message.Queue, bool) {
	q, found, adv := message.Queue{}, false, 0

READ_FIELD:
	s := strings.IndexByte(of, '"')
	if s == -1 {
		return q, found
	}

	found, of = true, of[s+1:]
	switch of[0] {
	case 'b': // brokerName
		of = of[10+1:]
		q.BrokerName, adv = readFirstString(of)
	case 'q': // queueId
		of = of[7+1:]
		var n int64
		n, adv = readFirstInt64(of)
		q.QueueID = uint8(n)
	case 't': // topic
		of = of[5+1:]
		q.Topic, adv = readFirstString(of)
	}
	of = of[adv:]

	goto READ_FIELD
}

func readFirstString(d string) (string, int) {
	s := strings.IndexByte(d, '"')
	e := strings.IndexByte(d[s+1:], '"')
	r := d[s+1 : s+1+e]
	return r, s + 1 + e + 1
}

func readFirstInt64(d string) (int64, int) {
	isNum := func(r rune) bool { return r >= '0' && r <= '9' }
	s := strings.IndexFunc(d, isNum)
	e := strings.IndexFunc(d[s:], func(r rune) bool { return !isNum(r) })
	r, err := strconv.ParseInt(d[s:s+e], 10, 64)
	if err != nil {
		panic(err.Error())
	}
	return r, s + e
}
