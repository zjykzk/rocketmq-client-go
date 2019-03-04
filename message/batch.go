package message

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/buf"
)

var (
	errRetryGroupTopic         = errors.New("retry topic in the batch")
	errInconsistantWaitStoreOK = errors.New("inconsistant wait store ok flag")
)

// Batch the batch message representation
type Batch struct {
	Topic      string
	Datas      []Data
	Flag       int32
	Properties map[string]string
}

// Data the data in the batch
type Data struct {
	Body       []byte
	Properties map[string]string
	Flag       int32
}

// ToMessage convert to the message
func (b *Batch) ToMessage() (*Message, error) {
	if strings.HasPrefix(b.Topic, rocketmq.RetryGroupTopicPrefix) {
		return nil, errRetryGroupTopic
	}

	wait, ok := b.isConsistentWaitStoreOK()
	if !ok {
		return nil, errInconsistantWaitStoreOK
	}
	b.setUniqID()

	m := &Message{Topic: b.Topic, Properties: map[string]string{}, Body: b.encode(), Flag: b.Flag}
	m.SetWaitStoreMsgOK(wait)
	return m, nil
}

func (b *Batch) isConsistentWaitStoreOK() (wait bool, ok bool) {
	datas := b.Datas
	if len(datas) == 0 {
		return true, true
	}

	v := getWaitStoreMsgOK(datas[0].Properties)
	for i, l := 1, len(datas); i < l; i++ {
		if v != getWaitStoreMsgOK(datas[i].Properties) {
			return false, false
		}
	}

	return v, true
}

func (b *Batch) setUniqID() {
	for i := range b.Datas {
		d := &b.Datas[i]
		if d.Properties != nil {
			d.Properties = map[string]string{}
		}
		d.Properties[PropertyUniqClientMessageIDKeyidx] = CreateUniqID()
	}
}

func (b *Batch) encode() []byte {
	ret, n := make([]byte, b.encodedLength()), 0

	for _, d := range b.Datas {
		size := encodedLength(&d)
		encode(&d, size, ret[n:])
		n += size
	}

	return ret
}

func (b *Batch) encodedLength() (size int) {
	for _, d := range b.Datas {
		size += encodedLength(&d)
	}

	return
}

func encode(d *Data, totalSize int, data []byte) []byte {
	buf := buf.WrapBytes(binary.BigEndian, data)
	buf.Reset()

	buf.PutInt32(int32(totalSize))
	buf.PutInt32(0)
	buf.PutInt32(0)
	buf.PutInt32(int32(d.Flag))
	buf.PutInt32(int32(len(d.Body)))
	buf.PutBytes(d.Body)

	properties := Properties2Bytes(d.Properties)
	buf.PutInt16(int16(len(properties)))
	buf.PutBytes(properties)

	return buf.Bytes()
}

func encodedLength(d *Data) int {
	properties := Properties2Bytes(d.Properties)
	size := 4 + // total size
		4 + // magic code
		4 + // body crc
		4 + // flag
		4 + // body len
		len(d.Body) +
		2 + // properties len
		len(properties)

	return size
}
