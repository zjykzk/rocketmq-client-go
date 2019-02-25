package message

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStr2Property(t *testing.T) {
	p := map[string]string{
		"a": "a",
		"b": "b",
	}

	p0 := String2Properties(Properties2String(p))

	for k, v := range p {
		assert.Equal(t, v, p0[k])
	}
}

func TestMessage(t *testing.T) {
	m := &Message{Topic: "topic", Body: []byte("body"), Properties: make(map[string]string)}
	m.SetKey("keys")
	assert.Equal(t, "keys", m.Properties[PropertyKeys])

	m.PutProperty("k", "v")
	assert.Equal(t, 2, len(m.Properties))
	assert.Equal(t, "v", m.GetProperty("k"))

	m.SetTags("tag")
	assert.Equal(t, "tag", m.GetTags())

	m.SetKeys([]string{"k1", "k2"})
	assert.Equal(t, "k1 k2", m.Properties[PropertyKeys])

	assert.Equal(t, 0, m.GetDelayTimeLevel())
	m.SetDelayTimeLevel(123)
	assert.Equal(t, 123, m.GetDelayTimeLevel())

	assert.True(t, m.GetWaitStoreMsgOK())
	m.SetWaitStoreMsgOK(false)
	assert.False(t, m.GetWaitStoreMsgOK())

	assert.Equal(t, "", m.GetUniqID())
	m.SetUniqID("u")
	assert.Equal(t, "u", m.GetUniqID())

	// FROM THE ROCKETMQ JAVA-SDK ut
	//	id = "645100FA00002A9F000000489A3AA09E"
	//  topic = "abc"
	//  body = "hello!q!"
	//  bornHost = "127.0.0.1", 0
	//  commitLogOffset = 123456
	//  preparedTransactionOffset = 0
	//  queueId = 0
	//  queueOffset = 123
	//  reconsumeTimes = 0
	//  properties = {
	//  "a": "123"
	//  "b": "hello"
	//  "c": "3.14"
	// }
	data := "0000007BDAA320A7000000000000000000000000000000000000007B000000000001E2400000000000000161554B21B47F00000100000000000000000000000064643BD4000000000000000000000000000000000000000868656C6C6F217121036162630015610131323302620168656C6C6F026301332E313402"

	db, err := hex.DecodeString(data)
	assert.Nil(t, err)

	msgs, err := Decode(db)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(msgs))
	mext := msgs[0]
	assert.Equal(t, "abc", mext.Topic)
	assert.Equal(t, "hello!q!", string(mext.Body))
	assert.Equal(t, []byte{127, 0, 0, 1}, mext.BornHost.Host)
	assert.Equal(t, uint16(0), mext.BornHost.Port)
	assert.Equal(t, int64(123456), mext.CommitLogOffset)
	assert.Equal(t, int64(0), mext.PreparedTransactionOffset)
	assert.Equal(t, uint8(0), mext.QueueID)
	assert.Equal(t, int64(123), mext.QueueOffset)
	assert.Equal(t, int32(0), mext.ReconsumeTimes)
	assert.Equal(t, map[string]string{"a": "123", "b": "hello", "c": "3.14"}, mext.Properties)
}
