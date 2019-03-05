package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zlib"

	"github.com/zjykzk/rocketmq-client-go/buf"
)

// Message the message
type Message struct {
	Topic      string
	Body       []byte
	Properties map[string]string
	Flag       int32
}

// SetKey update keys
func (m *Message) SetKey(keys string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyKeys] = keys
}

// PutProperty update property
func (m *Message) PutProperty(k, v string) {
	m.makeSurePropertiesInst()
	m.Properties[k] = v
}

// ClearProperty remove the property
func (m *Message) ClearProperty(k string) {
	delete(m.Properties, k)
}

// GetProperty get the property byt the specify key
func (m *Message) GetProperty(k string) string {
	return m.Properties[k]
}

// GetTags return the property of the tags
func (m *Message) GetTags() string {
	return m.Properties[PropertyTags]
}

// SetTags set the property of the tags
func (m *Message) SetTags(tags string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyTags] = tags
}

// SetKeys update the property of the keys with multi-value, split with space
func (m *Message) SetKeys(ks []string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyKeys] = strings.Join(ks, KeySep)
}

// GetDelayTimeLevel returns the property of the delay time level
func (m *Message) GetDelayTimeLevel() int {
	l := m.Properties[PropertyDelayTimeLevel]
	if l == "" {
		return 0
	}
	i, _ := strconv.Atoi(l) // IGNORE
	return i
}

// SetDelayTimeLevel update the property of the delay time level
func (m *Message) SetDelayTimeLevel(l int) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyDelayTimeLevel] = strconv.Itoa(l)
}

// SetWaitStoreMsgOK update the property of the waiting store msg ok
func (m *Message) SetWaitStoreMsgOK(ok bool) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyWaitStoreMsgOK] = strconv.FormatBool(ok)
}

// GetWaitStoreMsgOK returns the property of the waiting store msag ok
func (m *Message) GetWaitStoreMsgOK() bool {
	return getWaitStoreMsgOK(m.Properties)
}

func getWaitStoreMsgOK(properties map[string]string) bool {
	switch ok := properties[PropertyWaitStoreMsgOK]; ok {
	case "", "true":
		return true
	case "false":
		return false
	default:
		panic("BUG")
	}
}

// SetConsumeStartTimestamp update the property of the consuming start timestamp
func (m *Message) SetConsumeStartTimestamp(timestamp int64) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyConsumeStartTimestamp] = strconv.FormatInt(timestamp, 10)
}

// GetConsumeStartTimestamp returns the property of the consuming start timestamp
func (m *Message) GetConsumeStartTimestamp() (timestamp int64, ok bool) {
	l, ok := m.Properties[PropertyConsumeStartTimestamp]
	if !ok {
		return
	}
	timestamp, _ = strconv.ParseInt(l, 10, 64) // IGNORE
	return
}

// GetUniqID returns the unique id from the properties
func (m *Message) GetUniqID() string {
	return GetUniqID(m.Properties)
}

// GetUniqID returns the unique id from the properties
func GetUniqID(properties map[string]string) string {
	return properties[PropertyUniqClientMessageIDKeyidx]
}

// SetUniqID set the unique id
func (m *Message) SetUniqID(uniqID string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyUniqClientMessageIDKeyidx] = uniqID
}

func (m *Message) makeSurePropertiesInst() {
	if m.Properties == nil {
		m.Properties = map[string]string{}
	}
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"Message [topic=%s, flag=%d, properties=%v, body=%s",
		m.Topic, m.Flag, m.Properties, m.Body,
	)
}

// Addr the ip address
type Addr struct {
	Host []byte
	Port uint16
}

func (addr *Addr) String() string {
	buf, e := bytes.Buffer{}, len(addr.Host)-1
	for i := 0; i < e; i++ {
		buf.WriteString(strconv.Itoa(int(addr.Host[i])))
		buf.WriteByte('.')
	}
	if e >= 0 {
		buf.WriteString(strconv.Itoa(int(addr.Host[e])))
	}
	buf.WriteByte(':')
	buf.WriteString(strconv.Itoa(int(addr.Port)))

	return string(buf.Bytes())
}

// Ext the message presentation with storage information
type Ext struct {
	Message
	StoreSize                 int32
	QueueOffset               int64
	SysFlag                   int32
	BornTimestamp             int64
	BornHost                  Addr
	StoreTimestamp            int64
	StoreHost                 Addr
	MsgID                     string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
	QueueID                   uint8
}

func (m *Ext) String() string {
	return fmt.Sprintf("MessageExt:[Message=%s,QueueID=%d,StoreSize=%d,QueueOffset=%d,SysFlag=%d,"+
		"BornTimestamp=%d,BornHost=%s,StoreTimestamp=%d,StoreHost=%s,MsgID=%s,CommitLogOffset=%d,"+
		"BodyCRC=%d,ReconsumeTimes=%d,PreparedTransactionOffset=%d]", m.Message.String(), m.QueueID,
		m.StoreSize, m.QueueOffset, m.SysFlag, m.BornTimestamp, m.BornHost.String(), m.StoreTimestamp,
		m.StoreHost.String(), m.MsgID, m.CommitLogOffset, m.BodyCRC, m.ReconsumeTimes,
		m.PreparedTransactionOffset)
}

// Decode decodes the bytes to messages
func Decode(d []byte) (msgs []*Ext, err error) {
	buf := buf.WrapBytes(binary.BigEndian, d)

AGAIN:
	if buf.Len() == 0 {
		return
	}
	m := &Ext{}
	m.StoreSize, err = buf.GetInt32()
	if err != nil {
		return
	}
	buf.GetInt32() // MagicCode
	m.BodyCRC, err = buf.GetInt32()
	if err != nil {
		return
	}

	queueID, err := buf.GetInt32()
	if err != nil {
		return
	}
	m.QueueID = uint8(queueID)

	m.Flag, err = buf.GetInt32()
	if err != nil {
		return
	}

	m.QueueOffset, err = buf.GetInt64()
	if err != nil {
		return
	}

	m.CommitLogOffset, err = buf.GetInt64()
	if err != nil {
		return
	}

	m.SysFlag, err = buf.GetInt32()
	if err != nil {
		return
	}

	m.BornTimestamp, err = buf.GetInt64()
	if err != nil {
		return
	}

	bs, err := buf.GetBytes(4)
	if err != nil {
		return
	}
	port, err := buf.GetInt32()
	if err != nil {
		return
	}
	m.BornHost.Host, m.BornHost.Port = bs, uint16(port)

	m.StoreTimestamp, err = buf.GetInt64()
	if err != nil {
		return
	}

	bs, err = buf.GetBytes(4)
	if err != nil {
		return
	}
	port, err = buf.GetInt32()
	if err != nil {
		return
	}
	m.StoreHost.Host, m.StoreHost.Port = bs, uint16(port)

	m.ReconsumeTimes, err = buf.GetInt32()
	if err != nil {
		return
	}

	m.PreparedTransactionOffset, err = buf.GetInt64()
	if err != nil {
		return
	}

	bodyLen, err := buf.GetInt32()
	if err != nil {
		return
	}

	if bodyLen > 0 {
		bs, err = buf.GetBytes(int(bodyLen))
		if (m.SysFlag & Compress) == Compress {
			z, err := zlib.NewReader(bytes.NewReader(bs))
			if err != nil {
				return nil, err
			}

			bs, err = ioutil.ReadAll(z)
			if err != nil {
				return nil, err
			}
			z.Close()
		}
		m.Body = bs
	}

	topicLen, err := buf.GetInt8()
	bs, err = buf.GetBytes(int(topicLen))
	if err != nil {
		return
	}
	m.Topic = string(bs)

	propertiesLen, err := buf.GetInt16()
	if err != nil {
		return
	}

	bs, err = buf.GetBytes(int(propertiesLen))
	if err != nil {
		return
	}

	m.Properties = String2Properties(string(bs))
	m.MsgID = CreateMessageID(&m.StoreHost, m.CommitLogOffset)

	msgs = append(msgs, m)
	goto AGAIN
}

// predefined consts
const (
	MagicCodePostion      = 4
	FlagPostion           = 16
	PhysicOffsetPostion   = 28
	StoreTimestampPostion = 56
	MagicCode             = 0xAABBCCDD ^ 1880681586 + 8
	BodySizePosition      = 4 /* 1 TOTALSIZE*/ +
		4 /* 2 MAGICCODE */ +
		4 /* 3 BODYCRC*/ +
		4 /* 4 QUEUEID*/ +
		4 /* 5 FLAG */ +
		8 /* 6 QUEUEOFFSET */ +
		8 /* 7 PHYSICALOFFSET */ +
		4 /* 8 SYSFLAG */ +
		8 /* 9 BORNTIMESTAMP */ +
		8 /* 10 BORNHOST */ +
		8 /* 11 STORETIMESTAMP */ +
		8 /* 12 STOREHOSTADDRESS */ +
		4 /* 13 RECONSUMETIMES */ +
		8 /* 14 Prepared Transaction Offset */

	NameValueSep = byte(1)
	PropertySep  = byte(2)
)

var (
	nameValueSepStr = string([]byte{NameValueSep})
	propertySepStr  = string([]byte{PropertySep})
)

// Properties2Bytes converts properties to byte array
func Properties2Bytes(properties map[string]string) []byte {
	if len(properties) == 0 {
		return nil
	}

	bs, n := make([]byte, propertiesLength(properties)), 0
	for k, v := range properties {
		n += copy(bs[n:], k)
		bs[n] = NameValueSep
		n++

		n += copy(bs[n:], v)
		bs[n] = PropertySep
		n++
	}

	return bs
}

func propertiesLength(properties map[string]string) (size int) {
	for k, v := range properties {
		size += len(k) + 1 + len(v) + 1
	}
	return
}

// Properties2String converts properties to string
func Properties2String(properties map[string]string) string {
	return string(Properties2Bytes(properties))
}

// String2Properties converts string to map
func String2Properties(properties string) map[string]string {
	ret := make(map[string]string, 32)
	for _, p := range strings.Split(properties, propertySepStr) {
		nv := strings.Split(p, nameValueSepStr)
		if len(nv) == 2 {
			ret[nv[0]] = nv[1]
		}
	}

	return ret
}
