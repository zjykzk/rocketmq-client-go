package message

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/buf"
	"github.com/zjykzk/rocketmq-client-go/flag"
)

// Message the message
type Message struct {
	Topic      string
	Body       []byte
	Properties map[string]string
	Flag       int32
}

func (m *Message) SetKey(keys string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyKeys] = keys
}

func (m *Message) PutProperty(k, v string) {
	m.makeSurePropertiesInst()
	m.Properties[k] = v
}

func (m *Message) ClearProperty(k string) {
	delete(m.Properties, k)
}

func (m *Message) GetProperty(k string) string {
	return m.Properties[k]
}

func (m *Message) GetTags() string {
	return m.Properties[PropertyTags]
}

func (m *Message) SetTags(tags string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyTags] = tags
}

func (m *Message) SetKeys(ks []string) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyKeys] = strings.Join(ks, KeySep)
}

func (m *Message) GetDelayTimeLevel() int {
	l := m.Properties[PropertyDelayTimeLevel]
	if l == "" {
		return 0
	}
	i, _ := strconv.Atoi(l) // IGNORE
	return i
}

func (m *Message) SetDelayTimeLevel(l int) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyDelayTimeLevel] = strconv.Itoa(l)
}

func (m *Message) SetWaitStoreMsgOK(ok bool) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyWaitStoreMsgOK] = strconv.FormatBool(ok)
}

func (m *Message) GetWaitStoreMsgOK() bool {
	switch ok := m.Properties[PropertyWaitStoreMsgOK]; ok {
	case "", "true":
		return true
	case "false":
		return false
	default:
		panic("BUG")
	}
}

func (m *Message) SetConsumeStartTimestamp(timestamp int64) {
	m.makeSurePropertiesInst()
	m.Properties[PropertyConsumeStartTimestamp] = strconv.FormatInt(timestamp, 10)
}

func (m *Message) GetConsumeStartTimestamp() (timestamp int64, ok bool) {
	l, ok := m.Properties[PropertyConsumeStartTimestamp]
	if !ok {
		return
	}
	timestamp, _ = strconv.ParseInt(l, 10, 64) // IGNORE
	return
}

// GetUniqID returns the unique id
func (m *Message) GetUniqID() string {
	return m.Properties[PropertyUniqClientMessageIDKeyidx]
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

type Addr struct {
	Host []byte
	Port uint16
}

func (addr *Addr) String() string {
	return fmt.Sprintf("%v:%d", addr.Host, addr.Port)
}

type MessageExt struct {
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

func (m *MessageExt) String() string {
	return fmt.Sprintf("MessageExt:[Message=%s,QueueID=%d,StoreSize=%d,QueueOffset=%d,SysFlag=%d,"+
		"BornTimestamp=%d,BornHost=%s,StoreTimestamp=%d,StoreHost=%s,MsgID=%s,CommitLogOffset=%d,"+
		"BodyCRC=%d,ReconsumeTimes=%d,PreparedTransactionOffset=%d]", m.Message.String(), m.QueueID,
		m.StoreSize, m.QueueOffset, m.SysFlag, m.BornTimestamp, m.BornHost.String(), m.StoreTimestamp,
		m.StoreHost.String(), m.MsgID, m.CommitLogOffset, m.BodyCRC, m.ReconsumeTimes,
		m.PreparedTransactionOffset)
}

// Decode decodes the bytes to messages
func Decode(d []byte) (msgs []*MessageExt, err error) {
	buf := buf.WrapBytes(binary.BigEndian, d)

AGAIN:
	if buf.Len() == 0 {
		return
	}
	m := &MessageExt{}
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
		if (m.SysFlag & flag.Compress) == flag.Compress {
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

// Properties2String converts properties to string
func Properties2String(properties map[string]string) string {
	if len(properties) == 0 {
		return ""
	}

	bs := make([]byte, 0, 1024)
	for k, v := range properties {
		bs = append(bs, []byte(k)...)
		bs = append(bs, NameValueSep)
		bs = append(bs, []byte(v)...)
		bs = append(bs, PropertySep)
	}
	return string(bs)
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
