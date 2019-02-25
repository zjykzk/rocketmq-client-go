package remote

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/zjykzk/rocketmq-client-go/buf"
)

const (
	// ProtoJSON json codec
	ProtoJSON = iota
	// ProtoRocketMQ rocket mq codec
	ProtoRocketMQ
)

const (
	responsType    = 1
	commandFlag    = 0
	commandVersion = 252
)

var opaque int32

// Command remoting command
type Command struct {
	Code      Code              `json:"code"`
	Language  LanguageCode      `json:"language"`
	Version   int16             `json:"version"`
	Opaque    int32             `json:"opaque"`
	Flag      int32             `json:"flag"`
	Remark    string            `json:"remark"`
	ExtFields map[string]string `json:"extFields"`
	Body      []byte            `json:"body,-"`
}

// HeaderOfMapper converts header to map
type HeaderOfMapper interface {
	ToMap() map[string]string
}

func nextOpaque() int32 {
	return atomic.AddInt32(&opaque, 1)
}

// ID returns the request ID
func (cmd *Command) ID() int64 {
	return int64(cmd.Opaque)
}

func (cmd *Command) isResponseType() bool {
	return cmd.Flag&(responsType) == responsType
}

func (cmd *Command) markResponseType() {
	cmd.Flag = (cmd.Flag | responsType)
}

//NewCommand create command with empty body
func NewCommand(code Code, header HeaderOfMapper) *Command {
	return NewCommandWithBody(code, header, nil)
}

//NewCommandWithBody with body
func NewCommandWithBody(code Code, header HeaderOfMapper, body []byte) *Command {
	cmd := &Command{
		Code:     code,
		Opaque:   nextOpaque(),
		Flag:     commandFlag,
		Language: gO,
		Version:  commandVersion,
		Body:     body,
	}

	if header != nil {
		cmd.ExtFields = header.ToMap()
	}
	return cmd
}

// Encode encode the command
func (cmd *Command) Encode(interface{}) ([]byte, error) {
	return encode(cmd)
}

// Decode decode the raw command data
func (cmd *Command) Decode(buf []byte) (interface{}, error) {
	return decode(buf)
}

// Read decode the raw command data
func (cmd *Command) Read(r io.Reader) ([]byte, error) {
	return ReadPacket(r)
}

func (cmd *Command) String() string {
	return fmt.Sprintf("code=%d,Language=%s,opaque=%d,flag=%d,remark=%s,ExtFields=%v,body=%s",
		cmd.Code, cmd.Language, cmd.Opaque, cmd.Flag, cmd.Remark, cmd.ExtFields, cmd.Body)
}

func encode(cmd *Command) ([]byte, error) {
	var (
		remarkBytes       []byte
		remarkBytesLen    int
		extFieldsBytes    []byte
		extFieldsBytesLen int
	)
	remarkBytesLen = 0
	if len(cmd.Remark) > 0 {
		remarkBytes = []byte(cmd.Remark)
		remarkBytesLen = len(remarkBytes)
	}
	if cmd.ExtFields != nil {
		extFieldsBytes = rocketMqCustomHeaderSerialize(cmd.ExtFields)
		extFieldsBytesLen = len(extFieldsBytes)
	}

	sz := 4 + 4 + 2 + 1 + 2 + 4 + 4 + 4 + remarkBytesLen + 4 + extFieldsBytesLen + len(cmd.Body)
	bb := buf.NewByteBufferWithSize(binary.BigEndian, sz)
	bb.PutInt32(int32(0)) // total length
	bb.PutInt32(int32(0)) // header length
	bb.PutInt16(cmd.Code.ToInt16())
	bb.PutInt8(cmd.Language.ToInt8())
	bb.PutInt16(cmd.Version)
	bb.PutInt32(cmd.Opaque)
	bb.PutInt32(cmd.Flag)
	bb.PutInt32(int32(remarkBytesLen))
	if remarkBytesLen > 0 {
		bb.PutBytes(remarkBytes)
	}
	bb.PutInt32(int32(extFieldsBytesLen))
	if extFieldsBytesLen > 0 {
		bb.PutBytes(extFieldsBytes)
	}

	headerLen := bb.Len() - 8 // 4 totallen + headerlen
	if len(cmd.Body) > 0 {
		bb.PutBytes(cmd.Body)
	}

	ret := bb.Bytes()
	fixLenAndProtoType(bb, headerLen)
	return ret, nil
}

func fixLenAndProtoType(bb *buf.ByteBuffer, headerLen int) {
	l := bb.Len()
	bb.Reset()
	bb.PutInt32(int32(l) - 4)
	bb.PutInt32(int32(headerLen&0xFFFFFF | (ProtoRocketMQ << 24))) // prototype[1 byte]|header [3 byte]
}

// ReadPacket read one packet
func ReadPacket(r io.Reader) ([]byte, error) {
	var totalLen int32
	err := binary.Read(r, binary.BigEndian, &totalLen)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, totalLen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func decode(buf []byte) (cmd *Command, err error) {
	totalLen, headerLen := int32(len(buf)), int32(binary.BigEndian.Uint32(buf))
	proto := headerLen >> 24
	headerLen = headerLen & 0xFFFFFF

	if totalLen < headerLen {
		return nil, errors.New("header length error")
	}
	switch proto {
	case ProtoJSON:
		cmd, err := fromJSONProto(buf[4 : headerLen+4])
		if err != nil {
			return nil, err
		}
		cmd.Body = buf[headerLen+4:]
		return cmd, nil
	case ProtoRocketMQ:
		return fromRocketMQProto(bytes.NewReader(buf[4:]), totalLen-headerLen-4)
	default:
		return nil, fmt.Errorf("unknow prototype:%d", headerLen>>24)
	}
}

func fromJSONProto(buf []byte) (*Command, error) {
	cmd := Command{}
	err := json.Unmarshal(buf, &cmd)
	return &cmd, err
}

func fromRocketMQProto(r io.Reader, bodyLen int32) (cmd *Command, err error) {
	cmd = &Command{}
	// int code(~32767)
	var code int16
	binary.Read(r, binary.BigEndian, &code)
	if err != nil {
		return nil, err
	}
	cmd.Code = int16ToCode(code)
	var lc int8
	binary.Read(r, binary.BigEndian, &lc)
	cmd.Language = int8ToLanguageCode(lc)
	// int version(~32767)
	binary.Read(r, binary.BigEndian, &cmd.Version)
	// int opaque
	binary.Read(r, binary.BigEndian, &cmd.Opaque)
	// int flag
	binary.Read(r, binary.BigEndian, &cmd.Flag)
	// String remark
	var remarkLen, extFieldsLen int32
	binary.Read(r, binary.BigEndian, &remarkLen)
	if remarkLen > 0 {
		var remarkData = make([]byte, remarkLen)
		binary.Read(r, binary.BigEndian, &remarkData)
		cmd.Remark = string(remarkData)
	}
	binary.Read(r, binary.BigEndian, &extFieldsLen)
	if extFieldsLen > 0 {
		var extFieldsData = make([]byte, extFieldsLen)
		binary.Read(r, binary.BigEndian, &extFieldsData)
		extFiledMap := customHeaderDeserialize(extFieldsData)
		cmd.ExtFields = extFiledMap
	}
	cmd.Body = make([]byte, bodyLen)
	r.Read(cmd.Body)
	return
}

func rocketMqCustomHeaderSerialize(extFields map[string]string) (byteData []byte) {
	sz := 0
	for k, v := range extFields {
		sz += 2 + len(k) + 4 + len(v)
	}

	bb := buf.NewByteBufferWithSize(binary.BigEndian, sz)
	for key, value := range extFields {
		bb.PutInt16(int16(len(key)))
		bb.PutBytes([]byte(key))
		bb.PutInt32(int32(len(value)))
		bb.PutBytes([]byte(value))
	}
	byteData = bb.Bytes()
	return
}

func customHeaderDeserialize(extFieldDataBytes []byte) (extFiledMap map[string]string) {
	if len(extFieldDataBytes) <= 0 {
		return
	}

	extFiledMap = make(map[string]string)
	bb := buf.WrapBytes(binary.BigEndian, extFieldDataBytes)

	for bb.Len() > 0 {
		i16, _ := bb.GetInt16()
		key, _ := bb.GetBytes(int(i16))
		i32, _ := bb.GetInt32()
		val, _ := bb.GetBytes(int(i32))
		extFiledMap[string(key)] = string(val)
	}
	return
}

func struct2Map(structBody interface{}) (resultMap map[string]string) {
	resultMap = make(map[string]string)
	value := reflect.ValueOf(structBody)
	for value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	valueType := value.Type()
	for i := 0; i < valueType.NumField(); i++ {
		field := valueType.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := field.Name
		smallName := strings.Replace(name, string(name[0]), string(strings.ToLower(string(name[0]))), 1)
		resultMap[smallName] = fmt.Sprintf("%v", value.FieldByName(name))
	}
	return
}
