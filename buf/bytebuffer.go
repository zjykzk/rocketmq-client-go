package buf

import (
	"bytes"
	"encoding/binary"
)

// ByteBuffer byte buffer
type ByteBuffer struct {
	*bytes.Buffer
	endian binary.ByteOrder
	b      [8]byte
}

// WrapBytes wraps the bytes
func WrapBytes(endian binary.ByteOrder, bs []byte) *ByteBuffer {
	return &ByteBuffer{
		endian: endian,
		Buffer: bytes.NewBuffer(bs),
	}
}

// NewByteBuffer new byte buffer
func NewByteBuffer(endian binary.ByteOrder) *ByteBuffer {
	return &ByteBuffer{
		Buffer: &bytes.Buffer{},
		endian: endian,
	}
}

// NewByteBufferWithSize new byte buffer
func NewByteBufferWithSize(endian binary.ByteOrder, size int) *ByteBuffer {
	return &ByteBuffer{
		Buffer: bytes.NewBuffer(make([]byte, 0, size)),
		endian: endian,
	}
}

// PutByte put byte
func (bb *ByteBuffer) PutByte(v byte) {
	bb.WriteByte(v)
}

// GetByte put byte
func (bb *ByteBuffer) GetByte() (v byte, err error) {
	v, err = bb.ReadByte()
	return
}

// PutBytes put byte
func (bb *ByteBuffer) PutBytes(v []byte) {
	bb.Write(v)
}

// GetBytes get bytes
func (bb *ByteBuffer) GetBytes(len int) (v []byte, err error) {
	v = make([]byte, len)
	_, err = bb.Read(v)
	return
}

// PutUint8 put uint8
func (bb *ByteBuffer) PutUint8(v uint8) {
	bb.WriteByte(byte(v))
}

// GetUint8 put uint8
func (bb *ByteBuffer) GetUint8() (v uint8, err error) {
	v0, err := bb.ReadByte()
	v = uint8(v0)
	return
}

// PutInt8 put uint8
func (bb *ByteBuffer) PutInt8(v int8) {
	bb.WriteByte(byte(v))
}

// GetInt8 put int8
func (bb *ByteBuffer) GetInt8() (v int8, err error) {
	v0, err := bb.ReadByte()
	v = int8(v0)
	return
}

// PutUint16 put uint16
func (bb *ByteBuffer) PutUint16(v uint16) {
	bs := bb.b[:2]
	bb.endian.PutUint16(bs, v)
	bb.Write(bs)
}

// GetUint16 get uint16
func (bb *ByteBuffer) GetUint16() (v uint16, err error) {
	bs := bb.b[:2]
	_, err = bb.Read(bs)
	if err != nil {
		return
	}

	v = bb.endian.Uint16(bs)
	return
}

// PutInt16 put uint16
func (bb *ByteBuffer) PutInt16(v int16) {
	bb.PutUint16(uint16(v))
}

// GetInt16 get int16
func (bb *ByteBuffer) GetInt16() (v int16, err error) {
	bs := bb.b[:2]
	_, err = bb.Read(bs)
	if err != nil {
		return
	}

	v = int16(bb.endian.Uint16(bs))
	return
}

// PutUint32 put uint32
func (bb *ByteBuffer) PutUint32(v uint32) {
	bs := bb.b[:4]
	bb.endian.PutUint32(bs, v)
	bb.Write(bs)
}

// GetUint32 get uint32
func (bb *ByteBuffer) GetUint32() (v uint32, err error) {
	bs := bb.b[:4]
	_, err = bb.Read(bs)
	if err != nil {
		return
	}

	v = bb.endian.Uint32(bs)
	return
}

// PutInt32 put uint64
func (bb *ByteBuffer) PutInt32(v int32) {
	bb.PutUint32(uint32(v))
}

// GetInt32 get int64
func (bb *ByteBuffer) GetInt32() (v int32, err error) {
	bs := bb.b[:4]
	_, err = bb.Read(bs)
	if err != nil {
		return
	}

	v = int32(bb.endian.Uint32(bs))
	return
}

// PutUint64 put uint64
func (bb *ByteBuffer) PutUint64(v uint64) {
	bs := bb.b[:8]
	bb.endian.PutUint64(bs, v)
	bb.Write(bs)
}

// GetUint64 get uint64
func (bb *ByteBuffer) GetUint64() (v uint64, err error) {
	bs := bb.b[:8]
	_, err = bb.Read(bs)
	if err != nil {
		return
	}

	v = bb.endian.Uint64(bs)
	return
}

// PutInt64 put uint64
func (bb *ByteBuffer) PutInt64(v int64) {
	bb.PutUint64(uint64(v))
}

// GetInt64 get int64
func (bb *ByteBuffer) GetInt64() (v int64, err error) {
	bs := bb.b[:8]
	_, err = bb.Read(bs)
	if err != nil {
		return
	}

	v = int64(bb.endian.Uint64(bs))
	return
}
