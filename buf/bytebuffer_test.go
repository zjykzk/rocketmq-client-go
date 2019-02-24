package buf

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteBuffer(t *testing.T) {
	bb := NewByteBuffer(binary.BigEndian)

	bb.PutByte(byte(1))
	bb.PutBytes([]byte("byte"))
	bb.PutInt8(int8(8))
	bb.PutInt16(int16(16))
	bb.PutInt32(int32(32))
	bb.PutInt64(int64(64))
	bb.PutUint8(uint8(8))
	bb.PutUint16(uint16(16))
	bb.PutUint32(uint32(32))
	bb.PutUint64(uint64(64))

	b, err := bb.GetByte()
	assert.Nil(t, err)
	assert.Equal(t, byte(1), b)

	bs, err := bb.GetBytes(4)
	assert.Nil(t, err)
	assert.Equal(t, []byte("byte"), bs)

	i8, err := bb.GetInt8()
	assert.Nil(t, err)
	assert.Equal(t, int8(8), i8)

	i16, err := bb.GetInt16()
	assert.Nil(t, err)
	assert.Equal(t, int16(16), i16)

	i32, err := bb.GetInt32()
	assert.Nil(t, err)
	assert.Equal(t, int32(32), i32)

	i64, err := bb.GetInt64()
	assert.Nil(t, err)
	assert.Equal(t, int64(64), i64)

	ui8, err := bb.GetUint8()
	assert.Nil(t, err)
	assert.Equal(t, uint8(8), ui8)

	ui16, err := bb.GetUint16()
	assert.Nil(t, err)
	assert.Equal(t, uint16(16), ui16)

	ui32, err := bb.GetUint32()
	assert.Nil(t, err)
	assert.Equal(t, uint32(32), ui32)

	ui64, err := bb.GetUint64()
	assert.Nil(t, err)
	assert.Equal(t, uint64(64), ui64)
}
