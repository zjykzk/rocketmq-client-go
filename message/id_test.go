package message

import (
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkCreateUniqID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CreateUniqID()
	}
}

func TestUniqID(t *testing.T) {
	t.Log(fixString)
	now := time.Now()
	id := CreateUniqID()
	assert.True(t, strings.HasPrefix(id, fixString))

	s := id[len(fixString):]
	d, err := hex.DecodeString(s)
	assert.Nil(t, err)
	assert.Equal(t, uint32(now.Unix()), binary.BigEndian.Uint32(d))
	assert.Equal(t, uint8(0), d[4])
	assert.Equal(t, uint8(0), d[5])
	assert.Equal(t, uint8(1), d[6])
}

func TestID(t *testing.T) {
	addr := &Addr{Host: []byte{192, 168, 1, 1}, Port: 22}
	id := CreateMessageID(addr, 20)
	t.Log(id)
	t.Log(addr.String())

	addr1, commitOffset, err := ParseMessageID(id)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, addr, &addr1)
	assert.Equal(t, int64(20), commitOffset)

	assert.True(t, IsMessageID(id))
	assert.False(t, IsMessageID("111"))
	assert.True(t, IsMessageID("12345678901234567890123456789012"))
	assert.True(t, IsMessageID("r2345678901234567890123456789012"))
	assert.True(t, IsMessageID("f2345678901234567890123456789012"))
	assert.True(t, IsMessageID("fF345678901234567890123456789012"))
	assert.True(t, IsMessageID("aF345678901234567890123456789012"))
	assert.True(t, IsMessageID("aA345678901234567890123456789012"))
	assert.True(t, IsMessageID("aA3456789012345678901234567890X2"))
}
