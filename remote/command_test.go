package remote

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type s struct {
	I int8
	S string
}

func (s *s) ToMap() map[string]string {
	return map[string]string{
		"i": strconv.FormatInt(int64(s.I), 10),
		"s": s.S,
	}
}

func TestCommand(t *testing.T) {

	h := &s{
		I: int8(123),
		S: "ssssss",
	}
	t.Run("no body", func(t *testing.T) {
		cmd := NewCommand(UpdateAndCreateTopic, h)
		assert.Equal(t, 2, len(cmd.ExtFields))
		bs, err := encode(cmd)
		if err != nil {
			t.Fatal(err)
		}
		packet, err := ReadPacket(bytes.NewReader(bs))
		if err != nil {
			t.Fatal(err)
		}
		cmd1, err := decode(packet)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, cmd.Code, cmd1.Code)
		assert.Equal(t, cmd.ExtFields, cmd1.ExtFields)
		assert.Equal(t, cmd.Flag, cmd1.Flag)
		assert.Equal(t, cmd.Language, cmd1.Language)
		assert.Equal(t, cmd.Opaque, cmd1.Opaque)
		assert.Equal(t, cmd.Remark, cmd1.Remark)
		assert.Equal(t, cmd.Version, cmd1.Version)

		bs1, err := cmd.Encode(nil)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, len(bs), len(bs1))
		cmd1 = &Command{}
		packet, err = cmd.Read(bytes.NewReader(bs))
		if err != nil {
			t.Fatal(err)
		}
		cmd2, err := cmd1.Decode(packet)
		if err != nil {
			t.Fatal(err)
		}
		cmd1 = cmd2.(*Command)

		assert.Equal(t, cmd.Code, cmd1.Code)
		assert.Equal(t, cmd.ExtFields, cmd1.ExtFields)
		assert.Equal(t, cmd.Flag, cmd1.Flag)
		assert.Equal(t, cmd.Language, cmd1.Language)
		assert.Equal(t, cmd.Opaque, cmd1.Opaque)
		assert.Equal(t, cmd.Remark, cmd1.Remark)
		assert.Equal(t, cmd.Version, cmd1.Version)

	})

	t.Run("body", func(t *testing.T) {
		cmd := NewCommandWithBody(UpdateAndCreateTopic, h, []byte("body"))
		bs, err := encode(cmd)
		if err != nil {
			t.Fatal(err)
		}
		packet, err := ReadPacket(bytes.NewReader(bs))
		cmd1, err := decode(packet)
		assert.Nil(t, err)

		if cmd1 != nil {

			assert.Equal(t, []byte("body"), cmd1.Body)
		}
	})
}
