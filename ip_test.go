package rocketmq

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIP(t *testing.T) {
	ip, err := GetIP()
	assert.Nil(t, err)
	t.Log(hex.EncodeToString(ip))

	t.Log(GetIPStr())
}
