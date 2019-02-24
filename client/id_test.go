package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMQClientID(t *testing.T) {
	assert.Equal(t, "ip@instance@unit", BuildMQClientID("ip", "unit", "instance"))
	assert.Equal(t, "ip@@unit", BuildMQClientID("ip", "unit", ""))
	assert.Equal(t, "ip@instance", BuildMQClientID("ip", "   ", "instance"))
	assert.Equal(t, "ip@instance", BuildMQClientID("ip", "", "instance"))
}
