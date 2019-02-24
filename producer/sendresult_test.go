package producer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSendResult(t *testing.T) {
	assert.Equal(t, sendStatusDescs[0], OK.String())
	assert.Equal(t, sendStatusDescs[1], FlushDiskTimeout.String())
	assert.Equal(t, sendStatusDescs[2], FlushSlaveTimeout.String())
	assert.Equal(t, sendStatusDescs[3], SlaveNotAvailable.String())
	defer func() {
		if ok := recover(); ok != nil {
			return
		}
		assert.True(t, false)
	}()
	SendStatus(len(sendStatusDescs)).String()
	assert.True(t, false)
}
