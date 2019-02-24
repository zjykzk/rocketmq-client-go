package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPullStatus(t *testing.T) {
	assert.Equal(t, "FOUND", Found.String())
	assert.Equal(t, "NO_NEW_MSG", NoNewMessage.String())
	assert.Equal(t, "NO_MATCHED_MSG", NoMatchedMessage.String())
	assert.Equal(t, "OFFSET_ILLEGAL", OffsetIllegal.String())
}
