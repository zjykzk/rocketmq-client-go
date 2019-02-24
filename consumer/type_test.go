package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	assert.Equal(t, "CONSUME_ACTIVELY", Pull.String())
	assert.Equal(t, "CONSUME_PASSIVELY", Push.String())
}
