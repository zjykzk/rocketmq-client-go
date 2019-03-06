package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPull(t *testing.T) {
	f := buildPullFlag(true, true, true)
	assert.Equal(t, int32(1), f&PullCommitOffset)
	f = ClearCommitOffset(f)
	assert.Equal(t, int32(0), f&PullCommitOffset)
}
