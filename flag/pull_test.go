package flag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPull(t *testing.T) {
	f := BuildPull(true, true, true)
	assert.Equal(t, 1, f&PullCommitOffset)
	f = ClearCommitOffset(f)
	assert.Equal(t, 0, f&PullCommitOffset)
}
