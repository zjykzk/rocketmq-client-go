package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeoutLocker(t *testing.T) {
	l := newTimeoutLocker()
	assert.True(t, l.tryLock(time.Second))
	assert.False(t, l.tryLock(time.Millisecond))
	l.unlock()
	assert.True(t, l.tryLock(time.Millisecond))
}
