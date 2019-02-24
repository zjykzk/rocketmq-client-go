package route

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	tq, tq1 := &TopicQueue{}, &TopicQueue{}
	assert.True(t, tq.Equal(tq1))

	tq.BrokerName = "b"
	assert.False(t, tq.Equal(tq1))

	*tq1 = *tq
	tq.ReadCount = 1
	assert.False(t, tq.Equal(tq1))

	*tq1 = *tq
	tq.WriteCount = 1
	assert.False(t, tq.Equal(tq1))

	*tq1 = *tq
	tq.Perm = 10
	assert.False(t, tq.Equal(tq1))

	*tq1 = *tq
	tq.SyncFlag = 10
	assert.False(t, tq.Equal(tq1))

	*tq1 = *tq
	assert.True(t, tq.Equal(tq1))

	tqs := []*TopicQueue{
		&TopicQueue{
			BrokerName: "b",
		},
		&TopicQueue{
			BrokerName: "a",
		},
	}

	SortTopicQueue(tqs)
	assert.Equal(t, "a", tqs[0].BrokerName)
	assert.Equal(t, "b", tqs[1].BrokerName)
}

func TestPerm(t *testing.T) {
	assert.Equal(t, "---", PermToString(0))
	assert.Equal(t, "R--", PermToString(PermRead))
	assert.Equal(t, "-W-", PermToString(PermWrite))
	assert.Equal(t, "--X", PermToString(PermInherit))
	assert.Equal(t, "RWX", PermToString(PermRead|PermWrite|PermInherit))

	assert.True(t, IsReadable(PermRead))
	assert.True(t, IsWritable(PermWrite))
	assert.True(t, IsInherited(PermInherit))
	assert.True(t, IsReadable(PermRead|PermWrite))
	assert.True(t, IsWritable(PermRead|PermWrite))
	assert.True(t, IsInherited(PermInherit|PermRead|PermWrite))
}
