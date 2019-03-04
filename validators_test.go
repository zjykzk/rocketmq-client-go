package rocketmq

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckTopic(t *testing.T) {
	// empty topic
	assert.Equal(t, errEmptyTopic, CheckTopic(""))
	assert.Equal(t, errEmptyTopic, CheckTopic(" "))
	assert.Equal(t, errEmptyTopic, CheckTopic("\v"))
	assert.Equal(t, errEmptyTopic, CheckTopic("\t"))

	// long topic
	assert.Equal(t, errLongTopic, CheckTopic(strings.Repeat("1", maxTopicLen+1)))

	// mismatch the pattern
	assert.Equal(t, errMismatchPattern, CheckTopic("@#a"))
	assert.Equal(t, nil, CheckTopic("%0987654321abcdefghijklimopgrstovwxyzABCDEFGHIJKLIMOPGRSTOVWXYZ"))

	// default topic
	assert.Equal(t, errDefaultTopic, CheckTopic(DefaultTopic))
}
