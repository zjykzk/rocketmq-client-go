package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModel(t *testing.T) {
	assert.Equal(t, "BROADCASTING", BroadCasting.String())
	assert.Equal(t, "CLUSTERING", Clustering.String())
}
