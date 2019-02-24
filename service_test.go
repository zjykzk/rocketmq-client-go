package rocketmq

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	runShutdown := false
	// bad state
	s := &Server{
		State:        StateStopped,
		StartFunc:    func() error { return nil },
		ShutdownFunc: func() { runShutdown = true },
	}
	assert.NotNil(t, s.Start())
	s.Shutdown()
	assert.False(t, runShutdown)

	// ok
	s.State = StateCreating
	assert.Nil(t, s.Start())
	s.Shutdown()
	assert.True(t, runShutdown)
	runShutdown = false

	// start func return error
	s.State = StateCreating
	s.StartFunc = func() error { return errors.New("start failed") }
	assert.NotNil(t, s.Start())
	s.Shutdown()
	assert.False(t, runShutdown)
}
