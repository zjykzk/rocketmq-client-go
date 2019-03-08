package rocketmq

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	runShutdown := false
	shutdownFunc := func() { runShutdown = true }
	// bad state
	s := &Server{
		State:      StateStopped,
		StartFunc:  func() error { return nil },
		Shutdowner: &ShutdownCollection{shutdowns: []Shutdowner{ShutdownFunc(shutdownFunc)}},
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

func TestShutdownFuncCollection(t *testing.T) {
	fs := &ShutdownCollection{}
	count := 0
	fs.AddFuncs(func() { count++ }, func() { count += 2 })
	fs.Shutdown()
	assert.Equal(t, 3, count)
}
