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
	var order []int
	fs.AddLastFuncs(
		func() { order = append(order, 1) },
		func() { order = append(order, 2) },
	)
	fs.AddLast(ShutdownFunc(func() { order = append(order, 5) }))
	fs.Shutdown()
	assert.Equal(t, []int{1, 2, 5}, order)

	order = []int{}
	fs.AddFirst(ShutdownFunc(func() { order = append(order, 3) }))
	fs.AddFirstFuncs(func() { order = append(order, 4) })
	fs.Shutdown()
	assert.Equal(t, []int{4, 3, 1, 2, 5}, order)
}
