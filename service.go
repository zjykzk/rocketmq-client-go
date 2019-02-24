package rocketmq

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// State job state
type State int32

// Set update value atomic
func (s *State) Set(o, n State) bool {
	return atomic.CompareAndSwapInt32((*int32)(s), int32(o), int32(n))
}

// Get get the value atomic
func (s *State) Get() State {
	return State(atomic.LoadInt32((*int32)(s)))
}

func (s State) String() string {
	switch s {
	case StateCreating:
		return "Createing"
	case StateRunning:
		return "Running"
	case StateStopped:
		return "Stopped"
	case StateStartFailed:
		return "StartFailed"
	default:
		panic(fmt.Sprintf("BUG:unknow state:%d", s))
	}
}

const (
	// StateCreating job is creating
	StateCreating State = iota
	// StateRunning job is running
	StateRunning
	// StateStopped job is stopped
	StateStopped
	// StateStartFailed job starts failed
	StateStartFailed
)

// Server server with start&shutdown operation
type Server struct {
	State        State
	StartFunc    func() error
	ShutdownFunc func()
}

// Start starts server
// it checks the state, if current is not StateCreating, returns started failed error
func (s *Server) Start() error {
	if s.StartFunc == nil {
		return errors.New("empty start func")
	}

	if s.ShutdownFunc == nil {
		return errors.New("empty shutdown func")
	}

	if !s.State.Set(StateCreating, StateStartFailed) {
		return fmt.Errorf("Start failed, since current state is:%s", s.State.Get())
	}

	if err := s.StartFunc(); err != nil {
		return err
	}
	s.State.Set(StateStartFailed, StateRunning)
	return nil
}

// Shutdown shutdown server
// it checks the state,if current state is not StateRunning return false
func (s *Server) Shutdown() {
	if !s.State.Set(StateRunning, StateStopped) {
		return
	}
	s.ShutdownFunc()
}
