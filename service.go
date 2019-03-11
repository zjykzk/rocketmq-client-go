package rocketmq

import (
	"errors"
	"fmt"
	"sync/atomic"
)

var errNotRunning = errors.New("server not running")

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

// Shutdowner shutdown interface
type Shutdowner interface {
	Shutdown()
}

// ShutdownFunc shutdown func
type ShutdownFunc func()

// Shutdown call shutdown func
func (f ShutdownFunc) Shutdown() {
	f()
}

// ShutdownCollection shutdowner collection
type ShutdownCollection struct {
	shutdowns []Shutdowner
}

// AddLastFuncs add shutdown funcs
func (c *ShutdownCollection) AddLastFuncs(fs ...func()) {
	for _, f := range fs {
		c.shutdowns = append(c.shutdowns, ShutdownFunc(f))
	}
}

// AddLast append shutdowne funcs to the end
func (c *ShutdownCollection) AddLast(s ...Shutdowner) {
	c.shutdowns = append(c.shutdowns, s...)
}

// AddFirstFuncs insert the shutdowner at the beginning
func (c *ShutdownCollection) AddFirstFuncs(fs ...func()) {
	s := make([]Shutdowner, len(fs))
	for i, f := range fs {
		s[i] = ShutdownFunc(f)
	}
	c.AddFirst(s...)
}

// AddFirst insert the shutdowner at the beginning
func (c *ShutdownCollection) AddFirst(s ...Shutdowner) {
	l := len(s)
	r := make([]Shutdowner, len(c.shutdowns)+l)
	copy(r, s)
	copy(r[l:], c.shutdowns)

	c.shutdowns = r
}

// Shutdown call all the added shutdown func, in the added order
func (c *ShutdownCollection) Shutdown() {
	for _, shutdown := range c.shutdowns {
		shutdown.Shutdown()
	}
}

// Server server with start&shutdown operation
type Server struct {
	State      State
	StartFunc  func() error
	Shutdowner Shutdowner
}

// Start starts server
// it checks the state, if current is not StateCreating, returns started failed error
func (s *Server) Start() error {
	if s.StartFunc == nil {
		return errors.New("empty start func")
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

// CheckRunning check the server is under running state, return false if not
func (s *Server) CheckRunning() error {
	if s.State.Get() != StateRunning {
		return errNotRunning
	}
	return nil
}

// Shutdown shutdown server
// it checks the state,if current state is not StateRunning return false
func (s *Server) Shutdown() {
	if !s.State.Set(StateRunning, StateStopped) {
		return
	}
	if s.Shutdowner != nil {
		s.Shutdowner.Shutdown()
	}
}
