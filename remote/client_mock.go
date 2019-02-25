package remote

import "time"

type MockClient struct {
}

func (m *MockClient) RequestSync(addr string, cmd *Command, timeout time.Duration) (*Command, error) {
	return nil, nil
}
func (m *MockClient) RequestOneway(addr string, cmd *Command) error { return nil }
func (m *MockClient) Start() error                                  { return nil }
func (m *MockClient) Shutdown()                                     {}
