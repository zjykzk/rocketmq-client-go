package remote

import "time"

type FakeClient struct {
}

func (m *FakeClient) RequestSync(addr string, cmd *Command, timeout time.Duration) (*Command, error) {
	return nil, nil
}
func (m *FakeClient) RequestOneway(addr string, cmd *Command) error { return nil }
func (m *FakeClient) Start() error                                  { return nil }
func (m *FakeClient) Shutdown()                                     {}
func (m *FakeClient) RequestAsync(addr string, cmd *Command, timeout time.Duration, callback func(*Command, error)) error {
	return nil
}
