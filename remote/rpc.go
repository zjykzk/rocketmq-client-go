package remote

// RPC contains the rpc
type RPC struct {
	client Client
}

// NewRPC create the remoting rpc
func NewRPC(c Client) *RPC {
	return &RPC{client: c}
}
