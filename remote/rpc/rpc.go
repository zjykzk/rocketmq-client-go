package rpc

import "github.com/zjykzk/rocketmq-client-go/remote"

// RPC contains the rpc
type RPC struct {
	client remote.Client
}

// NewRPC create the remoting rpc
func NewRPC(c remote.Client) *RPC {
	return &RPC{client: c}
}
