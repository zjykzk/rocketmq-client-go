package remote

import (
	"errors"
	"fmt"
)

var (
	errTimeout      = errors.New("timeout")
	errConnClosed   = errors.New("connection closed")
	errConnDeactive = errors.New("connection deactive")
)

// IsTimeoutError timeout error
func IsTimeoutError(err error) bool {
	return err == errTimeout
}

// RPCError rpc error wraper
type RPCError struct {
	Code    Code
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("code:%d,message:%s", e.Code, e.Message)
}

func requestError(err error) *RPCError {
	return &RPCError{Code: -7, Message: err.Error()}
}

func dataError(err error) *RPCError {
	return &RPCError{Code: -8, Message: err.Error()}
}

func responseError(cmd *Command) *RPCError {
	return &RPCError{Code: cmd.Code, Message: cmd.Remark}
}

func brokerError(cmd *Command) error {
	return &RPCError{Code: cmd.Code, Message: cmd.Remark}
}

// BrokerError new error which represents error returned by the broker
func BrokerError(cmd *Command) *RPCError {
	return &RPCError{Code: cmd.Code, Message: cmd.Remark}
}

// RequestError new error which presents the request error, such connect timeout .eg
func RequestError(err error) *RPCError {
	return &RPCError{Code: -7, Message: err.Error()}
}

// DataError new error whichi represents the error which cannot process the response data
func DataError(err error) *RPCError {
	return &RPCError{Code: -8, Message: err.Error()}
}
