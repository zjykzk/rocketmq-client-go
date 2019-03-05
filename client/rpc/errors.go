package rpc

import (
	"fmt"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

// Error rpc error wraper
type Error struct {
	Code    remote.Code
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("code:%d,message:%s", e.Code, e.Message)
}

func requestError(err error) *Error {
	return &Error{Code: -7, Message: err.Error()}
}

func dataError(err error) *Error {
	return &Error{Code: -8, Message: err.Error()}
}

func responseError(cmd *remote.Command) *Error {
	return &Error{Code: cmd.Code, Message: cmd.Remark}
}

func brokerError(cmd *remote.Command) *Error {
	return &Error{Code: cmd.Code, Message: cmd.Remark}
}
