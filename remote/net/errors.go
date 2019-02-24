package net

import "errors"

var (
	errTimeout      = errors.New("timeout")
	errConnClosed   = errors.New("connection closed")
	errConnDeactive = errors.New("connection deactive")
)

// IsTimeoutError timeout error
func IsTimeoutError(err error) bool {
	return err == errTimeout
}
