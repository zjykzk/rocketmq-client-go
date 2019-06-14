package remote

import (
	"errors"
)

var (
	errTimeout      = errors.New("timeout")
	errConnClosed   = errors.New("connection closed")
	errConnDeactive = errors.New("connection deactive")
)
