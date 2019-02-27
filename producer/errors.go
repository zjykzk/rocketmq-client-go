package producer

import "errors"

var (
	errEmptyMessage = errors.New("empty message")
	errEmptyTopic   = errors.New("empty topic")
	errEmptyBody    = errors.New("empty body")
	errNoRouters    = errors.New("no routers")
)
