package remote

import (
	"fmt"
)

// Response header
type Response struct {
	Code    Code
	Message string
	Version int16
}

func (resp *Response) String() string {
	return fmt.Sprintf("Response: [code=%d,message=%s,version=%d]",
		resp.Code, resp.Message, resp.Version)
}
