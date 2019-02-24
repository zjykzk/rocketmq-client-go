package producer

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

type rpc interface {
	SendMessageSync(addr string, body []byte, h *remote.SendHeader, to time.Duration) (*remote.SendResponse, error)
}
