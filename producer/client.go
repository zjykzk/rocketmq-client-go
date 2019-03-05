package producer

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
)

type mqClient interface {
	Start() error
	Shutdown()

	RegisterProducer(p client.Producer) error
	UnregisterProducer(group string)
	SendMessageSync(broker string, body []byte, h *rpc.SendHeader, timeout time.Duration) (*rpc.SendResponse, error)
	UpdateTopicRouterInfoFromNamesrv(topic string) error
}
