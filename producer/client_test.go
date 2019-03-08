package producer

import (
	"time"

	"github.com/zjykzk/rocketmq-client-go/client"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
)

type fakeMQClient struct {
	sendMessageSyncErr error
	sendResponse       rpc.SendResponse
}

func (f *fakeMQClient) RegisterProducer(p client.Producer) error {
	return nil
}
func (f *fakeMQClient) UnregisterProducer(group string) {

}
func (f *fakeMQClient) SendMessageSync(
	broker string, body []byte, h *rpc.SendHeader, timeout time.Duration,
) (
	*rpc.SendResponse, error,
) {
	return &f.sendResponse, f.sendMessageSyncErr
}

func (f *fakeMQClient) UpdateTopicRouterInfoFromNamesrv(topic string) error {
	return nil
}
