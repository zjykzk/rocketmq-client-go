package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/client/rpc"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/remote"
)

type fakeRemoteClient struct {
	remote.MockClient

	requestSyncErr error
	command        remote.Command
}

func (f *fakeRemoteClient) RequestSync(string, *remote.Command, time.Duration) (*remote.Command, error) {
	return &f.command, f.requestSyncErr
}

func fakeClient() *MqClient {
	c := newMQClient(&Config{}, "", log.Std)
	c.Client = &fakeRemoteClient{}
	return c
}

func TestSendMessageSync(t *testing.T) {
	c := fakeClient()

	// no broker
	resp, err := c.SendMessageSync("", []byte{}, &rpc.SendHeader{}, time.Second)
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	c.brokerAddrs.put("", map[int32]string{0: "a"})
	resp, err = c.SendMessageSync("", []byte{}, &rpc.SendHeader{}, time.Second)
	assert.NotNil(t, err)
	assert.Nil(t, resp)
}
