package rpc

import (
	"encoding/json"
	"time"

	"github.com/zjykzk/rocketmq-client-go/remote"
)

type getConsumerIDsHeader string

func (h getConsumerIDsHeader) ToMap() map[string]string {
	return map[string]string{"consumerGroup": string(h)}
}

// GetConsumerIDs get the client id from the broker
func GetConsumerIDs(client remote.Client, addr, group string, to time.Duration) (
	ids []string, err error,
) {
	g := getConsumerIDsHeader(group)
	cmd, err := client.RequestSync(addr, remote.NewCommand(getConsumerListByGroup, g), to)
	if err != nil {
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
		return
	}

	if len(cmd.Body) == 0 {
		return
	}

	rp := &struct {
		IDs []string `json:"consumerIdList"`
	}{}
	err = json.Unmarshal(cmd.Body, rp)
	if err == nil {
		ids = rp.IDs
	} else {
		err = dataError(err)
	}
	return
}
