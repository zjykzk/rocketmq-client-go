package rpc

import (
	"strconv"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
)

// SendHeader send request header
type SendHeader struct {
	Group                 string
	Topic                 string
	DefaultTopic          string
	DefaultTopicQueueNums int32
	QueueID               uint8
	SysFlag               int32
	BornTimestamp         int64
	Flag                  int32
	Properties            string
	ReconsumeTimes        int32
	UnitMode              bool
	Batch                 bool
	MaxReconsumeTimes     int32
}

// ToMap serialzes to the map
func (h *SendHeader) ToMap() map[string]string {
	return map[string]string{
		"producerGroup":         h.Group,
		"topic":                 h.Topic,
		"defaultTopic":          h.DefaultTopic,
		"defaultTopicQueueNums": strconv.FormatInt(int64(h.DefaultTopicQueueNums), 10),
		"queueId":               strconv.FormatInt(int64(h.QueueID), 10),
		"sysFlag":               strconv.FormatInt(int64(h.SysFlag), 10),
		"bornTimestamp":         strconv.FormatInt(h.BornTimestamp, 10),
		"flag":                  strconv.FormatInt(int64(h.Flag), 10),
		"properties":            h.Properties,
		"reconsumeTimes":        strconv.FormatInt(int64(h.ReconsumeTimes), 10),
		"unitMode":              strconv.FormatBool(h.UnitMode),
		"batch":                 strconv.FormatBool(h.Batch),
		"maxReconsumeTimes":     strconv.FormatInt(int64(h.MaxReconsumeTimes), 10),
	}
}

// SendResponse send response
type SendResponse struct {
	Code    remote.Code
	Message string
	Version int16

	MsgID         string
	QueueOffset   int64
	QueueID       int32
	RegionID      string
	TraceOn       bool
	TransactionID string
}

// SendMessageSync send message to broker sync
func SendMessageSync(
	client remote.Client, addr string, body []byte, h *SendHeader, to time.Duration,
) (
	*SendResponse, error,
) {
	cmd, err := client.RequestSync(addr, remote.NewCommandWithBody(SendMessage, h, body), to)
	if err != nil {
		return nil, requestError(err)
	}

	resp := &SendResponse{Code: cmd.Code, Message: cmd.Remark}
	switch cmd.Code {
	case FlushDiskTimeout, FlushSlaveTimeout, SlaveNotAvailable:
	case Success:
	default:
		return resp, nil
	}

	resp.Code, resp.Message = cmd.Code, cmd.Remark
	resp.MsgID = cmd.ExtFields["msgId"]
	queueID, err := strconv.ParseInt(cmd.ExtFields["queueId"], 10, 32)
	if err != nil {
		return nil, dataError(err)
	}
	resp.QueueID = int32(queueID)

	queueOffset, err := strconv.ParseInt(cmd.ExtFields["queueOffset"], 10, 64)
	if err != nil {
		return nil, dataError(err)
	}
	resp.QueueOffset, resp.RegionID = queueOffset, cmd.ExtFields[message.PropertyMsgRegion]

	if resp.RegionID == "" {
		resp.RegionID = rocketmq.DefaultTraceRegionID
	}

	traceOn := cmd.ExtFields[message.PropertyTraceSwitch]
	if traceOn != "" {
		b, err := strconv.ParseBool(traceOn)
		if err != nil {
			return nil, dataError(err)
		}

		resp.TraceOn = b
	}

	return resp, nil
}
