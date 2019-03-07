package rpc

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/message"
	"github.com/zjykzk/rocketmq-client-go/remote"
)

type fakeRemoteClient struct {
	remote.FakeClient

	requestSyncErr error
	command        remote.Command
}

func (f *fakeRemoteClient) RequestSync(string, *remote.Command, time.Duration) (*remote.Command, error) {
	return &f.command, f.requestSyncErr
}

func TestSendMessageSync(t *testing.T) {
	rc := &fakeRemoteClient{}

	header := &SendHeader{}
	// request failed
	rc.requestSyncErr = errors.New("bad request")
	_, err := SendMessageSync(rc, "", []byte{}, header, time.Second)
	assert.NotNil(t, err)
	rc.requestSyncErr = nil

	// unknow code
	rc.command.Code = -1
	r, err := SendMessageSync(rc, "", []byte{}, header, time.Second)
	assert.Nil(t, err)
	assert.Equal(t, remote.Code(-1), r.Code)

	// bad queueID
	rc.command.Code = Success
	r, err = SendMessageSync(rc, "", []byte{}, header, time.Second)
	assert.NotNil(t, err)

	rc.command.ExtFields = map[string]string{
		"queueId": "3",
		"msgId":   "ID",
	}

	// bad offset
	r, err = SendMessageSync(rc, "", []byte{}, header, time.Second)
	assert.NotNil(t, err)

	// bad trace on
	rc.command.ExtFields["queueOffset"] = "4"
	rc.command.ExtFields[message.PropertyTraceSwitch] = "xxx"
	r, err = SendMessageSync(rc, "", []byte{}, header, time.Second)
	assert.NotNil(t, err)

	// OK
	rc.command.ExtFields[message.PropertyTraceSwitch] = "1"
	r, err = SendMessageSync(rc, "", []byte{}, header, time.Second)
	assert.Nil(t, err)

	assert.Equal(t, "ID", r.MsgID)
	assert.Equal(t, int32(3), r.QueueID)
	assert.Equal(t, int64(4), r.QueueOffset)
	assert.Equal(t, rocketmq.DefaultTraceRegionID, r.RegionID)
	assert.True(t, r.TraceOn)
}

func TestSendHeader(t *testing.T) {
	h := &SendHeader{}
	assert.Equal(t, map[string]string{
		"producerGroup":         "",
		"topic":                 "",
		"defaultTopic":          "",
		"defaultTopicQueueNums": "0",
		"queueId":               "0",
		"sysFlag":               "0",
		"bornTimestamp":         "0",
		"flag":                  "0",
		"properties":            h.Properties,
		"reconsumeTimes":        "0",
		"unitMode":              "false",
		"batch":                 "false",
		"maxReconsumeTimes":     "0",
	}, h.ToMap())
}

func TestPullHeader(t *testing.T) {
	h := &PullHeader{
		ConsumerGroup:        "cg",
		Topic:                "TestPullHeader",
		QueueOffset:          7,
		MaxCount:             100,
		SysFlag:              77,
		CommitOffset:         17,
		SuspendTimeoutMillis: 27,
		Subscription:         "subscription TestPullHeader",
		SubVersion:           777,
		ExpressionType:       "expre",
		QueueID:              37,
	}
	assert.Equal(t, map[string]string{
		"consumerGroup":        "cg",
		"topic":                "TestPullHeader",
		"queueId":              "37",
		"queueOffset":          "7",
		"maxMsgNums":           "100",
		"sysFlag":              "77",
		"commitOffset":         "17",
		"suspendTimeoutMillis": "27",
		"subscription":         "subscription TestPullHeader",
		"subVersion":           "777",
		"expressionType":       "expre",
	}, h.ToMap())
}
