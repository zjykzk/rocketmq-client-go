package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestParseResetOffsetBody(t *testing.T) {
	_, err := parseResetOffsetRequest("")
	assert.NotNil(t, err)

	_, err = parseResetOffsetRequest("{")
	assert.NotNil(t, err)

	// empty
	r, err := parseResetOffsetRequest("{}")
	assert.Nil(t, err)
	assert.Nil(t, r)

	// empty
	r, err = parseResetOffsetRequest(`{"offsetTable":{}}`)
	assert.Nil(t, err)
	assert.Nil(t, r)

	// empty
	r, err = parseResetOffsetRequest(`{"offsetTable":{}}`)
	assert.Nil(t, err)
	assert.Nil(t, r)

	// queueID
	r, err = parseResetOffsetRequest(`{"offsetTable":{{"queueID":1}:2}}`)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), r[message.Queue{QueueID: 1}])

	// brokerName,queueID
	r, err = parseResetOffsetRequest(`{"offsetTable":{{"brokerName":"broker name","queueID":1}:3}}`)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), r[message.Queue{QueueID: 1, BrokerName: "broker name"}])

	// brokerName,queueID,topic
	r, err = parseResetOffsetRequest(`{"offsetTable":{{"brokerName":"broker name","queueID":1,"topic":"topic"}:4}}`)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), r[message.Queue{QueueID: 1, BrokerName: "broker name", Topic: "topic"}])

	// mutli brokerName,queueID,topic
	r, err = parseResetOffsetRequest(`
{"offsetTable":{
	{"brokerName":"broker name","queueID":1,"topic":"topic"}:4,
	{"brokerName":"broker name2","queueID":2,"topic":"topic2"}:5
}}`)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), r[message.Queue{QueueID: 1, BrokerName: "broker name", Topic: "topic"}])
	assert.Equal(t, int64(5), r[message.Queue{QueueID: 2, BrokerName: "broker name2", Topic: "topic2"}])
}
