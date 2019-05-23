package rpc

import (
	"encoding/json"
	"fmt"
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
	cmd, err := client.RequestSync(addr, remote.NewCommandWithBody(sendMessage, h, body), to)
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

// PullHeader pull message header
type PullHeader struct {
	ConsumerGroup        string
	Topic                string
	QueueOffset          int64
	MaxCount             int32
	SysFlag              int32
	CommitOffset         int64
	SuspendTimeoutMillis int64
	Subscription         string
	SubVersion           int64
	ExpressionType       string
	QueueID              uint8
}

// ToMap converts pull header to map
func (p *PullHeader) ToMap() map[string]string {
	return map[string]string{
		"consumerGroup":        p.ConsumerGroup,
		"topic":                p.Topic,
		"queueId":              strconv.FormatInt(int64(p.QueueID), 10),
		"queueOffset":          strconv.FormatInt(p.QueueOffset, 10),
		"maxMsgNums":           strconv.FormatInt(int64(p.MaxCount), 10),
		"sysFlag":              strconv.FormatInt(int64(p.SysFlag), 10),
		"commitOffset":         strconv.FormatInt(p.CommitOffset, 10),
		"suspendTimeoutMillis": strconv.FormatInt(p.SuspendTimeoutMillis, 10),
		"subscription":         p.Subscription,
		"subVersion":           strconv.FormatInt(p.SubVersion, 10),
		"expressionType":       p.ExpressionType,
	}
}

// PullResponse pull message response
type PullResponse struct {
	Code            remote.Code
	Message         string
	Version         int16
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	Messages        []*message.Ext
	SuggestBrokerID int64
}

// PullMessageSync pull message sync
func PullMessageSync(
	client remote.Client, addr string, header *PullHeader, to time.Duration,
) (
	*PullResponse, error,
) {
	cmd, e := client.RequestSync(addr, remote.NewCommand(pullMessage, header), to)
	if e != nil {
		return nil, requestError(e)
	}

	return convertCmdToPullResponse(cmd)
}

func convertCmdToPullResponse(cmd *remote.Command) (pr *PullResponse, err error) {
	switch cmd.Code {
	case Success, PullNotFound, PullRetryImmediately, PullOffsetMoved:
	default:
		return nil, brokerError(cmd)
	}

	pr = &PullResponse{Code: cmd.Code, Message: cmd.Remark, Version: cmd.Version}

	if cmd.ExtFields == nil {
		return
	}

	pr.NextBeginOffset, err = strconv.ParseInt(cmd.ExtFields["nextBeginOffset"], 10, 64)
	if err != nil {
		err = dataError(err)
		return
	}
	pr.MinOffset, err = strconv.ParseInt(cmd.ExtFields["minOffset"], 10, 64)
	if err != nil {
		err = dataError(err)
		return
	}
	pr.MaxOffset, err = strconv.ParseInt(cmd.ExtFields["maxOffset"], 10, 64)
	if err != nil {
		err = dataError(err)
		return
	}
	pr.SuggestBrokerID, err = strconv.ParseInt(cmd.ExtFields["suggestWhichBrokerId"], 10, 64)
	if err != nil {
		err = dataError(err)
		return
	}
	pr.Messages, err = message.Decode(cmd.Body)
	if err != nil {
		err = dataError(err)
	}
	return
}

type queryMessageByIDHeader int64

func (h queryMessageByIDHeader) ToMap() map[string]string {
	return map[string]string{
		"offset": strconv.FormatInt(int64(h), 10),
	}
}

// ViewMessageByOffset querys the message by message id
func ViewMessageByOffset(client remote.Client, addr string, offset int64, to time.Duration) (
	*message.Ext, error,
) {
	h := queryMessageByIDHeader(offset)
	cmd, err := client.RequestSync(addr, remote.NewCommand(viewMessageByID, h), to)
	if err != nil {
		return nil, err
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	msgs, err := message.Decode(cmd.Body)
	if err != nil {
		return nil, dataError(err)
	}

	return msgs[0], nil
}

// SendBackHeader send message back params
type SendBackHeader struct {
	CommitOffset      int64
	Group             string
	DelayLevel        int32
	MessageID         string
	Topic             string
	IsUnitMode        bool
	MaxReconsumeTimes int
}

// ToMap converts send back header to map
func (h *SendBackHeader) ToMap() map[string]string {
	return map[string]string{
		"offset":            strconv.FormatInt(h.CommitOffset, 10),
		"group":             h.Group,
		"delayLevel":        strconv.FormatInt(int64(h.DelayLevel), 10),
		"originMsgId":       h.MessageID,
		"originTopic":       h.Topic,
		"unitMode":          strconv.FormatBool(h.IsUnitMode),
		"maxReconsumeTimes": strconv.Itoa(h.MaxReconsumeTimes),
	}
}

// SendBack send back the message
func SendBack(client remote.Client, addr string, h *SendBackHeader, to time.Duration) (err error) {
	cmd, err := client.RequestSync(addr, remote.NewCommand(consumerSendMsgBack, h), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
	}
	return
}

type maxOffsetHeader struct {
	topic   string
	queueID uint8
}

func (h *maxOffsetHeader) ToMap() map[string]string {
	return map[string]string{
		"topic":   h.topic,
		"queueId": strconv.FormatInt(int64(h.queueID), 10),
	}
}

type maxOffsetResponse struct {
	Offset int64 `json:"offset"`
}

// MaxOffset returns the max offset in the consume queue
func MaxOffset(client remote.Client, addr, topic string, queueID uint8, to time.Duration) (
	int64, *Error,
) {
	cmd, err := client.RequestSync(
		addr,
		remote.NewCommand(getMaxOffset, &maxOffsetHeader{
			topic:   topic,
			queueID: queueID,
		}),
		to)
	if err != nil {
		return 0, requestError(err)
	}

	if cmd.Code != Success {
		return 0, brokerError(cmd)
	}

	offset, err := strconv.ParseInt(cmd.ExtFields["offset"], 10, 64)
	if err != nil {
		return 0, dataError(err)
	}

	return offset, nil
}

type searchOffsetByTimestampHeader struct {
	topic           string
	queueID         uint8
	timestampMillis int64
}

func (h *searchOffsetByTimestampHeader) ToMap() map[string]string {
	return map[string]string{
		"topic":     h.topic,
		"queueId":   strconv.Itoa(int(h.queueID)),
		"timestamp": strconv.FormatInt(h.timestampMillis, 10),
	}
}

// SearchOffsetByTimestamp returns the offset of the specified message queue and the timestamp
func SearchOffsetByTimestamp(
	client remote.Client, addr, topic string, queueID uint8, timestamp time.Time, to time.Duration,
) (
	int64, *Error,
) {
	cmd, err := client.RequestSync(
		addr,
		remote.NewCommand(searchOffsetByTimestamp, &searchOffsetByTimestampHeader{
			topic:           topic,
			queueID:         queueID,
			timestampMillis: timestamp.UnixNano() / int64(time.Millisecond),
		}),
		to)
	if err != nil {
		return 0, requestError(err)
	}

	if cmd.Code != Success {
		return 0, brokerError(cmd)
	}

	offset, err := strconv.ParseInt(cmd.ExtFields["offset"], 10, 64)
	if err != nil {
		return 0, dataError(err)
	}

	return offset, nil
}

type registerFilterBody struct {
	ClientID      string         `json:"clientId"`
	Group         string         `json:"group"`
	SubscribeData *SubscribeData `json:"subscriptionData "`
}

// SubscribeData subscription information
type SubscribeData struct {
	Topic             string   `json:"topic"`
	Expr              string   `json:"subString"`
	Type              string   `json:"expressionType"`
	Tags              []string `json:"tagsSet"`
	Codes             []uint32 `json:"codeSet"`
	Version           int64    `json:"subVersion"`
	IsClassFilterMode bool     `json:"classFilterMode"`
	FilterClassSource string   `json:"-"`
}

func (sd *SubscribeData) String() string {
	return fmt.Sprintf(
		"SubscribeData:[topic=%s,subString=%s,expressType=%s,tagsSet=%s,version=%d]",
		sd.Topic, sd.Expr, sd.Type, sd.Tags, sd.Version,
	)
}

// RegisterFilter register the filter to the broker
func RegisterFilter(
	client remote.Client, addr, group, clientID string, subData *SubscribeData, to time.Duration,
) error {
	b, err := json.Marshal(registerFilterBody{
		Group:         group,
		SubscribeData: subData,
		ClientID:      clientID,
	})
	if err != nil {
		return dataError(err)
	}
	cmd, err := client.RequestSync(addr, remote.NewCommandWithBody(checkClientConfig, nil, b), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		return brokerError(cmd)
	}

	return nil
}

type pullMessageCallback struct {
	f func(*PullResponse, error)
}

func (cb pullMessageCallback) processCommand(cmd *remote.Command, err error) {
	if err != nil {
		cb.f(nil, err)
		return
	}

	pr, err := convertCmdToPullResponse(cmd)
	cb.f(pr, err)
}

// PullMessageAsync pull the message async
func PullMessageAsync(
	client remote.Client, addr string, header *PullHeader, to time.Duration, callback func(*PullResponse, error),
) error {
	err := client.RequestAsync(
		addr, remote.NewCommand(pullMessage, header), to, pullMessageCallback{callback}.processCommand,
	)
	if err != nil {
		return requestError(err)
	}

	return nil
}

type lockRequest struct {
	Group    string          `json:"consumerGroup"`
	ClientID string          `json:"clientId"`
	Queues   []message.Queue `json:"mqSet"`
}

type lockResponse struct {
	Queues []message.Queue `json:"lockOKMQSet"`
}

// LockMessageQueues send lock message queue request to the broker
func LockMessageQueues(
	client remote.Client, addr string, group, clientID string, queues []message.Queue, to time.Duration,
) ([]message.Queue, error) {
	d, _ := json.Marshal(&lockRequest{Group: group, ClientID: clientID, Queues: queues})
	cmd, err := client.RequestSync(
		addr, remote.NewCommandWithBody(lockBatchMQ, nil, d), to,
	)
	if err != nil {
		return nil, requestError(err)
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	var resp lockResponse
	err = json.Unmarshal(cmd.Body, &resp)
	return resp.Queues, err
}

// UnlockMessageQueuesOneway send unlock message queue request to the broker
func UnlockMessageQueuesOneway(
	client remote.Client, addr, group, clientID string, queues []message.Queue,
) error {
	d, _ := json.Marshal(&lockRequest{Group: group, ClientID: clientID, Queues: queues})
	err := client.RequestOneway(addr, remote.NewCommandWithBody(unlockBatchMQ, nil, d))
	if err != nil {
		return requestError(err)
	}

	return nil
}

type consumeMessageDirectlyHeader struct {
	group    string
	clientID string
	offsetID string
}

func (h *consumeMessageDirectlyHeader) ToMap() map[string]string {
	return map[string]string{
		"consumerGroup": h.group,
		"clientId":      h.clientID,
		"msgId":         h.offsetID,
	}
}

// ConsumeMessageDirectly send unlock message queue request to the broker
func ConsumeMessageDirectly(
	client remote.Client, addr, group, clientID, offsetID string, timeout time.Duration,
) (
	ret ConsumeMessageDirectlyResult, err error,
) {
	cmd, err := client.RequestSync(addr, remote.NewCommand(
		ConsumeMessageDirectlyCode, &consumeMessageDirectlyHeader{
			group:    group,
			clientID: clientID,
			offsetID: offsetID,
		}), timeout,
	)
	if err != nil {
		err = requestError(err)
		return
	}

	if cmd.Code != Success {
		err = brokerError(cmd)
		return
	}

	err = json.Unmarshal(cmd.Body, &ret)
	return
}
