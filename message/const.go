package message

// predefined consts
const (
	PropertyKeys                      = "KEYS"
	PropertyTags                      = "TAGS"
	PropertyWaitStoreMsgOK            = "WAIT"
	PropertyDelayTimeLevel            = "DELAY"
	PropertyRetryTopic                = "RETRY_TOPIC"
	PropertyRealTopic                 = "REAL_TOPIC"
	PropertyRealQueueID               = "REAL_QID"
	PropertyTransactionPrepared       = "TRAN_MSG"
	PropertyProducerGroup             = "PGROUP"
	PropertyMinOffset                 = "MIN_OFFSET"
	PropertyMaxOffset                 = "MAX_OFFSET"
	PropertyBuyerID                   = "BUYER_ID"
	PropertyOriginMessageID           = "ORIGIN_MESSAGE_ID"
	PropertyTransferFlag              = "TRANSFER_FLAG"
	PropertyCorrectionFlag            = "CORRECTION_FLAG"
	PropertyMQ2Flag                   = "MQ2_FLAG"
	PropertyReconsumeTime             = "RECONSUME_TIME"
	PropertyMsgRegion                 = "MSG_REGION"
	PropertyTraceSwitch               = "TRACE_ON"
	PropertyUniqClientMessageIDKeyidx = "UNIQ_KEY"
	PropertyMaxReconsumeTimes         = "MAX_RECONSUME_TIMES"
	PropertyConsumeStartTimestamp     = "CONSUME_START_TIME"

	KeySep = " "
)
