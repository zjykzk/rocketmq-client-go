package remote

import "strconv"

// Code command code
type Code int16

// ToInt16 to int16 value
func (c Code) ToInt16() int16 {
	return int16(c)
}

func int16ToCode(code int16) Code {
	return Code(code)
}

// Int16ToCode from int16 to Code
func Int16ToCode(code int16) Code {
	return Code(code)
}

// UnmarshalJSON unmarshal code
func (c *Code) UnmarshalJSON(b []byte) error {
	cc, err := strconv.Atoi(string(b))
	if err == nil {
		*c = Code(cc)
		return nil
	}
	return err
}

// request code
const (
	SendMessage                      = Code(10)
	PullMessage                      = Code(11)
	QueryMessage                     = Code(12)
	QueryBrokerOffset                = Code(13)
	QueryConsumerOffset              = Code(14)
	UpdateConsumerOffset             = Code(15)
	UpdateAndCreateTopic             = Code(17)
	GetAllTopicConfig                = Code(21)
	GetTopicConfigList               = Code(22)
	GetTopicNameList                 = Code(23)
	UpdateBrokerConfig               = Code(25)
	GetBrokerConfig                  = Code(26)
	TriggerDeleteFiles               = Code(27)
	GetBrokerRuntimeInfo             = Code(28)
	SearchOffsetByTimestamp          = Code(29)
	GetMaxOffset                     = Code(30)
	GetMinOffset                     = Code(31)
	GetEarliestMsgStoretime          = Code(32)
	ViewMessageByID                  = Code(33)
	HeartBeat                        = Code(34)
	UnregisterClient                 = Code(35)
	ConsumerSendMsgBack              = Code(36)
	EndTransaction                   = Code(37)
	GetConsumerListByGroup           = Code(38)
	CheckTransactionState            = Code(39)
	NotifyConsumerIdsChanged         = Code(40)
	LockBatchMq                      = Code(41)
	UnlockBatchMq                    = Code(42)
	GetAllConsumerOffset             = Code(43)
	GetAllDelayOffset                = Code(45)
	CheckClientConfig                = Code(46)
	PutKvConfig                      = Code(100)
	GetKvConfig                      = Code(101)
	DeleteKvConfig                   = Code(102)
	RegisterBroker                   = Code(103)
	UnregisterBroker                 = Code(104)
	GetRouteintoByTopic              = Code(105)
	GetBrokerClusterInfo             = Code(106)
	UpdateAndCreateSubscriptiongroup = Code(200)
	GetAllSubscriptiongroupConfig    = Code(201)
	GetTopicStatsInfo                = Code(202)
	GetConsumerConnectionList        = Code(203)
	GetProducerConnectionList        = Code(204)
	WipeWritePermOfBroker            = Code(205)
	GetAllTopicListFromNameserver    = Code(206)
	DeleteSubscriptiongroup          = Code(207)
	GetConsumeStats                  = Code(208)
	SuspendConsumer                  = Code(209)
	ResumeConsumer                   = Code(210)
	ResetConsumerOffsetInConsumer    = Code(211)
	ResetConsumerOffsetInBroker      = Code(212)
	AdjustConsumerThreadPool         = Code(213)
	WhoConsumeTheMessage             = Code(214)
	DeleteTopicInBroker              = Code(215)
	DeleteTopicInNamesrv             = Code(216)
	GetKvlistByNamespace             = Code(219)
	ResetConsumerClientOffset        = Code(220)
	GetConsumerStatusFromClient      = Code(221)
	InvokeBrokerToResetOffset        = Code(222)
	InvokeBrokerToGetConsumerStatus  = Code(223)
	QueryTopicConsumeByWho           = Code(300)
	GetTopicsByCluster               = Code(224)
	RegisterFilterServer             = Code(301)
	RegisterMessageFilterClass       = Code(302)
	QueryConsumeTimeSpan             = Code(303)
	GetSystemTopicListFromNs         = Code(304)
	GetSystemTopicListFromBroker     = Code(305)
	CleanExpiredConsumequeue         = Code(306)
	GetConsumerRunningInfo           = Code(307)
	QueryCorrectionOffset            = Code(308)
	ConsumeMessageDirectly           = Code(309)
	SendMessageV2                    = Code(310)
	GetUnitTopicList                 = Code(311)
	GetHasUnitSubTopicList           = Code(312)
	GetHasUnitSubUnunitTopicList     = Code(313)
	CloneGroupOffset                 = Code(314)
	ViewBrokerStatsData              = Code(315)
	CleanUnusedTopic                 = Code(316)
	GetBrokerConsumeStats            = Code(317)
	UpdateNamesrvConfig              = Code(318)
	GetNamesrvConfig                 = Code(319)
	SendBatchMessage                 = Code(320)
	QueryConsumeQueue                = Code(321)
)

// response code
const (
	UnknowError                = Code(-1)
	Success                    = Code(0)
	SystemError                = Code(1)
	SystemBusy                 = Code(2)
	RequestCodeNotSupported    = Code(3)
	TransactionFailed          = Code(4)
	FlushDiskTimeout           = Code(10)
	SlaveNotAvailable          = Code(11)
	FlushSlaveTimeout          = Code(12)
	MessageIllegal             = Code(13)
	ServiceNotAvailable        = Code(14)
	VersionNotSupported        = Code(15)
	NoPermission               = Code(16)
	TopicNotExist              = Code(17)
	TopicExistAlready          = Code(18)
	PullNotFound               = Code(19)
	PullRetryImmediately       = Code(20)
	PullOffsetMoved            = Code(21)
	QueryNotFound              = Code(22)
	SubscriptionParseFailed    = Code(23)
	SubscriptionNotExist       = Code(24)
	SubscriptionNotLatest      = Code(25)
	SubscriptionGroupNotExist  = Code(26)
	FilterDataNotExist         = Code(27)
	FilterDataNotLatest        = Code(28)
	TransactionShouldCommit    = Code(200)
	TransactionShouldRollback  = Code(201)
	TransactionStateUnknow     = Code(202)
	TransactionStateGroupWrong = Code(203)
	NoBuyerID                  = Code(204)
	NotInCurrentUnit           = Code(205)
	ConsumerNotOnline          = Code(206)
	ConsumeMsgTimeout          = Code(207)
	NoMessage                  = Code(208)
	ConnectBrokerException     = Code(10001)
	AccessBrokerException      = Code(10002)
	BrokerNotExistException    = Code(10003)
	NoNameServerException      = Code(10004)
	NotFoundTopicException     = Code(10005)

	// dora defined
	RequestTimeout    = Code(-100)
	ConnError         = Code(-101)
	ConnClosed        = Code(-102)
	QueryMessageSucc  = Code(600)
	QueryMessageQueue = Code(601)
	QueryMessageCount = Code(602)
)

// LanguageCode the language of client
type LanguageCode int8

// ToInt8 to int16 value
func (lc LanguageCode) ToInt8() int8 {
	return int8(lc)
}

func int8ToLanguageCode(code int8) LanguageCode {
	return LanguageCode(code)
}

func (lc LanguageCode) String() string {
	switch lc {
	case gO:
		return "go"
	case java:
		return "JAVA"
	default:
		return "unknown:" + strconv.Itoa(int(lc))
	}
}

// UnmarshalJSON unmarshal language code
func (lc *LanguageCode) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case "JAVA":
		*lc = java
	default:
		*lc = -1
	}
	return nil
}

const (
	java = LanguageCode(0)
	gO   = LanguageCode(9)
)
