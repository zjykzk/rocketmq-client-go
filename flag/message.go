package flag

const (
	Compress                = 1
	MultiTags               = 1 << 1
	TransactionNotType      = 0
	TransactionPreparedType = 0x1 << 2
	TransactionCommitType   = 0x2 << 2
	TransactionRollbackType = 0x3 << 2
)
