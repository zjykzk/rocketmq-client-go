package consumer

// pull flags
const (
	PullCommitOffset = 1 << iota
	PullSuspend
	PullSubscribe
)

func buildPullFlag(commitOffset, suspend, subscribe bool) (flag int32) {
	if commitOffset {
		flag |= PullCommitOffset
	}

	if suspend {
		flag |= PullSuspend
	}

	if subscribe {
		flag |= PullSubscribe
	}

	return
}

// ClearCommitOffset clears the commitoffset flag
func ClearCommitOffset(flag int32) int32 {
	return flag &^ PullCommitOffset
}
