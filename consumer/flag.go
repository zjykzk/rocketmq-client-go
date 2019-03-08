package consumer

// pull flags
const (
	PullCommitOffset = 1 << iota
	PullSuspend
	PullSubscribe
	PullClassFilter
)

func buildPullFlag(commitOffset, suspend, subscribe, classFilter bool) (flag int32) {
	if commitOffset {
		flag |= PullCommitOffset
	}

	if suspend {
		flag |= PullSuspend
	}

	if subscribe {
		flag |= PullSubscribe
	}

	if classFilter {
		flag |= PullClassFilter
	}

	return
}

// ClearCommitOffset clears the commitoffset flag
func ClearCommitOffset(flag int32) int32 {
	return flag &^ PullCommitOffset
}
