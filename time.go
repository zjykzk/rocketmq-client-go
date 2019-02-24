package rocketmq

import "time"

// UnixMilli returns a Unix time, the number of millisecond elapsed
// since January 1, 1970 UTC.
func UnixMilli() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
