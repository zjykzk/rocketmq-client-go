package rocketmq

import (
	"strconv"
	"time"
)

// UnixMilli returns a Unix time, the number of millisecond elapsed
// since January 1, 1970 UTC.
func UnixMilli() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// ParseMillis parses the millisecond in the string form, returns the time in the time.Duration
func ParseMillis(s string) (time.Duration, error) {
	d, err := strconv.ParseInt(s, 64, 10)
	if err != nil {
		return 0, err
	}
	return time.Duration(d) * time.Millisecond, nil
}
