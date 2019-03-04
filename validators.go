package rocketmq

import (
	"errors"
	"regexp"
	"strings"
)

const (
	maxTopicLen = 255
	pattern     = "^[%|a-zA-Z0-9_-]+$"
)

var (
	errEmptyTopic      = errors.New("empty topic")
	errLongTopic       = errors.New("long topic")
	errMismatchPattern = errors.New("mismatch the pattern:" + pattern)
	errDefaultTopic    = errors.New("default topic")
)

// CheckTopic returns the error the topic is invalid
// the rule of the topic is following:
// 1. only space
// 2. over the max length
// 3. does match the pattern
func CheckTopic(topic string) error {
	if strings.TrimSpace(topic) == "" {
		return errEmptyTopic
	}

	if len(topic) > maxTopicLen {
		return errLongTopic
	}

	ok, _ := regexp.MatchString(pattern, topic)
	if !ok {
		return errMismatchPattern
	}

	if DefaultTopic == topic {
		return errDefaultTopic
	}

	return nil
}
