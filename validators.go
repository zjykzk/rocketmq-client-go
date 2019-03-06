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

	errEmptyGroup = errors.New("empty group")
	errLongGroup  = errors.New("long group")
)

// CheckTopic returns the error the topic is invalid
// the rule of the topic is following:
// 1. only space
// 2. over the max length
// 3. does not match the pattern
// 4. default topic
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

// CheckGroup returns the error the topic is invalid
// the rule of the topic is following:
// 1. only space
// 2. over the max length
// 3. does not match the pattern
func CheckGroup(group string) error {
	if strings.TrimSpace(group) == "" {
		return errEmptyGroup
	}

	if len(group) > maxTopicLen {
		return errLongGroup
	}

	ok, _ := regexp.MatchString(pattern, group)
	if !ok {
		return errMismatchPattern
	}

	return nil
}
