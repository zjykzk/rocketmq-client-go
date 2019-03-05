package consumer

import (
	"hash/fnv"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/client"
)

const (
	subAll = "*"
	// ExprTypeTag TAG literal
	ExprTypeTag = "TAG"
)

// BuildSubscribeData build the subscribe data
func BuildSubscribeData(group, topic, expr string) *client.SubscribeData {
	d := &client.SubscribeData{Topic: topic, Expr: expr}
	if expr == "" {
		d.Expr = subAll
		return d
	}

	tags := strings.Split(expr, "||")
	d.Tags = make([]string, 0, len(tags))
	d.Codes = make([]uint32, 0, len(tags))
	tagHasher := fnv.New32()
	for _, tag := range tags {
		tag = strings.Trim(tag, " ")
		if tag == "" {
			continue
		}
		d.Tags = append(d.Tags, tag)
		tagHasher.Write([]byte(tag))
		d.Codes = append(d.Codes, tagHasher.Sum32())
	}
	return d
}

// IsTag returns true if the expresstion type is "TAG" or empty string, false otherwise
func IsTag(typ string) bool {
	return typ == "" || ExprTypeTag == typ
}

// ParseTags parse the expression as tag elements
func ParseTags(expr string) []string {
	if expr == "" || expr == subAll {
		return nil
	}

	tags := strings.Split(expr, "||")
	for i := range tags {
		tags[i] = strings.Trim(tags[i], " ")
	}

	return tags
}
