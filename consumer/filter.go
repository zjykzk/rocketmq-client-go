package consumer

import (
	"hash/fnv"
	"strings"

	"github.com/zjykzk/rocketmq-client-go/client"
)

// ExprType the filter type of the subcription
type ExprType int

const (
	// ExprTypeTag tag filter
	// Only support or operation such as
	// "tag1 || tag2 || tag3", <br>
	// If null or * expression,meaning subscribe all.
	ExprTypeTag ExprType = iota

	// ExprTypeSQL92 sql filter
	//
	//Keywords:
	//AND, OR, NOT, BETWEEN, IN, TRUE, FALSE, IS, NULL
	//Boolean, like: TRUE, FALSE
	//String, like: 'abc'
	//Decimal, like: 123
	//Float number, like: 3.1415
	//
	//Grammar:
	//AND, OR
	//>, >=, <, <=, =
	//BETWEEN A AND B, equals to >=A AND <=B
	//NOT BETWEEN A AND B, equals to >B OR <A
	//IN ('a', 'b'), equals to ='a' OR ='b', this operation only support String type.
	//IS NULL, IS NOT NULL, check parameter whether is null, or not.
	//=TRUE, =FALSE, check parameter whether is true, or false.
	//
	//Example: (a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)
	ExprTypeSQL92

	subAll = "*"
)

var exprTypeNames = [...]string{"TAG", "SQL92"}

func (t ExprType) String() string {
	return exprTypeNames[t]
}

// BuildSubscribeData build the subscribe data with tag type
func BuildSubscribeData(group, topic, expr string) *client.SubscribeData {
	d := &client.SubscribeData{Topic: topic, Expr: expr, Type: ExprTypeTag.String()}
	if expr == "" {
		d.Expr = subAll
		// ignore the tags, so the file Tags is nil
	}

	if d.Expr == subAll {
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
	return ExprTypeTag.String() == typ
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
