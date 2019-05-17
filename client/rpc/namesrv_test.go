package rpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/remote"
)

func testGetTopicRouteInfo(t *testing.T) {
	//topic, err := getTopicRouteInfo("localhost:9876", c, "Perform_topic_1", time.Second*10)
	topic, err := GetTopicRouteInfo(&remote.FakeClient{}, "10.200.20.70:9876", "Perform_topic_1", time.Second*10)
	t.Log(topic, err)
}

func testReadField(t *testing.T) {
	fields, err := readFields(`{"i":1, "empty":{}, "object":{"a":"a"}, "f":1.1}`)
	assert.Nil(t, err)
	assert.Equal(t, "1", fields["i"])
	assert.Equal(t, "{}", fields["empty"])
	assert.Equal(t, `{"a":"a"}`, fields["object"])
	assert.Equal(t, "1.1", fields["f"])
}

func TestReadString(t *testing.T) {
	d := []byte(`"a"`)
	s, i, err := readString(d)
	assert.Nil(t, err)
	assert.Equal(t, "a", string(s))
	assert.Equal(t, 3, i)

	d = []byte(`   "a\"  "`)
	s, i, err = readString(d)
	assert.Nil(t, err)
	assert.Equal(t, `a\"  `, string(s))
	assert.Equal(t, len(d), i)

	d = []byte(`   "a\"  "  `)
	s, i, err = readString(d)
	assert.Nil(t, err)
	assert.Equal(t, `a\"  `, string(s))
	assert.Equal(t, len(d)-2, i)

	d = []byte(`"`)
	_, _, err = readString(d)
	assert.NotNil(t, err)

	d = []byte(`"abc\"`)
	_, _, err = readString(d)
	assert.NotNil(t, err)
}

func TestReadNumber(t *testing.T) {
	d := []byte(`100`)
	n, i, err := readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "100", string(n))
	assert.Equal(t, 3, i)

	d = []byte(`-100 `)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "-100", string(n))
	assert.Equal(t, 4, i)

	d = []byte(`100 `)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "100", string(n))
	assert.Equal(t, 3, i)

	d = []byte(`-100e-100 `)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "-100e-100", string(n))
	assert.Equal(t, 9, i)

	d = []byte(`-100E-100 `)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "-100E-100", string(n))
	assert.Equal(t, 9, i)

	d = []byte(`-100E+100 `)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "-100E+100", string(n))
	assert.Equal(t, 9, i)

	d = []byte(`0.1`)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "0.1", string(n))
	assert.Equal(t, 3, i)

	d = []byte(`0.1e-1`)
	n, i, err = readNumber(d)
	assert.Nil(t, err)
	assert.Equal(t, "0.1e-1", string(n))
	assert.Equal(t, 6, i)

	d = []byte(`[]`)
	n, i, err = readNumber(d)
	assert.NotNil(t, err)
}

func TestReadArray(t *testing.T) {
	d := []byte(`[]`)
	n, i, err := readArray(d)
	assert.Nil(t, err)
	assert.Equal(t, "[]", string(n))
	assert.Equal(t, 2, i)

	d = []byte(`["a"]`)
	n, i, err = readArray(d)
	assert.Nil(t, err)
	assert.Equal(t, `["a"]`, string(n))
	assert.Equal(t, 5, i)

	d = []byte(`["a", "b"]`)
	n, i, err = readArray(d)
	assert.Nil(t, err)
	assert.Equal(t, `["a", "b"]`, string(n))
	assert.Equal(t, 10, i)

	d = []byte(`[1, -1]`)
	n, i, err = readArray(d)
	assert.Nil(t, err)
	assert.Equal(t, `[1, -1]`, string(n))
	assert.Equal(t, 7, i)
}

func TestReadObject(t *testing.T) {

}
