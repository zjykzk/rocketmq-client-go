package fastjson

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadString(t *testing.T) {
	d := []byte(`"a"`)
	s, i, err := readString(d)
	assert.Nil(t, err)
	assert.Equal(t, "a", string(s))
	assert.Equal(t, 3, i)

	d = []byte(` "a\"  "`)
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

	d = []byte(`[{}]`)
	n, i, err = readArray(d)
	assert.Nil(t, err)
	assert.Equal(t, `[{}]`, string(n))
	assert.Equal(t, 4, i)

	d = []byte(`[{},{1:2}]`)
	n, i, err = readArray(d)
	assert.Nil(t, err)
	assert.Equal(t, `[{},{1:2}]`, string(n))
	assert.Equal(t, 10, i)

	d = []byte(`[}]`)
	n, i, err = readArray(d)
	assert.NotNil(t, err)

	d = []byte(`["]`)
	n, i, err = readArray(d)
	assert.NotNil(t, err)

	d = []byte(`[1"]`)
	n, i, err = readArray(d)
	assert.NotNil(t, err)

	d = []byte(`[`)
	n, i, err = readArray(d)
	assert.NotNil(t, err)
}

func TestReadObject(t *testing.T) {
	d := []byte(`{}`)
	n, i, err := readObject(d)
	assert.Nil(t, err)
	assert.Equal(t, "{}", string(n))
	assert.Equal(t, 2, i)

	d = []byte(`{1: 2}`)
	n, i, err = readObject(d)
	assert.Nil(t, err)
	assert.Equal(t, "{1: 2}", string(n))
	assert.Equal(t, 6, i)

	d = []byte(`{1:2,2:3}`)
	n, i, err = readObject(d)
	assert.Nil(t, err)
	assert.Equal(t, "{1:2,2:3}", string(n))
	assert.Equal(t, 9, i)

	d = []byte(`{"1":2}`)
	n, i, err = readObject(d)
	assert.Nil(t, err)
	assert.Equal(t, `{"1":2}`, string(n))
	assert.Equal(t, 7, i)

	d = []byte(`{["1"]:2}`)
	n, i, err = readObject(d)
	assert.Nil(t, err)
	assert.Equal(t, `{["1"]:2}`, string(n))
	assert.Equal(t, 9, i)

	d = []byte(`{{1:2}:2}`)
	n, i, err = readObject(d)
	assert.Nil(t, err)
	assert.Equal(t, `{{1:2}:2}`, string(n))
	assert.Equal(t, 9, i)

	d = []byte(`{`)
	n, i, err = readObject(d)
	assert.NotNil(t, err)

	d = []byte(`{1"}`)
	n, i, err = readObject(d)
	assert.NotNil(t, err)

	//d = []byte(`{,}`) // CANNOT parsed TODO
	//n, i, err = readObject(d)
	//assert.NotNil(t, err)
}

func TestParseObject(t *testing.T) {
	d := []byte(`{{1:2}:2}`)
	m, err := ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), m["{1:2}"])

	d = []byte(`{1:2}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), m["1"])

	d = []byte(`{"1":2}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), m["1"])

	d = []byte(`{"1":"2"}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte("2"), m["1"])

	d = []byte(`{"1":["2"]}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte(`["2"]`), m["1"])

	d = []byte(`{"1":{"2":1}}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte(`{"2":1}`), m["1"])

	d = []byte(`{true:{"2":1}}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte(`{"2":1}`), m["true"])

	d = []byte(`{"null":null}`)
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Equal(t, []byte("null"), m["null"])

	d = []byte(`{trueA:{"2":1}}`)
	m, err = ParseObject(d)
	assert.NotNil(t, err)

	d = []byte("null")
	m, err = ParseObject(d)
	assert.Nil(t, err)
	assert.Nil(t, m)
}

func TestParseArray(t *testing.T) {
	d := []byte(" []")
	a, err := ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(a))

	d = []byte("[1]")
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(a))
	assert.Equal(t, "1", string(a[0]))

	d = []byte("[1,  2]")
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(a))
	assert.Equal(t, "1", string(a[0]))
	assert.Equal(t, "2", string(a[1]))

	d = []byte(`["1"]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(a))
	assert.Equal(t, "1", string(a[0]))

	d = []byte(`["1","2"]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(a))
	assert.Equal(t, "1", string(a[0]))
	assert.Equal(t, "2", string(a[1]))

	d = []byte(`[[]]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(a))
	assert.Equal(t, "[]", string(a[0]))

	d = []byte(`[[],[1]]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(a))
	assert.Equal(t, "[]", string(a[0]))
	assert.Equal(t, "[1]", string(a[1]))

	d = []byte(`[{1:2}]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(a))
	assert.Equal(t, "{1:2}", string(a[0]))

	d = []byte(`[{1:2},{1:1}]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(a))
	assert.Equal(t, "{1:2}", string(a[0]))
	assert.Equal(t, "{1:1}", string(a[1]))

	d = []byte(`[true, false]`)
	a, err = ParseArray(d)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(a))
	assert.Equal(t, "true", string(a[0]))
	assert.Equal(t, "false", string(a[1]))

	d = []byte(`[trueA, false]`)
	a, err = ParseArray(d)
	assert.NotNil(t, err)

	d = []byte(`[null,null]`)
	a, err = ParseArray(d)
	assert.Equal(t, "null", string(a[0]))
	assert.Equal(t, "null", string(a[1]))
	assert.Nil(t, err)

	d = []byte(`[n,null]`)
	a, err = ParseArray(d)
	assert.NotNil(t, err)
}

func TestReadBool(t *testing.T) {
	_, _, err := readBool([]byte("a"))
	assert.NotNil(t, err)

	_, _, err = readBool([]byte("ta"))
	assert.NotNil(t, err)

	_, _, err = readBool([]byte("f"))
	assert.NotNil(t, err)

	_, _, err = readBool([]byte("True"))
	assert.NotNil(t, err)

	_, _, err = readBool([]byte("fAlse"))
	assert.NotNil(t, err)

	b, i, err := readBool([]byte("truea"))
	assert.Nil(t, err)
	assert.Equal(t, "true", string(b))
	assert.Equal(t, 4, i)

	b, i, err = readBool([]byte("falsee"))
	assert.Nil(t, err)
	assert.Equal(t, "false", string(b))
	assert.Equal(t, 5, i)
}

func TestReadNull(t *testing.T) {
	_, _, err := readNull([]byte("a"))
	assert.NotNil(t, err)
	d, i, err := readNull([]byte("null"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("null"), d)
	assert.Equal(t, 4, i)
}
