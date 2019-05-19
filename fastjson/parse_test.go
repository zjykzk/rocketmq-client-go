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
}
