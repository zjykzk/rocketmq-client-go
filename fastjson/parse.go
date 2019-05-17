package fastjson

import (
	"errors"
	"unicode"
)

// ParseObject returns one map whose key is the key field name
// value is sub-byte array
func ParseObject(d []byte) (map[string][]byte, error) {
	m := map[string][]byte{}

	i := firstNotSpace(d)
	var k string
	for l := len(d); i < l; {
		switch n := d[i]; n {
		case '{', ',', ':':
			s, j, err := readKV(d[i+1:])
			if err != nil {
				return nil, err
			}
			i += j + 1

			if n == ':' {
				m[k] = s
			} else {
				k = string(s)
			}
		case '}':
			return m, nil
		default:
			return nil, errors.New(
				"[BUG] canot parseObject \"" + string(d) + "\", char:'" + string(n) + "'",
			)
		}
	}
	return nil, errors.New("[BUG] canot parseObject \"" + string(d) + "\"")
}

func readString(d []byte) ([]byte, int, error) {
	s, e := -1, -1
	l := len(d)
	for i := 0; i < l; i++ {
		if d[i] == '"' {
			if i > 0 && d[i-1] == '\\' {
				continue
			}
			s = i
			break
		}
	}

	for i := s + 1; i < l; i++ {
		if d[i] == '"' {
			if i > 0 && d[i-1] == '\\' {
				continue
			}
			e = i
			break
		}
	}

	if s == -1 || e == -1 {
		return nil, e, errors.New("[BUG] cannot process string:" + string(d))
	}

	return d[s+1 : e], e + 1, nil
}

func readNumber(d []byte) ([]byte, int, error) {
	for i, n := range d {
		switch {
		case n >= '0' && n <= '9':
		case n == '-' || n == '+':
		case n == 'e' || n == 'E':
		case n == '.':
		default:
			if i == 0 {
				return nil, 0, errors.New(
					"[BUG] cannot process number \"" + string(d) + "\", unknow char:'" + string(n) + "'",
				)
			}

			return d[0:i], i, nil
		}
	}

	return d, len(d), nil
}

func readArray(d []byte) ([]byte, int, error) {
	for i, l := 0, len(d); i < l; {
		switch n := d[i]; {
		case n == '[' || n == ',' || unicode.IsSpace(rune(n)):
			i++
		case n == ']':
			return d[:i+1], i + 1, nil
		case (n >= '0' && n <= '9') || n == '-':
			_, k, err := readNumber(d[i:])
			if err != nil {
				return nil, 0, err
			}
			i += k
		case n == '"':
			_, k, err := readString(d[i:])
			if err != nil {
				return nil, 0, err
			}
			i += k
		case n == '}':
			_, k, err := readObject(d[i:])
			if err != nil {
				return nil, 0, err
			}
			i += k
		default:
			return nil, 0, errors.New(
				"[BUG] cannot process array:\"" + string(d) + "\", unknow char:'" + string(n) + "'",
			)
		}
	}

	return nil, 0, errors.New("[BUG] cannot process array:\"" + string(d) + "\"")
}

func readObject(d []byte) ([]byte, int, error) {
	for i, l := 0, len(d); i < l; {
		switch n := d[i]; {
		case n == '{' || n == ',' || n == ':':
			_, k, err := readKV(d[i+1:])
			if err != nil {
				return nil, 0, err
			}
			i += k + 1
		case n == '}':
			return d[:i+1], i + 1, nil
		}
	}
	return nil, 0, nil
}

func readKV(d []byte) ([]byte, int, error) {
	for i, l := 0, len(d); i < l; {
		switch n := d[i]; n {
		case '{':
			return readObject(d)
		case '[':
			return readArray(d)
		case '"':
			return readString(d)
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return readNumber(d)
		case ':', '}', ',':
			return d[:i], i, nil
		default:
			if unicode.IsSpace(rune(n)) {
				i++
				continue
			}
			return nil, 0, errors.New(
				"[BUG] canot readKV \"" + string(d) + "\", char:'" + string(n) + "'",
			)
		}
	}
	return nil, 0, errors.New("[BUG] canot readKV \"" + string(d) + "\"")
}

func firstNotSpace(d []byte) int {
	s := 0
	for ; unicode.IsSpace(rune(d[s])); s++ {
	}
	return s
}
