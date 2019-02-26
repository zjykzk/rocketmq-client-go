package remote

import "strconv"

// Code command code
type Code int16

// ToInt16 to int16 value
func (c Code) ToInt16() int16 {
	return int16(c)
}

func int16ToCode(code int16) Code {
	return Code(code)
}

// Int16ToCode from int16 to Code
func Int16ToCode(code int16) Code {
	return Code(code)
}

// UnmarshalJSON unmarshal code
func (c *Code) UnmarshalJSON(b []byte) error {
	cc, err := strconv.Atoi(string(b))
	if err == nil {
		*c = Code(cc)
		return nil
	}
	return err
}

// LanguageCode the language of client
type LanguageCode int8

// ToInt8 to int16 value
func (lc LanguageCode) ToInt8() int8 {
	return int8(lc)
}

func int8ToLanguageCode(code int8) LanguageCode {
	return LanguageCode(code)
}

func (lc LanguageCode) String() string {
	switch lc {
	case gO:
		return "go"
	case java:
		return "JAVA"
	default:
		return "unknown:" + strconv.Itoa(int(lc))
	}
}

// UnmarshalJSON unmarshal language code
func (lc *LanguageCode) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case "JAVA":
		*lc = java
	default:
		*lc = -1
	}
	return nil
}

const (
	java = LanguageCode(0)
	gO   = LanguageCode(9)
)
