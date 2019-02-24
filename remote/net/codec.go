package net

import (
	"errors"
	"io"
)

// ErrBadContent indicate the data is not recognized
var ErrBadContent = errors.New("bad content")

// ErrNeedContent indicate need to read data
var ErrNeedContent = errors.New("need content")

// Encoder encode the object to the bytes
type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

// Decoder decode the bytes to the object
type Decoder interface {
	Decode([]byte) (interface{}, error)
}

// PacketReader reads the packet
type PacketReader interface {
	Read(io.Reader) ([]byte, error)
}

// EncoderFunc encoder function
type EncoderFunc func(interface{}) ([]byte, error)

// Encode call encoder function
func (f EncoderFunc) Encode(o interface{}) ([]byte, error) {
	return f(o)
}

// DecoderFunc decoder function
type DecoderFunc func([]byte) (interface{}, error)

// Decode call decoder function
func (f DecoderFunc) Decode(d []byte) (interface{}, error) {
	return f(d)
}

// PacketReaderFunc packet read function
type PacketReaderFunc func(io.Reader) ([]byte, error)

func (f PacketReaderFunc) Read(r io.Reader) ([]byte, error) {
	return f(r)
}
