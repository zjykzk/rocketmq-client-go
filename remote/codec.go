package remote

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
	Encode(*Command) ([]byte, error)
}

// Decoder decode the bytes to the object
type Decoder interface {
	Decode([]byte) (*Command, error)
}

// PacketReader reads the packet
type PacketReader interface {
	Read(io.Reader) ([]byte, error)
}

// EncoderFunc encoder function
type EncoderFunc func(*Command) ([]byte, error)

// Encode call encoder function
func (f EncoderFunc) Encode(cmd *Command) ([]byte, error) {
	return f(cmd)
}

// DecoderFunc decoder function
type DecoderFunc func([]byte) (*Command, error)

// Decode call decoder function
func (f DecoderFunc) Decode(d []byte) (*Command, error) {
	return f(d)
}

// PacketReaderFunc packet read function
type PacketReaderFunc func(io.Reader) ([]byte, error)

func (f PacketReaderFunc) Read(r io.Reader) ([]byte, error) {
	return f(r)
}
