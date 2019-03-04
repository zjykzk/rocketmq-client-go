package message

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zjykzk/rocketmq-client-go"
	"github.com/zjykzk/rocketmq-client-go/buf"
)

const (
	sz = 4 + 2 + 4 + 4 + 2
)

// GUID the global unique id generator
type GUID struct {
	fixString string
	counter   int32
}

// initialize the fixString which is the hex-encoded string of data following:
// | ip address| pid     | random  |
// +-----------+---------+---------+
// |  4 bytes  | 2 bytes | 3 bytes |
// +-----------+---------+---------+
//
func (g *GUID) init() {
	bs := make([]byte, 9)

	binary.BigEndian.PutUint32(bs[2:], uint32(os.Getpid()))
	if ip, err := rocketmq.GetIP(); err == nil {
		if len(ip) > 4 {
			ip = ip[len(ip)-4:]
		}
		copy(bs, ip)
	} else {
		binary.BigEndian.PutUint32(bs, uint32(unixMillis(time.Now())))
	}
	if _, err := rand.Read(bs[6:]); err != nil {
		now := uint32(unixMillis(time.Now()))
		bs[6], bs[7], bs[8] = byte(now>>16), byte(now>>8), byte(now)
	}
	g.fixString = strings.ToUpper(hex.EncodeToString(bs))
}

// Create create new global unique id hex-encoded string with length 32
//
// fixString + hex-encoded(id)
//
// the id's content is following:
//
// |<- unix time ->|<- increment num ->|
// +---------------+-------------------+
// |  4 bytes      |  3 bytes          |
// +---------------+-------------------+
func (g *GUID) Create() string {
	id := uint64(time.Now().Unix())<<24 | uint64(atomic.AddInt32(&g.counter, 1))&0xffffff
	bs := make([]byte, 1+4+3)
	binary.BigEndian.PutUint64(bs, id)

	return g.fixString + strings.ToUpper(hex.EncodeToString(bs[1:])) // since, bs[0] == id>>56 == 0
}

// NewGenerator creates the guid generator
func NewGenerator() *GUID {
	g := &GUID{}
	g.init()
	return g
}

func unixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

var (
	guid = NewGenerator()
)

// CreateUniqID returns the global unique id
func CreateUniqID() string {
	return guid.Create()
}

// CreateMessageID create id using store host address and the message commited offset
// returns the string of length 32
func CreateMessageID(storeHost *Addr, commitOffset int64) string {
	buf := buf.WrapBytes(binary.BigEndian, make([]byte, 0, 8))
	buf.PutBytes(storeHost.Host)
	buf.PutInt32(int32(storeHost.Port))
	buf.PutInt64(commitOffset)
	return strings.ToUpper(hex.EncodeToString(buf.Bytes()))
}

// ParseMessageID parse the id and get the ip address and commit offset
func ParseMessageID(id string) (addr Addr, commitOffset int64, err error) {
	bs, err := hex.DecodeString(id)
	if err != nil {
		return
	}

	buf := buf.WrapBytes(binary.BigEndian, bs)
	addr.Host, err = buf.GetBytes(4)
	if err != nil {
		return
	}

	port, err := buf.GetInt32()
	if err != nil {
		return
	}
	addr.Port = uint16(port)

	commitOffset, err = buf.GetInt64()
	return
}

// IsMessageID returns true if the id follows the rules:
// 1. the length is 32
// 2. the character is the hex character
func IsMessageID(id string) bool {
	l := len(id)
	if len(id) != 32 {
		return false
	}

	for i := 0; i < l; i++ {
		switch c := id[i]; {
		case c < '0' && c > '9':
			return false
		case c < 'a' && c > 'f':
			return false
		case c < 'A' && c > 'F':
			return false
		}
	}
	return true
}
