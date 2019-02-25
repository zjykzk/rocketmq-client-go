package net

import (
	"io"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zjykzk/rocketmq-client-go/log"
)

type mockRequest []byte

func (r mockRequest) ID() int64 {
	return 1
}

type mockResponse string

func (r mockResponse) ID() int64 {
	return 1
}

func decodeMockResponse(data []byte) (interface{}, error) {
	return mockResponse(data), nil
}

func encodeMockRequest(o interface{}) ([]byte, error) {
	return []byte(o.(mockRequest)), nil
}

func mockPacketReader(data io.Reader) ([]byte, error) {
	ret := make([]byte, 1024)
	n, err := data.Read(ret)
	return ret[:n], err
}

func TestClient(t *testing.T) {
	port := strconv.Itoa(rand.Intn(10000) + 50000)
	go startServer(port)
	time.Sleep(time.Second)

	c := NewClient(
		EncoderFunc(encodeMockRequest), PacketReaderFunc(mockPacketReader), DecoderFunc(decodeMockResponse),
		&Config{ReadTimeout: time.Second, WriteTimeout: time.Second, DialTimeout: time.Second},
		&log.MockLogger{})
	c.Start()
	defer c.Shutdown()
	t.Run("request sync", func(t *testing.T) {
		d := mockRequest("request sync")
		_, err := c.RequestSync("localhost:123", d, time.Second)
		assert.NotNil(t, err)
		r, err := c.RequestSync("localhost:"+port, d, time.Second)
		assert.Nil(t, err)
		assert.Equal(t, []byte(d), []byte(r.(mockResponse)))
	})
}

func TestFuture(t *testing.T) {
	c := NewClient(
		EncoderFunc(encodeMockRequest), PacketReaderFunc(mockPacketReader), DecoderFunc(decodeMockResponse),
		&Config{ReadTimeout: time.Second, WriteTimeout: time.Second, DialTimeout: time.Second},
		&log.MockLogger{},
	)

	var (
		id       int64
		getCount int64
		count    = int64(10000)
	)
	t.Run("put", func(t *testing.T) {
		t.Parallel()
		for i := int64(0); i < count; i++ {
			c.putFuture(time.Millisecond, atomic.AddInt64(&id, 1), nil)
		}
	})

	getAndRemove := func(t *testing.T) {
		for atomic.LoadInt64(&getCount) < count {
			cc := 2
			fs := c.getFutures(func(*responseFuture) bool { cc--; return cc >= 0 })
			atomic.AddInt64(&getCount, int64(len(fs)))
		}
	}

	t.Run("getFutures and remove", func(t *testing.T) {
		t.Parallel()
		go getAndRemove(t)
		go getAndRemove(t)
	})
}
