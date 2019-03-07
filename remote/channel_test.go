package remote

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/log"
)

func echoDecode(data []byte) (*Command, error) {
	return &Command{Body: data}, nil
}

func echoEncode(o *Command) ([]byte, error) {
	return o.Body, nil
}

func echoReadPacket(r io.Reader) ([]byte, error) {
	ret := make([]byte, 1024)
	_, err := r.Read(ret)
	return ret, err
}

type fakeHandler struct {
	logger log.Logger
}

func (h *fakeHandler) OnActive(ctx *ChannelContext) {
	h.logger.Infof("active %s", ctx)
}
func (h *fakeHandler) OnDeactive(ctx *ChannelContext) {
	h.logger.Infof("deactive %s", ctx)
}
func (h *fakeHandler) OnClose(ctx *ChannelContext) {
	h.logger.Infof("closed %s", ctx)
}

func (h *fakeHandler) OnError(ctx *ChannelContext, err error) {
	h.logger.Infof("error %s, %s", ctx, err)
}

func (h *fakeHandler) OnMessage(ctx *ChannelContext, cmd *Command) {
	h.logger.Infof("%v", cmd)
}
func handleError(err error) {
	fmt.Printf("error:%v\n", err)
}

func handleConn(tcpConn *net.TCPConn, addr string) {
	if tcpConn == nil {
		return
	}
	buff := make([]byte, 1024)
	for {
		n, err := tcpConn.Read(buff)
		if err == io.EOF {
			fmt.Printf("client:[%s] closed \n", tcpConn.RemoteAddr().String())
			return
		}
		if err != nil {
			handleError(err)
		}
		if string(buff[:n]) == "exit" {
			fmt.Printf("client:[%s] exited\n", tcpConn.RemoteAddr().String())
		}
		if n > 0 {
			fmt.Printf("%s send message:%s", addr, string(buff[:n]))
		}
		tcpConn.Write(buff[:n])
	}
}

func startServer(port string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:"+port)
	handleError(err)
	if err != nil {
		return
	}

	tcpListener, err := net.ListenTCP("tcp4", tcpAddr)
	handleError(err)
	if err != nil {
		return
	}

	fmt.Printf("server started\n")
	defer tcpListener.Close()

	for {
		tcpConn, err := tcpListener.AcceptTCP()
		handleError(err)
		if err != nil {
			return
		}

		fmt.Printf("client:[%s] connected\n", tcpConn.RemoteAddr().String())
		defer tcpConn.Close()
		go handleConn(tcpConn, tcpConn.RemoteAddr().String())
	}
}

func TestChannel(t *testing.T) {
	port := strconv.Itoa(rand.Intn(10000) + 50000)
	go startServer(port)
	time.Sleep(time.Second)

	ch, err := newChannel("localhost:"+port,
		ChannelConfig{
			ClientConfig: ClientConfig{
				ReadTimeout:  time.Second,
				WriteTimeout: time.Second,
				DialTimeout:  time.Second,
			},
			Encoder:      EncoderFunc(echoEncode),
			PacketReader: PacketReaderFunc(echoReadPacket),
			Decoder:      DecoderFunc(echoDecode),
			Handler:      &fakeHandler{logger: log.Std},
			logger:       log.Std,
		})

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, StateConnected, ch.getState())

	err = ch.SendSync(&Command{Body: []byte(fmt.Sprintf(`POST s HTTP/1.1 Host: localhost`))})
	assert.Nil(t, err)

	resp := make([]byte, 1024)
	ch.ctx.Conn.Read(resp)
	t.Log(string(resp))

	ch.close()
}
