package rpc

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/zjykzk/rocketmq-client-go/remote"
	"github.com/zjykzk/rocketmq-client-go/route"
)

// DeleteTopicInNamesrv delete topic in the broker
func DeleteTopicInNamesrv(client remote.Client, addr, topic string, to time.Duration) (err error) {
	h := deleteTopicHeader(topic)
	cmd, err := client.RequestSync(addr, remote.NewCommand(deleteTopicInNamesrv, h), to)
	if err != nil {
		return requestError(err)
	}

	if cmd.Code != Success {
		return brokerError(cmd)
	}
	return
}

// GetBrokerClusterInfo get the cluster info from the namesrv
func GetBrokerClusterInfo(client remote.Client, addr string, to time.Duration) (*route.ClusterInfo, error) {
	cmd, err := client.RequestSync(addr, remote.NewCommand(getBrokerClusterInfo, nil), to)
	if err != nil {
		return nil, err
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	info := &route.ClusterInfo{}
	if len(cmd.Body) == 0 {
		return info, nil
	}

	bodyjson := strings.Replace(string(cmd.Body), ",0:", ",\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1) // fastJson key is string todo todo
	bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
	err = json.Unmarshal([]byte(bodyjson), info)
	if err != nil {
		err = dataError(err)
	}
	return info, err
}

type getTopicRouteInfoHeader string

func (h getTopicRouteInfoHeader) ToMap() map[string]string {
	return map[string]string{"topic": string(h)}
}

// GetTopicRouteInfo returns the topic information.
func GetTopicRouteInfo(client remote.Client, addr string, topic string, to time.Duration) (
	router *route.TopicRouter, err *Error,
) {
	h := getTopicRouteInfoHeader(topic)
	cmd, e := client.RequestSync(addr, remote.NewCommand(getRouteintoByTopic, h), to)
	if e != nil {
		return nil, requestError(e)
	}

	if cmd.Code != Success {
		return nil, brokerError(cmd)
	}

	if len(cmd.Body) == 0 {
		return
	}

	router = &route.TopicRouter{}
	bodyjson := strings.Replace(string(cmd.Body), ",0:", ",\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1) // fastJson key is string todo todo
	bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
	bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
	if e = json.Unmarshal([]byte(bodyjson), router); e != nil {
		return nil, dataError(e)
	}
	return
}

func readFields(str string) (map[string]string, error) {
	return nil, nil // TODO
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
				return nil, 0, errors.New("[BUG] cannot process number:" + string(d))
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
		default:
			return nil, 0, errors.New("[BUG] cannot process array:" + string(d) + ", char:" + string(n))
		}
	}

	return nil, 0, errors.New("[BUG] cannot process array:" + string(d))
}

func readObject(d []byte) ([]byte, int, error) {
	return nil, 0, nil // TODO
}

func firstNotSpace(d []byte) int {
	s := 0
	for ; unicode.IsSpace(rune(d[s])); s++ {
	}
	return s
}
