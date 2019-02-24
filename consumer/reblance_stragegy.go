package consumer

import (
	"errors"

	"github.com/zjykzk/rocketmq-client-go/message"
)

// Averagely reblance average
type Averagely struct{}

var errEmptyCurrentClientID = errors.New("empty current client id")
var errEmptyClientIDs = errors.New("empty client ids")
var errEmptyQueues = errors.New("empty queues")
var errNotFoundCurrentClientID = errors.New("not found current id")

func clientIndex(clientID string, clientIDs []string) int {
	for i, c := range clientIDs {
		if c == clientID {
			return i
		}
	}
	return -1
}

// Assign assign averagely
func (a *Averagely) Assign(group, curClientID string, clientIDs []string, queues []*message.Queue) (
	[]*message.Queue, error,
) {
	if curClientID == "" {
		return nil, errEmptyCurrentClientID
	}

	if len(clientIDs) == 0 {
		return nil, errEmptyClientIDs
	}

	if len(queues) == 0 {
		return nil, errEmptyQueues
	}

	idx := clientIndex(curClientID, clientIDs)
	if -1 == idx {
		return nil, errNotFoundCurrentClientID
	}

	queueCount, clientCount := len(queues), len(clientIDs)

	if clientCount >= queueCount {
		idx %= queueCount
		return queues[idx : idx+1], nil
	}

	mod := queueCount % clientCount
	size := queueCount / clientCount
	s := size*idx + mod

	if mod > idx {
		size++
		s = size * idx
	}

	return queues[s : s+size], nil
}

// Name return reblance's name
func (a *Averagely) Name() string {
	return "AVERAGE"
}
