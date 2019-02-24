package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zjykzk/rocketmq-client-go/message"
)

func TestAvg(t *testing.T) {
	queues := []*message.Queue{
		&message.Queue{
			QueueID: 1,
		},
		&message.Queue{
			QueueID: 2,
		},
		&message.Queue{
			QueueID: 3,
		},
		&message.Queue{
			QueueID: 4,
		},
		&message.Queue{
			QueueID: 5,
		},
		&message.Queue{
			QueueID: 6,
		},
	}

	avg := Averagely{}
	ret, err := avg.Assign("", "", nil, nil)
	assert.NotNil(t, err)
	ret, err = avg.Assign("", "1", nil, nil)
	assert.NotNil(t, err)
	ret, err = avg.Assign("", "1", nil, queues)
	assert.NotNil(t, err)

	ret, err = avg.Assign("", "1", []string{"2"}, queues)
	assert.NotNil(t, err)

	ret, err = avg.Assign("", "1", []string{"1"}, queues)
	assert.Equal(t, queues, ret)

	type testCase struct {
		clientID     string
		allClientIDs []string
		expectedQIDs []int
	}

	cases := []testCase{
		{
			clientID:     "2",
			allClientIDs: []string{"1", "2"},
			expectedQIDs: []int{4, 5, 6},
		},
		{
			clientID:     "1",
			allClientIDs: []string{"1", "2", "3", "4"},
			expectedQIDs: []int{1, 2},
		},
		{
			clientID:     "2",
			allClientIDs: []string{"1", "2", "3", "4"},
			expectedQIDs: []int{3, 4},
		},
		{
			clientID:     "4",
			allClientIDs: []string{"1", "2", "3", "4"},
			expectedQIDs: []int{6},
		},
		{
			clientID:     "6",
			allClientIDs: []string{"1", "2", "3", "4", "5", "6"},
			expectedQIDs: []int{6},
		},
		{
			clientID:     "7",
			allClientIDs: []string{"1", "2", "3", "4", "5", "6", "7"},
			expectedQIDs: []int{1},
		},
	}

	run := func() {
		for _, c := range cases {
			ret, err = avg.Assign("", c.clientID, c.allClientIDs, queues)
			assert.Nil(t, err)
			assert.Equal(t, len(c.expectedQIDs), len(ret))

			for i, id := range c.expectedQIDs {
				assert.Equal(t, id, ret[i].QueueID)
			}
		}
	}

	run()

	queues = []*message.Queue{
		&message.Queue{
			QueueID: 1,
		},
		&message.Queue{
			QueueID: 2,
		},
		&message.Queue{
			QueueID: 3,
		},
	}
	cases = []testCase{
		{
			clientID:     "7",
			allClientIDs: []string{"1", "2", "3", "4", "5", "6", "7"},
			expectedQIDs: []int{1},
		},
		{
			clientID:     "3",
			allClientIDs: []string{"1", "2", "3", "4", "5", "6", "7"},
			expectedQIDs: []int{3},
		},
	}
	run()

	queues = []*message.Queue{
		&message.Queue{
			QueueID: 1,
		},
	}
	cases = []testCase{
		{
			clientID:     "7",
			allClientIDs: []string{"1", "2", "3", "4", "5", "6", "7"},
			expectedQIDs: []int{1},
		},
	}
	run()
}
