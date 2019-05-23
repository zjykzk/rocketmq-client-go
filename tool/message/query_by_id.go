package message

import (
	"flag"
	"fmt"
	"os"

	"github.com/zjykzk/rocketmq-client-go/admin"
	"github.com/zjykzk/rocketmq-client-go/log"
	"github.com/zjykzk/rocketmq-client-go/tool/command"
)

func init() {
	cmd := &queryByID{}
	flags := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
	flags.StringVar(&cmd.offsetID, "i", "", "message id")
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", cmd.Name())
		flags.PrintDefaults()
	}
	cmd.flags = flags

	command.RegisterCommand(cmd)
}

type queryByID struct {
	offsetID string
	flags    *flag.FlagSet
}

func (q *queryByID) Name() string {
	return "queryByID"
}

func (q *queryByID) Desc() string {
	return "query the message by the offset id"
}

func (q *queryByID) Run(args []string) {
	q.flags.Parse(args)

	if len(q.offsetID) == 0 {
		fmt.Println("empty message id: [" + q.offsetID + "]")
		return
	}

	logger := log.Std
	a := admin.NewAdmin([]string{"X"}, logger)
	a.Start()

	msg, err := a.QueryMessageByID(q.offsetID)
	if err != nil {
		fmt.Printf("Error:%v\n", err)
		return
	}
	fmt.Printf("%v\n", msg)
}

func (q *queryByID) Usage() {
	q.flags.Usage()
}
