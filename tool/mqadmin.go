package main

import (
	"os"

	"github.com/zjykzk/rocketmq-client-go/tool/command"

	_ "github.com/zjykzk/rocketmq-client-go/tool/admin"
	_ "github.com/zjykzk/rocketmq-client-go/tool/consumer"
	_ "github.com/zjykzk/rocketmq-client-go/tool/message"
)

func main() {
	command.Run(os.Args[1:])
}
