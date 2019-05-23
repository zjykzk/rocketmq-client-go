package command

import (
	"fmt"
	"log"
)

type command interface {
	Name() string
	Desc() string
	Run(args []string)
	Usage()
}

var (
	commands = make(map[string]command, 32)
)

// RegisterCommand register the command in the admin
func RegisterCommand(cmd command) {
	name := cmd.Name()
	old := commands[name]
	if old != nil {
		log.Printf("[WARN] %s exist, %v", name, old)
	}

	commands[name] = cmd
}

func printUsage() {
	println(
		`mqadmin is a tool for manage RocketMQ.

Usage:

        mqadmin <command> [arguments]

The commands are:
`)
	for _, v := range commands {
		if v.Name() == "help" {
			continue
		}

		fmt.Printf("\t%-20s%s\n", v.Name(), v.Desc())
	}

	println(`
Use "mqadmin help <command> for more information about a command."
`)
}

// Run run the command
func Run(args []string) {
	if len(args) < 1 {
		printUsage()
		return
	}

	cmd := commands[args[0]]
	if cmd == nil {
		printUsage()
		return
	}

	cmd.Run(args[1:])
}
