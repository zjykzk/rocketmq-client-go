package command

import (
	"log"
)

type command interface {
	Name() string
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
	for _, v := range commands {
		v.Usage()
	}
}

// Run run the command
func Run(args []string) {
	if len(args) < 1 {
		printUsage()
		return
	}
	cmdName := args[0]
	cmd := commands[cmdName]
	if cmd == nil {
		printUsage()
		return
	}

	cmd.Run(args[1:])
}
