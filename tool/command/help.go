package command

type helper struct {
	name string
}

func (h *helper) Name() string {
	return "help"
}

func (h *helper) Desc() string {
	return "empty"
}

func (h *helper) Run(args []string) {
	if len(args) == 0 {
		printUsage()
		return
	}

	cmd := commands[args[0]]
	cmd.Usage()
}

func (h *helper) Usage() {}

func init() {
	RegisterCommand(&helper{})
}
