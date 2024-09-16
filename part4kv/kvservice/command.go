package kvservice

// Command is the concrete command type KVService submits to the Raft log to
// manage its state machine.
//
// Supported kinds:
//
// CommandGet: key is the key to get, value is ignored
// CommandPut: assigns value to the key
type Command struct {
	kind CommandKind

	key, value string

	// id is the Raft ID of the server submitting this command.
	id int
}

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
)

var commandName = map[CommandKind]string{
	CommandInvalid: "invalid",
	CommandGet:     "get",
	CommandPut:     "put",
}

func (ck CommandKind) String() string {
	return commandName[ck]
}
