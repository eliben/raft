package kvservice

// Command is the concrete command type KVService submits to the Raft log to
// manage its state machine. It's also used to carry the results of the command
// after it's applied to the state machine.
//
// Supported commands:
//
// CommandGet:
//
// * Key is the key to get, Value is ignored
// * ResultFound is true iff Key was found in the store
// * ResultValue is the value, if Key was found in the store
//
// CommandPut: assigns value to the key
//
// * Key,Value are the pair to assign (store[key]=value)
// * ResultFound is true iff Key was previously found in the store
// * ResultValue is the old value of Key, if it was previously found
type Command struct {
	Kind CommandKind

	Key, Value string

	ResultValue string
	ResultFound bool

	// id is the Raft ID of the server submitting this command.
	Id int
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
