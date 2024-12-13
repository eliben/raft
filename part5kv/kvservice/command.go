// Command type: stored in Raft by the KV service.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package kvservice

// Command is the concrete command type KVService submits to the Raft log to
// manage its state machine. It's also used to carry the results of the command
// after it's applied to the state machine. These are the supported commands:
//
// CommandGet: queries a key's value
//
// * Key is the key to get, Value is ignored
// * CompareValue is ignored
// * ResultFound is true iff Key was found in the store
// * ResultValue is the value, if Key was found in the store
//
// CommandPut: assigns value to the key
//
// * Key,Value are the pair to assign (store[key]=value)
// * CompareValue is ignored
// * ResultFound is true iff Key was previously found in the store
// * ResultValue is the old value of Key, if it was previously found
//
// CommandAppend: appends to a key's value
//
//	If Key wasn't previously in the store, it's created with the given
//	Value (as if it was present with an empty value before this operation).
//
// * Performs Store[Key] = Store[Key] + Value, where "+" is a string append
// * CompareValue is ignored
// * ResultFound is true iff the Key was found in the store
// * ResultValue is the old value of Key, before the append
//
// CommandCAS: atomic compare-and-swap, performs:
//
//	if Store[Key] == CompareValue {
//	  Store[Key] = Value
//	} else {
//	  nop
//	}
//
// * Key is the key this command acts on
// * CompareValue is the previous value the command compares to
// * Value is the new value the command assigns
// * ResultFound is true iff Key was previously found in the store
// * ResultValue is the old value of Key, if it was previously found
type Command struct {
	Kind CommandKind

	Key, Value string

	CompareValue string

	ResultValue string
	ResultFound bool

	// ServiceID is the Raft ID of the service submitting this command.
	ServiceID int

	// ClientID and RequestID uniquely identify the request+client.
	ClientID, RequestID int64

	// IsDuplicate is used to mark the command as a duplicate by the updater. When
	// the updater notices a command that has a client+request ID that has already
	// been executed, the command is not applied to the datastore; instead,
	// IsDuplicate is set to true.
	IsDuplicate bool
}

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
	CommandAppend
	CommandCAS
)

var commandName = map[CommandKind]string{
	CommandInvalid: "invalid",
	CommandGet:     "get",
	CommandPut:     "put",
	CommandAppend:  "append",
	CommandCAS:     "cas",
}

func (ck CommandKind) String() string {
	return commandName[ck]
}
