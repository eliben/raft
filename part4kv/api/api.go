package api

// Defines the data structures used in the REST API between kvservice and
// clients. Uses bespoke ResponseStatus per response instead of HTTP status
// codes because some statuses like "not leader" or "failed commit" don't have a
// good match in standard HTTP status codes.

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
	Status ResponseStatus
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Status ResponseStatus
	Value  string
}

type ResponseStatus int

const (
	StatusInvalid ResponseStatus = iota
	StatusOK
	StatusNotLeader
	StatusFailedCommit
)

var responseName = map[ResponseStatus]string{
	StatusInvalid:      "invalid",
	StatusOK:           "OK",
	StatusNotLeader:    "NotLeader",
	StatusFailedCommit: "FailedCommit",
}

func (rs ResponseStatus) String() string {
	return responseName[rs]
}
