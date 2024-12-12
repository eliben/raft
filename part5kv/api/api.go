// REST API data types for the KV service.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package api

// Defines the data structures used in the REST API between kvservice and
// clients. These structs are JSON-encoded into the body of HTTP requests
// and responses passed between services and clients.
// Uses bespoke ResponseStatus per response instead of HTTP status
// codes because some statuses like "not leader" or "failed commit" don't have a
// good match in standard HTTP status codes.
// Each request type has fields of unique identification of requests.

type Response interface {
	Status() ResponseStatus
}

type PutRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
}

type PutResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (pr *PutResponse) Status() ResponseStatus {
	return pr.RespStatus
}

type AppendRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
}

type AppendResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (ar *AppendResponse) Status() ResponseStatus {
	return ar.RespStatus
}

type GetRequest struct {
	Key string

	ClientID  int64
	RequestID int64
}

type GetResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	Value      string
}

func (gr *GetResponse) Status() ResponseStatus {
	return gr.RespStatus
}

type CASRequest struct {
	Key          string
	CompareValue string
	Value        string

	ClientID  int64
	RequestID int64
}

type CASResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (cr *CASResponse) Status() ResponseStatus {
	return cr.RespStatus
}

type ResponseStatus int

const (
	StatusInvalid ResponseStatus = iota
	StatusOK
	StatusNotLeader
	StatusFailedCommit
	StatusDuplicateRequest
)

var responseName = map[ResponseStatus]string{
	StatusInvalid:          "invalid",
	StatusOK:               "OK",
	StatusNotLeader:        "NotLeader",
	StatusFailedCommit:     "FailedCommit",
	StatusDuplicateRequest: "DuplicateRequest",
}

func (rs ResponseStatus) String() string {
	return responseName[rs]
}
