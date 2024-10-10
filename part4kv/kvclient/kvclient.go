// KV client library. Go code that wants to talk to the KV service should use
// this client instead of sending REST requests directly.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package kvclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/eliben/raft/part4kv/api"
)

// DebugClient enables debug output
const DebugClient = 1

type KVClient struct {
	addrs []string

	// assumedLeader is the index (in addrs) of the service we assume is the
	// current leader. It is zero-initialized by default, without loss of
	// generality.
	assumedLeader int

	clientID int32
}

// New creates a new KVClient. serviceAddrs is the addresses (each a string
// with the format "host:port") of the services in the KVService cluster the
// client will contact.
func New(serviceAddrs []string) *KVClient {
	return &KVClient{
		addrs:         serviceAddrs,
		assumedLeader: 0,
		clientID:      clientCount.Add(1),
	}
}

// clientCount is used internally for debugging
var clientCount atomic.Int32

// Put the key=value pair into the store. Returns an error, or
// (prevValue, keyFound, false), where keyFound specifies whether the key was
// found in the store prior to this command, and prevValue is its previous
// value if it was found.
func (c *KVClient) Put(ctx context.Context, key string, value string) (string, bool, error) {
	putReq := api.PutRequest{
		Key:   key,
		Value: value,
	}
	var putResp api.PutResponse
	err := c.send(ctx, "put", putReq, &putResp)
	return putResp.PrevValue, putResp.KeyFound, err
}

// Get the value of key from the store. Returns an error, or
// (value, found, false), where found specifies whether the key was found in
// the store, and value is its value.
func (c *KVClient) Get(ctx context.Context, key string) (string, bool, error) {
	getReq := api.GetRequest{
		Key: key,
	}
	var getResp api.GetResponse
	err := c.send(ctx, "get", getReq, &getResp)
	return getResp.Value, getResp.KeyFound, err
}

// CAS operation: if prev value of key == compare, assign new value. Returns an
// error, or (prevValue, keyFound, false), where keyFound specifies whether the
// key was found in the store prior to this command, and prevValue is its
// previous value if it was found.
func (c *KVClient) CAS(ctx context.Context, key string, compare string, value string) (string, bool, error) {
	casReq := api.CASRequest{
		Key:          key,
		CompareValue: compare,
		Value:        value,
	}
	var casResp api.CASResponse
	err := c.send(ctx, "cas", casReq, &casResp)
	return casResp.PrevValue, casResp.KeyFound, err
}

func (c *KVClient) send(ctx context.Context, route string, req any, resp api.Response) error {
	// This loop rotates through the list of service addresses until we get
	// a response that indicates we've found the leader of the cluster. It
	// starts at c.assumedLeader
FindLeader:
	for {
		// There's a two-level context tree here: we have the user context - ctx,
		// and we create our own context to impose a timeout on each request to
		// the service. If our timeout expires, we move on to try the next service.
		// In the meantime, we have to keep an eye on the user context - if that's
		// canceled at any time (due to timeout, explicit cancellation, etc), we
		// bail out.
		retryCtx, retryCtxCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		path := fmt.Sprintf("http://%s/%s/", c.addrs[c.assumedLeader], route)

		c.clientlog("sending %#v to %v", req, path)
		if err := sendJSONRequest(retryCtx, path, req, resp); err != nil {
			// Since the contexts are nested, the order of testing here matters.
			// We have to check the parent context first - if it's done, it means
			// we have to return.
			if contextDone(ctx) {
				c.clientlog("parent context done; bailing out")
				retryCtxCancel()
				return err
			} else if contextDeadlineExceeded(retryCtx) {
				// If the parent context is not done, but our retry context is done,
				// it's time to retry a different service.
				c.clientlog("timed out: will try next address")
				c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
				retryCtxCancel()
				continue FindLeader
			}
			retryCtxCancel()
			return err
		}
		c.clientlog("received response %#v", resp)

		// No context/timeout on this request - we've actually received a response.
		switch resp.Status() {
		case api.StatusNotLeader:
			c.clientlog("not leader: will try next address")
			c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
			retryCtxCancel()
			continue FindLeader
		case api.StatusOK:
			retryCtxCancel()
			return nil
		case api.StatusFailedCommit:
			retryCtxCancel()
			return fmt.Errorf("commit failed; please retry")
		default:
			panic("unreachable")
		}
	}
}

// clientlog logs a debugging message if DebugClient > 0
func (c *KVClient) clientlog(format string, args ...any) {
	if DebugClient > 0 {
		clientName := fmt.Sprintf("[client%03d]", c.clientID)
		format = clientName + " " + format
		log.Printf(format, args...)
	}
}

func sendJSONRequest(ctx context.Context, path string, reqData any, respData any) error {
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)
	if err := enc.Encode(reqData); err != nil {
		return fmt.Errorf("JSON-encoding request data: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, body)
	if err != nil {
		return fmt.Errorf("creating HTTP request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(respData); err != nil {
		return fmt.Errorf("JSON-decoding response data: %w", err)
	}
	return nil
}

// contextDone checks whether ctx is done for any reason. It doesn't block.
func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// contextDeadlineExceeded checks whether ctx is done because of an exceeded
// deadline. It doesn't block.
func contextDeadlineExceeded(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return true
		}
	default:
	}
	return false
}
