package kvclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/eliben/raft/part4kv/api"
)

const DebugClient = 1

type KVClient struct {
	addrs []string

	// assumedLeader is the index (in addrs) of the service we assume is the
	// current leader. It is zero-initialized by default, without loss of
	// generality.
	assumedLeader int
}

// New creates a new KVClient. serviceAddrs is the addresses (each a string
// with the format "host:port") of the services in the KVService cluster the
// client will contact.
func New(serviceAddrs []string) *KVClient {
	return &KVClient{
		addrs:         serviceAddrs,
		assumedLeader: 0,
	}
}

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

func (c *KVClient) send(ctx context.Context, route string, req any, resp api.Response) error {
	for {
		path := fmt.Sprintf("http://%s/%s/", c.addrs[c.assumedLeader], route)
		c.clientlog("sending %v to %v", req, path)
		if err := sendJSONRequest(ctx, path, req, resp); err != nil {
			return err
		}
		c.clientlog("received response %v", resp)

		switch resp.Status() {
		case api.StatusNotLeader:
			c.clientlog("not leader: will try next address")
			c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
		case api.StatusOK:
			return nil
		case api.StatusFailedCommit:
			return fmt.Errorf("commit failed; please retry")
		default:
			panic("unreachable")
		}
	}
}

// clientlog logs a debugging message if DebugClient > 0
func (c *KVClient) clientlog(format string, args ...any) {
	if DebugClient > 0 {
		format = "[client] " + format
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
		return fmt.Errorf("issuing HTTP request: %w", err)
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(respData); err != nil {
		return fmt.Errorf("JSON-decoding response data: %w", err)
	}
	return nil
}
