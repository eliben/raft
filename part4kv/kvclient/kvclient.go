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

func (c *KVClient) Put(ctx context.Context, key string, value string) error {
	for {
		path := fmt.Sprintf("http://%s/put/", c.addrs[c.assumedLeader])
		putReq := api.PutRequest{
			Key:   key,
			Value: value,
		}
		c.clientlog("sending %v to %v", putReq, path)
		putResp, err := sendJSONRequest[api.PutResponse](ctx, path, putReq)
		if err != nil {
			return err
		}
		c.clientlog("received response %v", putResp)

		switch putResp.Status {
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

func sendJSONRequest[ResponseT any](ctx context.Context, path string, data any) (*ResponseT, error) {
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)
	if err := enc.Encode(data); err != nil {
		return nil, fmt.Errorf("JSON-encoding request data: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, body)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("issuing HTTP request: %w", err)
	}

	var responseData ResponseT
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&responseData); err != nil {
		return nil, fmt.Errorf("JSON-decoding response data: %w", err)
	}
	return &responseData, nil
}
