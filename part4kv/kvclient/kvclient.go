package kvclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/eliben/raft/part4kv/api"
)

const DebugClient = 1

type KVClient struct {
	addrs []string
}

// New creates a new KVClient. serviceAddrs is the addresses (each a string
// with the format "host:port") of the services in the KVService cluster the
// client will contact.
func New(serviceAddrs []string) *KVClient {
	return &KVClient{
		addrs: serviceAddrs,
	}
}

func (c *KVClient) Put(key string, value string) error {
	path := fmt.Sprintf("http://%s/put/", c.addrs[0])
	putReq := api.PutRequest{
		Key:   key,
		Value: value,
	}
	c.clientlog("sending %v to %v", putReq, path)
	putResp, err := sendJSONRequest[api.PutResponse](path, putReq)
	if err != nil {
		return err
	}
	c.clientlog("received response %v", putResp)
	return nil
}

// clientlog logs a debugging message if DebugClient > 0
func (c *KVClient) clientlog(format string, args ...any) {
	if DebugClient > 0 {
		format = "[client] " + format
		log.Printf(format, args...)
	}
}

func sendJSONRequest[ResponseT any](path string, data any) (*ResponseT, error) {
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)
	if err := enc.Encode(data); err != nil {
		return nil, fmt.Errorf("JSON-encoding request data: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, path, body)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
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
