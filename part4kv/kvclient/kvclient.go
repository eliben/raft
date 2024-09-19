package kvclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/eliben/raft/part4kv/api"
)

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
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)
	if err := enc.Encode(api.PutRequest{Key: key, Value: value}); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, c.addrs[0], body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	var pr api.PutResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&pr); err != nil {
		return err
	}

	fmt.Println(pr)
	return nil
}
