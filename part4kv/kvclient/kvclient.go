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
	body := new(bytes.Buffer)
	enc := json.NewEncoder(body)
	if err := enc.Encode(api.PutRequest{Key: key, Value: value}); err != nil {
		return err
	}

	path := fmt.Sprintf("http://%s/put/", c.addrs[0])
	req, err := http.NewRequest(http.MethodPost, path, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	c.clientlog("sending PUT request to %v: %v=%v", c.addrs[0], key, value)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	//bb, err := io.ReadAll(resp.Body)
	//if err != nil {
	//panic(err)
	//}
	//fmt.Println("YY", string(bb))

	var pr api.PutResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&pr); err != nil {
		return err
	}

	fmt.Println(pr)
	return nil
}

// clientlog logs a debugging message if DebugClient > 0
func (c *KVClient) clientlog(format string, args ...any) {
	if DebugClient > 0 {
		format = "[client] " + format
		log.Printf(format, args...)
	}
}
