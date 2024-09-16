package main

import (
	"fmt"

	"github.com/eliben/raft/part3/raft"
)

type KVService struct {
	rs *raft.Server
}

func main() {
	kvs := &KVService{}
	fmt.Println(kvs)
}
