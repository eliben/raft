module github.com/eliben/raft/part4kv

go 1.23.1

replace github.com/eliben/raft/part3/raft => ../part3/raft/

require github.com/eliben/raft/part3/raft v0.0.0-00010101000000-000000000000

require github.com/fortytw2/leaktest v1.3.0 // indirect
