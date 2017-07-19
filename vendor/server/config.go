package server

import (
	"github.com/coreos/etcd/raft/raftpb"
)

type Config struct {
	Host    string
	Port    int
	Handler interface{}
	ConfChangeC chan<- raftpb.ConfChange
}

func DefaultConfig(port int) *Config {
	return &Config{
		Host:    "",
		Port:    port,
		ConfChangeC:confChangeC,
	}
}

