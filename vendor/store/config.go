package store

import (
	"github.com/coreos/etcd/raft/raftpb"
)

type Config struct {
	Host    string
	Port    int
	Handler interface{}
	SnapDir string
	WalDir  string
	ConfChangeC chan<- raftpb.ConfChange
	Kv *KvStore
}

func DefaultConfig( ConfChangeC chan raftpb.ConfChange,kv *KvStore,port int) *Config {
	return &Config{
		Host:    "",
		Port:    port,
		ConfChangeC:ConfChangeC,
		Kv:kv,
	}
}

