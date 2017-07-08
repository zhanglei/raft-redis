package store

import (
	"github.com/coreos/etcd/raft/raftpb"
)

type Config struct {
	Proto   string
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
		Proto:   "tcp",
		Host:    "127.0.0.1",
		Port:    port,
	//	handler: NewDefaultHandler(),
		SnapDir:"/home/wida/data/snap/",
		WalDir:"/home/wida/data/wal/",
		ConfChangeC:ConfChangeC,
		Kv:kv,
	}
}

