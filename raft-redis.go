package main

import (
	"flag"
	"raft"
	"strings"
	"github.com/coreos/etcd/raft/raftpb"
	"store"
	"redis"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 6389, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *store.KvStore
	getSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady := raftd.NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = store.NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)



	server, err := redis.NewServer(store.DefaultConfig(confChangeC,kvs,*kvport))
	if err != nil {
		panic(err)
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	}()

	// exit when raftd goes down
	if err, ok := <-errorC; ok {
		panic(err)
	}
}
