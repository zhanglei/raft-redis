package main

import (
	redis "redis"
)

func main() {
	server, err := redis.NewServer(redis.DefaultConfig().SnapDir("data/snap/").WalDir("data/wal/"))
	if err != nil {
		panic(err)
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
