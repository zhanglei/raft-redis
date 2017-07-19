# raft-redis

基于raft协议试下的简单redis 服务器

# 开始使用
### 单一节点跑raft-redis

第一步：

```sh
raft-redis --id 1 --cluster http://127.0.0.1:12379 --port 6389
```

第二步：

```
redis-cli -p 6389 set key hello
```

最后：

```
redis-cli -p 6389 get key
```

### 本地伪分布式集群

首先安装 [goreman](https://github.com/mattn/goreman), 然后执行如下shell，会启动3个raft-redis 实例

```sh
goreman start
```


### 动态拓展集群机器

raft-redis 支持使用redis 命令行工具（redis-cli）使用 addnode 命令动态新增集群机器.

如下实例, 首先开启三节点集群实例:
```sh
raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 6389
raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 6399
raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 6169
```

第4个节点可以使用 addnode 动态添加
```sh
redis-cli -p 6389 addnode 4 http://127.0.0.1:42379
```

然后节点4机器使用如下命令启动，主要参数 join
```sh
raft-redis --id 4 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379,http://127.0.0.1:42379 --port 6059 --join
```

同样在节点摘除的时候使用如下命令:
```sh
redis-cli -p 6389 removenode 4 http://127.0.0.1:42379
```

# benchmark

![benchmark](https://github.com/raft-redis/doc/benchmark.png)

# 感谢

[etcd-raft](https://github.com/coreos/etcd/tree/master/raft)

[go-redis-server](https://github.com/docker/go-redis-server)