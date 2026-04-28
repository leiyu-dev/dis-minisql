# 分布式 MiniSQL

本目录是在 `minisql/` C++ 内核外实现的 Java 分布式层。它把 MiniSQL 当作本地 SQL 执行器，由 Java 负责分片、复制、ZooKeeper 元数据、WAL、恢复、负载均衡和中心化查询协调。

## 架构

- `CoordinatorServer`：中心协调节点，对外提供 `/sql` 和 `/admin/*`，负责选择分片键、哈希路由、Raft 风格多数派写入、读负载均衡、scatter-gather 查询和动态扩容管理。
- `DataNodeServer`：数据节点，对内提供 `/execute`、`/wal`、`/health`，写请求先追加本地 `wal.jsonl`，再调用 MiniSQL batch 模式执行。
- `ZkMetadataStore`：在 ZooKeeper 中维护 `/dis-minisql/nodes` 在线节点和 `/dis-minisql/shards` 分片副本表。
- `MiniSqlCli`：以子进程方式调用 `../minisql/build/bin/main --batch <sql-file>`。DataNode 会维护 snapshot 和 snapshot 后的 WAL 增量，恢复时只重放缺失增量。

## 构建

先构建 C++ 内核：

```bash
cd ../minisql
mkdir -p build
cd build
cmake ..
make -j
```

再构建 Java 分布式层：

```bash
cd ../../dis-minisql
mvn package
```

## 单机三节点演示

复制示例配置后按需修改真实机器 IP：

```bash
cp src/main/resources/cluster.example.json cluster.json
./scripts/start-zookeeper.sh
java -jar target/dis-minisql-1.0.0.jar init-zk cluster.json
./scripts/start-node.sh cluster.json node-a
./scripts/start-node.sh cluster.json node-b
./scripts/start-node.sh cluster.json node-c
./scripts/start-coordinator.sh cluster.json
```

提交 SQL：

```bash
curl -X POST http://127.0.0.1:8080/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"insert into t values (1, '\''alice'\'');"}'
```

读请求如果传入 `shardKey`，只访问对应分片的一个在线副本；不传则访问每个分片的一个副本并返回多个结果：

```bash
curl -X POST http://127.0.0.1:8080/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select * from t;","shardKey":"1"}'
```

Coordinator 会在响应的 `mergedOutput` 字段中返回解析后的合并结果。当前支持跨分片 `select` 合并、`order by`、`count/sum/min/max` 以及简化等值 join：

```bash
curl -X POST http://127.0.0.1:8080/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select count(*) from t;"}'

curl -X POST http://127.0.0.1:8080/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"select * from user join orders on user.id = orders.user_id;"}'
```

## 运维接口

```bash
curl http://127.0.0.1:8080/admin/health
curl -X POST http://127.0.0.1:8080/admin/rebalance
curl -X POST http://127.0.0.1:8080/admin/snapshot
```

- `/admin/health` 返回在线节点、分片、副本、primary、term 和 commitIndex。
- `/admin/rebalance` 按当前在线节点重新计算分片副本，用于新 DataNode 加入后的动态扩容。
- `/admin/snapshot` 触发在线 DataNode 生成 snapshot 并压缩 WAL。

## 容错说明

ZooKeeper ephemeral node 用于在线节点发现。Coordinator 写入时会为每个分片写请求分配递增的 `shardLogIndex`，确认当前 primary 和 term，然后把请求发送到该分片在线的 `SERVING` 副本。只有多数派副本成功后，Coordinator 才推进分片 `commitIndex` 并返回成功；一个副本宕机时，后续读写会自动避开它。

DataNode 恢复启动时先进入 `RECOVERING` 状态，从共享分片的在线副本拉取 WAL，按 `requestId` 去重并追加缺失日志，再应用 snapshot SQL 之后的 WAL 增量。恢复完成后节点切换为 `SERVING`，才会被 Coordinator 选作读写副本。DataNode 会周期性生成 snapshot SQL 并压缩已包含进 snapshot 的 WAL。

该实现是教学版 Raft 风格强一致：每个分片有 primary、term、commitIndex，多数派确认后提交。它不实现完整 Raft 的日志回滚和选举超时，但已经把写成功条件从“任意副本成功”提升为“多数派成功”。

## 系统测试

```bash
python3 scripts/e2e_system_test.py
```

测试会自动启动临时 ZooKeeper、三个初始 DataNode 和 Coordinator，覆盖建库建表、Raft 多数派写入、聚合、排序、join、单节点故障后继续写入、第四节点加入、动态 rebalance、健康检查和 snapshot 运维接口。
