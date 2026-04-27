# 分布式 MiniSQL

本目录是在 `minisql/` C++ 内核外实现的 Java 分布式层。它把 MiniSQL 当作本地 SQL 执行器，由 Java 负责分片、复制、ZooKeeper 元数据、WAL、恢复、负载均衡和中心化查询协调。

## 架构

- `CoordinatorServer`：中心协调节点，对外提供 `/sql`，负责选择分片键、哈希路由、写副本、读负载均衡和 scatter-gather 查询。
- `DataNodeServer`：数据节点，对内提供 `/execute`、`/wal`、`/health`，写请求先追加本地 `wal.jsonl`，再调用 MiniSQL 执行。
- `ZkMetadataStore`：在 ZooKeeper 中维护 `/dis-minisql/nodes` 在线节点和 `/dis-minisql/shards` 分片副本表。
- `MiniSqlCli`：以子进程方式调用 `../minisql/build/bin/main`。由于原 MiniSQL 不自动加载数据库，DataNode 会在执行前重放 WAL，从而支持节点宕机后的最终一致恢复。

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

## 容错说明

ZooKeeper ephemeral node 用于在线节点发现。Coordinator 写入时会把请求发送到该分片当前在线的所有副本；一个副本宕机时，后续读写会自动避开它。DataNode 恢复启动后，会从共享分片的在线副本拉取 WAL 并追加缺失日志，然后之后的请求通过 WAL 重放恢复执行状态。

该实现选择最终一致性：只要每个分片仍有至少一个副本存活，系统可以继续服务；恢复节点会通过 WAL 追赶。由于原 MiniSQL 无事务和日志，本项目不实现分布式事务。
