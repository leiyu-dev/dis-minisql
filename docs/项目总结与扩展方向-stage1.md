# 分布式 MiniSQL 项目总结与扩展方向

## 1. 项目总结

本项目在原有 `minisql/` 单机数据库内核外，实现了一套简化版分布式数据库系统。系统没有改动 MiniSQL 的存储、执行器和 SQL 解析内核，而是在 Java 层新增 Coordinator、DataNode、ZooKeeper 元数据管理、WAL 日志和 HTTP 通信接口。这样的设计符合课程要求：MiniSQL 内核没有事务系统和日志系统，因此分布式事务不作为实现目标，分布式能力主要由外层 Java 系统负责。

当前系统采用中心化 Coordinator 架构。用户请求先进入 Coordinator，Coordinator 根据 SQL 类型决定执行方式：写入请求根据分片键做哈希分片，DDL 请求广播到所有在线 DataNode，读请求可以指定分片键访问单个分片，也可以对所有分片做 scatter-gather 查询。DataNode 负责维护本地 WAL，并通过子进程调用 MiniSQL 完成真正的 SQL 执行。

系统使用 ZooKeeper 管理集群元数据。DataNode 启动后会把自己的节点信息注册为 ephemeral znode，因此节点宕机后 ZooKeeper 会自动将其从在线节点列表中移除。Coordinator 每次路由请求时读取当前在线节点和分片副本信息，从而在节点宕机后自动避开故障副本。

在容错容灾方面，当前实现重点解决“节点无正在执行任务时宕机，恢复后补执行离线期间其他副本已完成操作”的场景。每个写请求会先写入 DataNode 本地 `wal.jsonl`，WAL 中记录请求 ID、分片 ID、SQL 和时间戳。节点恢复时会从拥有相同分片的在线副本拉取 WAL，只保留自己负责分片的日志，并按 `requestId` 去重后追加到本地。之后 DataNode 在执行请求时重放 WAL，从而恢复离线期间缺失的操作。该机制实现的是最终一致性，而不是强一致性。

总体来看，本项目已经具备分布式 MiniSQL 的核心雏形：分片、备份、副本发现、节点故障切换、恢复补日志、读负载均衡和中心化查询入口均已实现。它更接近教学场景下的 BigTable/GFS 简化模型，而不是完整生产级分布式数据库。

## 2. 当前实现的主要能力

### 2.1 分片存储

系统由 Java 层选择分片键，并使用 `sha256(shardKey) mod shardCount` 计算目标分片。对于 `insert into ... values (...)`，如果请求中没有显式传入 `shardKey`，系统默认取第一列作为分片键。这样可以在不修改 MiniSQL 内核的前提下，把不同数据路由到不同 DataNode。

### 2.2 副本管理

每个分片维护多个副本。副本关系存储在 ZooKeeper 的 `/dis-minisql/shards/<shardId>` 下，节点在线状态存储在 `/dis-minisql/nodes/<nodeId>` 下。Coordinator 通过这两类信息判断某个分片当前有哪些在线副本可用。

### 2.3 容错恢复

系统使用 Java 层 WAL 解决 MiniSQL 无日志的问题。写请求先落 WAL，再交给 MiniSQL 执行。节点恢复时从同分片副本拉取 WAL，并根据 `requestId` 去重，避免重复执行同一个写入请求。

当前容错模型有明确边界：它不保证某条 SQL 执行到一半时宕机的原子性，也不处理事务回滚。它主要保证节点离线期间错过的操作可以在恢复后补执行，从而达到最终一致。

### 2.4 负载均衡

读请求使用 per-shard 轮询策略。同一个分片有多个在线副本时，Coordinator 会在这些副本之间轮流选择读节点，避免所有读请求集中在单个副本上。

### 2.5 分布式查询

系统提供中心化查询入口。对于指定分片键的查询，Coordinator 只访问对应分片；对于未指定分片键的查询，Coordinator 会访问所有分片的一个在线副本，并把各节点结果一起返回。这实现了基础的分布式查询框架。

## 4. 进一步扩展的方向

### 4.1 增强副本一致性

可以把当前“至少一个副本成功即成功”的策略改为更严格的写入确认策略
实现强一致，可以为每个分片引入 Raft。每个分片的多个副本组成一个 Raft group，所有写入先写入 Raft 日志，提交后再交给 MiniSQL 执行。这样可以解决副本日志顺序、主副本切换和脑裂问题。

### 4.2 建立全局或分片级日志序列

当前 WAL 使用 `requestId` 去重，本地 `sequence` 只表示节点内部顺序。后续可以扩展为每个分片维护单调递增的 `shardLogIndex`，由 Coordinator 分配。这样恢复时可以明确知道本节点缺少哪些日志区间，而不是只依赖拉取全量 WAL 后去重。

更进一步，可以在 ZooKeeper 中记录每个副本已应用到的 `lastAppliedIndex`，Coordinator 可以根据这些进度判断副本是否落后、是否适合承担读请求。

### 4.3 增加快照与 WAL 压缩

当前恢复需要重放完整 WAL。后续可以周期性生成快照：

- DataNode 定期把当前 MiniSQL 数据目录打包为 snapshot。
- snapshot 记录对应的最大 WAL index。
- 恢复时先加载最近 snapshot，再重放 snapshot 之后的 WAL。
- 老旧 WAL 可以在所有副本都完成 snapshot 后删除。

这可以显著缩短恢复时间，并避免 `wal.jsonl` 无限增长。

### 4.4 改进 MiniSQL 执行器接入方式

当前通过子进程调用 MiniSQL，适合作业演示，但性能有限。可以考虑以下改造：

- 保持一个长期运行的 MiniSQL 进程，DataNode 通过 stdin/stdout 持续发送 SQL。
- 给 MiniSQL 增加非交互式 batch 模式，例如 `main --execute file.sql`。
- 在 C++ 层增加轻量 RPC 接口，让 Java 通过 TCP 调用。
- 使用 JNI 直接调用 MiniSQL 的执行器接口。

其中 batch 模式改造成本最低，也最适合当前项目。长期运行进程或 RPC 模式可以减少频繁启动进程的开销。

### 4.5 完善复杂查询处理

当前系统只完成了分布式查询框架。后续可以在 Coordinator 中增加一个简单查询计划器：

- 解析 `select` 的表名、投影列和 where 条件。
- 判断查询是否可以下推到单个分片。
- 对跨分片查询做并行下发。
- 对各分片结果做合并、去重、排序和聚合。
- 对 join 查询，根据 join key 判断是否可本地 join，否则在 Coordinator 做内存 hash join。

可以优先支持等值 join，例如：

```sql
select * from user join orders on user.id = orders.user_id;
```

如果两个表使用相同分片键，可以把 join 下推到各个分片；否则需要把一侧小表广播到所有分片，或者把分片结果拉回 Coordinator 统一 join。

### 4.6 增加表级元数据和分片键管理

当前系统默认取插入语句第一列作为分片键，比较简单。后续可以增加表级元数据：

- 表名。
- 字段列表。
- 主键。
- 分片键。
- 副本数。
- 表所在分片集合。

创建表时可以扩展语法或配置，例如指定 `shard key(id)`。Coordinator 根据表元数据选择分片键，而不是依赖第一列约定。表元数据可存储在 ZooKeeper 或独立 metadata 文件中。

### 4.7 支持动态扩容和数据迁移

当前分片数量固定，节点列表也相对静态。后续可以实现：

- 新 DataNode 加入集群。
- Coordinator 重新计算分片分布。
- 从旧副本复制 snapshot 和 WAL 到新节点。
- 切换分片副本关系。
- 下线旧节点或减少热点节点压力。

为了减少扩容时大量数据迁移，可以把简单哈希改为一致性哈希，或引入虚拟分片。虚拟分片数量固定且较大，物理节点变化时只迁移部分虚拟分片。

### 4.8 增强故障检测与自动修复

目前依赖 ZooKeeper ephemeral node 判断节点在线状态。后续可以扩展：

- Coordinator 定期探测 `/health`。
- 发现副本数不足时自动选择节点补副本。
- 节点恢复后自动比较 WAL 进度。
- 对长期落后的副本标记为 `RECOVERING`，暂不承担读请求。
- 恢复完成后再切换为 `SERVING`。

这样可以避免落后副本在恢复期间被读请求选中，提高查询结果稳定性。

### 4.11 改进返回结果格式

当前 DataNode 返回 MiniSQL 原始 stdout，Coordinator 只是把多个节点响应放在 JSON 中。后续可以解析 MiniSQL 表格输出，转换为结构化格式：

```json
{
  "columns": ["id", "name"],
  "rows": [[1, "alice"], [2, "bob"]]
}
```

结构化结果有利于 Coordinator 做排序、聚合、join，也方便客户端展示。

## 5. 推荐的后续实现优先级

如果继续迭代，建议按以下顺序推进：

1. 已完成：增加分片级日志序号和副本状态。Coordinator 会为分片写入分配 `shardLogIndex`，DataNode 通过 `RECOVERING`/`SERVING` 状态避免恢复中的副本承担读写。
2. 已完成：实现 snapshot 和 WAL 压缩。DataNode 会保存 snapshot SQL，并压缩已经包含进 snapshot 的 WAL。
3. 已完成：改造 MiniSQL 调用方式。MiniSQL 主程序支持 `--batch <sql-file>`，Java 层通过 batch 文件提交 SQL，避免交互式 stdin 协议。
4. 已完成：完善 Coordinator 查询结果解析。Coordinator 会解析 MiniSQL 表格输出，合并跨分片 `select` 结果，并支持基础 `count/sum/min/max`、`order by` 和简化等值 hash join。
5. 已完成：动态扩容、Raft 风格强一致和自动化运维能力。Coordinator 提供 `/admin/rebalance`、`/admin/health`、`/admin/snapshot`，写入采用 primary、term、commitIndex 和多数派确认。

这些改造完成后，日志恢复路径已经更清晰，DataNode 恢复成本也不会随 WAL 无限增长；执行器调用也从交互式协议改为更适合服务端调用的 batch 模式；Coordinator 具备了基础结果解析、跨分片查询后处理、动态扩容和多数派写入能力。后续重点可以转向完整 Raft 成员变更、热点感知迁移和更完整 SQL 优化器。

## 6. 总体评价

当前系统已经完成了从单机 MiniSQL 到分布式 MiniSQL 的关键跨越。它通过 Java 外层系统补齐了单机内核缺失的分片、副本、故障发现、WAL 和恢复能力，在实现复杂度可控的前提下覆盖了作业要求中的主要功能。

不过，当前实现仍然是教学型原型系统，重点在展示分布式数据库的核心机制，而不是追求生产级完整性。后续如果继续扩展，应围绕“一致性更强、恢复更快、查询更完整、运维更自动化”四个方向逐步完善。
