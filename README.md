# 分布式 MiniSQL 系统

## 项目概述

本项目以 MiniSQL（C++ 单机数据库内核）为基础，使用 Java 实现了一套完整的分布式数据库系统。系统支持数据分布、集群管理、分布式查询、副本管理、容错容灾、负载均衡及跨节点 JOIN 等特性，使用 ZooKeeper 管理集群元数据与节点状态。

---

## 系统架构总览

```
  ┌────────────────────────────────────────────────┐
  │                   SQL Client                   │
  │            (client 模块, Java)                 │
  └──────────────────────┬─────────────────────────┘
                         │  TCP / JSON 协议
  ┌──────────────────────▼─────────────────────────┐
  │                  Coordinator                   │
  │    SQL 路由 · 元数据管理 · 结果合并 · JOIN      │
  │            (coordinator 模块, Java)            │
  └──┬───────────────────┬───────────────────┬─────┘
     │                   │                   │
     ▼                   ▼                   ▼
 DataNode-1          DataNode-2          DataNode-3
 (datanode,          (datanode,          (datanode,
  Java 进程)          Java 进程)          Java 进程)
     │                   │                   │
     ▼                   ▼                   ▼
 [minisql]           [minisql]           [minisql]
  子进程               子进程               子进程
  (C++ 内核)           (C++ 内核)           (C++ 内核)

                  ZooKeeper 集群
     /dis-minisql/nodes/{nodeId}      ← 数据节点注册
     /dis-minisql/databases/{name}    ← 数据库元数据
     /dis-minisql/tables/{db}/{tbl}   ← 表元数据 + 分片键
     /dis-minisql/shards/{db}/{tbl}/{id} ← 分片分配信息
```

---

## 目录结构

```
dis-minisql/
├── minisql/                    C++ 数据库内核（已提供，不修改）
├── common/                     公共模块（协议、工具类）
├── datanode/                   数据节点模块
├── coordinator/                协调节点模块
├── client/                     客户端模块
├── docs/                       设计文档与实验报告
├── pom.xml                     Maven 父 POM
├── start-coordinator.sh        启动协调节点脚本
├── start-datanode.sh           启动数据节点脚本
└── start-client.sh             启动客户端脚本
```

---

## 快速上手

### 1. 编译 MiniSQL C++ 内核

```bash
cd minisql
mkdir -p build && cd build
cmake .. && make -j4
# 生成可执行文件: minisql/build/minisql
```

### 2. 编译 Java 项目

```bash
cd /path/to/dis-minisql
mvn package -DskipTests
```

### 3. 启动 ZooKeeper

```bash
# 单机模式（开发/测试）
zkServer.sh start
```

### 4. 启动数据节点（3 个节点示例）

```bash
# 终端 1
./start-datanode.sh node1 9001

# 终端 2
./start-datanode.sh node2 9002

# 终端 3
./start-datanode.sh node3 9003
```

### 5. 启动协调节点

```bash
./start-coordinator.sh 8080 localhost:2181 2
# 参数: port  zk-connect  replication-factor
```

### 6. 启动客户端

```bash
./start-client.sh localhost 8080
```

---

## 支持的 SQL 操作

| 操作              | 说明                                 |
|-------------------|--------------------------------------|
| `CREATE DATABASE` | 广播到所有节点，ZK 注册元数据         |
| `DROP DATABASE`   | 广播到所有节点，ZK 删除元数据         |
| `SHOW DATABASES`  | 直接从 ZooKeeper 元数据返回           |
| `USE database`    | 客户端会话级切换                      |
| `CREATE TABLE`    | 广播到所有节点，ZK 记录分片信息       |
| `DROP TABLE`      | 广播到所有节点，ZK 删除分片信息       |
| `SHOW TABLES`     | 直接从 ZooKeeper 元数据返回           |
| `CREATE INDEX`    | 广播到所有节点                        |
| `INSERT`          | 按主键哈希路由到指定分片主节点         |
| `SELECT`          | 有主键等值条件→单分片；否则→全分片扇出 |
| `UPDATE`          | 有主键等值条件→单分片；否则→全节点扇出 |
| `DELETE`          | 有主键等值条件→单分片；否则→全节点扇出 |
| `SELECT ... JOIN` | 协调节点拉取两表数据，内存哈希 JOIN   |

---

## 命令行参数

### DataNode

| 参数                     | 说明                         | 默认值        |
|--------------------------|------------------------------|---------------|
| `--node-id=<id>`         | 节点唯一标识                 | `host:port`   |
| `--host=<host>`          | 监听地址                     | 必填          |
| `--port=<port>`          | 监听端口                     | 必填          |
| `--zk=<zk-connect>`      | ZooKeeper 连接串             | 必填          |
| `--minisql=<path>`       | minisql 二进制路径           | 必填          |
| `--data-dir=<dir>`       | 数据文件目录                 | 必填          |
| `--replication-factor=n` | 副本数                       | `1`           |

### Coordinator

| 参数                     | 说明                         | 默认值        |
|--------------------------|------------------------------|---------------|
| `--host=<host>`          | 监听地址                     | `0.0.0.0`     |
| `--port=<port>`          | 监听端口                     | `8080`        |
| `--zk=<zk-connect>`      | ZooKeeper 连接串             | 必填          |
| `--replication-factor=n` | 副本数（影响分片分配）       | `1`           |

---

## 技术栈

| 组件                  | 版本    | 用途                          |
|-----------------------|---------|-------------------------------|
| Java                  | 11      | 分布式层主要实现语言           |
| Maven                 | 3.8+    | 项目构建与依赖管理             |
| Apache Curator        | 5.5.0   | ZooKeeper 客户端              |
| Jackson               | 2.15.2  | JSON 序列化/反序列化           |
| JSQLParser            | 4.7     | SQL 语法解析（路由与 JOIN）    |
| Logback               | 1.4.11  | 日志                          |
| ZooKeeper             | 3.8+    | 集群管理、节点注册             |
| MiniSQL (C++)         | -       | 单机 SQL 执行内核              |
