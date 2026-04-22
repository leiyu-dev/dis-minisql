package com.minisql.coordinator.zk;

import com.minisql.common.model.NodeInfo;
import com.minisql.common.model.ShardInfo;
import com.minisql.common.model.TableMeta;
import com.minisql.common.util.JsonUtil;
import com.minisql.common.zk.ZkPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * ZooKeeper façade used by the Coordinator.
 *
 * Responsibilities:
 *   - Watch the live set of data nodes (ephemeral znodes under /dis-minisql/nodes).
 *   - Read/write table metadata and shard assignments.
 *   - Register the coordinator itself as an ephemeral node (for HA awareness).
 */
public class CoordinatorZkManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorZkManager.class);

    private final CuratorFramework client;
    private CuratorCache nodeCache;
    private Consumer<List<NodeInfo>> nodeChangeListener;

    public CoordinatorZkManager(String zkConnectString, int sessionMs, int connMs) {
        this.client = CuratorFrameworkFactory.builder()
                .connectString(zkConnectString)
                .sessionTimeoutMs(sessionMs)
                .connectionTimeoutMs(connMs)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    public void start() throws Exception {
        client.start();
        client.blockUntilConnected();
        log.info("Coordinator connected to ZooKeeper");

        ensurePersistent(ZkPaths.ROOT);
        ensurePersistent(ZkPaths.NODES);
        ensurePersistent(ZkPaths.DATABASES);
        ensurePersistent(ZkPaths.TABLES);
        ensurePersistent(ZkPaths.SHARDS);

        startNodeWatch();
    }

    public void setNodeChangeListener(Consumer<List<NodeInfo>> listener) {
        this.nodeChangeListener = listener;
    }

    // ------------------------------------------------------------------
    // Node registry
    // ------------------------------------------------------------------

    public List<NodeInfo> getLiveNodes() {
        List<NodeInfo> nodes = new ArrayList<>();
        try {
            List<String> children = client.getChildren().forPath(ZkPaths.NODES);
            for (String child : children) {
                byte[] data = client.getData().forPath(ZkPaths.NODES + "/" + child);
                if (data != null && data.length > 0) {
                    nodes.add(JsonUtil.fromBytes(data, NodeInfo.class));
                }
            }
        } catch (Exception e) {
            log.error("Failed to read live nodes", e);
        }
        return nodes;
    }

    // ------------------------------------------------------------------
    // Database registry
    // ------------------------------------------------------------------

    public void registerDatabase(String dbName) throws Exception {
        String path = ZkPaths.database(dbName);
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(path, dbName.getBytes());
        }
    }

    public void deleteDatabase(String dbName) throws Exception {
        String path = ZkPaths.database(dbName);
        if (client.checkExists().forPath(path) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        }
        // Also remove table/shard metadata
        String tableRoot = ZkPaths.tableRoot(dbName);
        if (client.checkExists().forPath(tableRoot) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(tableRoot);
        }
        String shardRoot = ZkPaths.SHARDS + "/" + dbName;
        if (client.checkExists().forPath(shardRoot) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(shardRoot);
        }
    }

    public List<String> listDatabases() throws Exception {
        if (client.checkExists().forPath(ZkPaths.DATABASES) == null) return new ArrayList<>();
        return client.getChildren().forPath(ZkPaths.DATABASES);
    }

    // ------------------------------------------------------------------
    // Table metadata
    // ------------------------------------------------------------------

    public void saveTableMeta(TableMeta meta) throws Exception {
        String path = ZkPaths.table(meta.getDbName(), meta.getTableName());
        ensurePersistent(ZkPaths.tableRoot(meta.getDbName()));
        byte[] data = JsonUtil.toBytes(meta);
        if (client.checkExists().forPath(path) == null) {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path, data);
        } else {
            client.setData().forPath(path, data);
        }
    }

    public TableMeta getTableMeta(String dbName, String tableName) {
        try {
            String path = ZkPaths.table(dbName, tableName);
            if (client.checkExists().forPath(path) == null) return null;
            byte[] data = client.getData().forPath(path);
            return JsonUtil.fromBytes(data, TableMeta.class);
        } catch (Exception e) {
            log.error("Failed to get table meta for {}.{}", dbName, tableName, e);
            return null;
        }
    }

    public void deleteTableMeta(String dbName, String tableName) throws Exception {
        String path = ZkPaths.table(dbName, tableName);
        if (client.checkExists().forPath(path) != null) {
            client.delete().forPath(path);
        }
        // Remove shard info
        String shardRoot = ZkPaths.shardRoot(dbName, tableName);
        if (client.checkExists().forPath(shardRoot) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(shardRoot);
        }
    }

    public List<String> listTables(String dbName) throws Exception {
        String path = ZkPaths.tableRoot(dbName);
        if (client.checkExists().forPath(path) == null) return new ArrayList<>();
        return client.getChildren().forPath(path);
    }

    // ------------------------------------------------------------------
    // Shard metadata
    // ------------------------------------------------------------------

    public void saveShardInfo(ShardInfo shard) throws Exception {
        String path = ZkPaths.shard(shard.getDbName(), shard.getTableName(), shard.getShardId());
        ensurePersistent(ZkPaths.shardRoot(shard.getDbName(), shard.getTableName()));
        byte[] data = JsonUtil.toBytes(shard);
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT).forPath(path, data);
        } else {
            client.setData().forPath(path, data);
        }
    }

    public ShardInfo getShardInfo(String dbName, String tableName, int shardId) {
        try {
            String path = ZkPaths.shard(dbName, tableName, shardId);
            if (client.checkExists().forPath(path) == null) return null;
            return JsonUtil.fromBytes(client.getData().forPath(path), ShardInfo.class);
        } catch (Exception e) {
            log.error("Failed to get shard info for {}.{} shard {}", dbName, tableName, shardId, e);
            return null;
        }
    }

    public List<ShardInfo> getAllShards(String dbName, String tableName) {
        List<ShardInfo> shards = new ArrayList<>();
        try {
            String root = ZkPaths.shardRoot(dbName, tableName);
            if (client.checkExists().forPath(root) == null) return shards;
            for (String child : client.getChildren().forPath(root)) {
                byte[] data = client.getData().forPath(root + "/" + child);
                if (data != null) shards.add(JsonUtil.fromBytes(data, ShardInfo.class));
            }
        } catch (Exception e) {
            log.error("Failed to list shards for {}.{}", dbName, tableName, e);
        }
        return shards;
    }

    // ------------------------------------------------------------------
    // Failover: promote a replica to primary
    // ------------------------------------------------------------------

    public void promoteToPrimary(String dbName, String tableName, int shardId, String newPrimaryId) throws Exception {
        ShardInfo shard = getShardInfo(dbName, tableName, shardId);
        if (shard == null) return;
        List<String> replicas = new ArrayList<>(shard.getReplicaNodeIds());
        replicas.remove(newPrimaryId);
        replicas.add(shard.getPrimaryNodeId()); // old primary may rejoin later
        shard.setPrimaryNodeId(newPrimaryId);
        shard.setReplicaNodeIds(replicas);
        saveShardInfo(shard);
        log.info("Promoted {} to primary for {}.{} shard {}", newPrimaryId, dbName, tableName, shardId);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void startNodeWatch() {
        nodeCache = CuratorCache.build(client, ZkPaths.NODES);
        nodeCache.listenable().addListener(
                CuratorCacheListener.builder()
                        .forCreates(n -> notifyNodeChange())
                        .forDeletes(n -> notifyNodeChange())
                        .forChanges((o, n) -> notifyNodeChange())
                        .build());
        nodeCache.start();
    }

    private void notifyNodeChange() {
        if (nodeChangeListener != null) {
            nodeChangeListener.accept(getLiveNodes());
        }
    }

    private void ensurePersistent(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(path);
        }
    }

    @Override
    public void close() {
        if (nodeCache != null) nodeCache.close();
        client.close();
    }
}
