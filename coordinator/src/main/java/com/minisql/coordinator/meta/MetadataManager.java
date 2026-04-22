package com.minisql.coordinator.meta;

import com.minisql.common.model.ShardInfo;
import com.minisql.common.model.TableMeta;
import com.minisql.coordinator.zk.CoordinatorZkManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages table and shard metadata in ZooKeeper.
 *
 * When a CREATE TABLE statement arrives:
 *   1. Parses the schema to find the primary-key column (used as shard key).
 *   2. Creates one shard per live node (hash-based partitioning).
 *   3. Assigns the i-th shard's primary to nodes[i % n] and replicates to
 *      the next {@code replicationFactor - 1} nodes.
 *   4. Persists the TableMeta and all ShardInfo objects in ZooKeeper.
 */
public class MetadataManager {

    private static final Logger log = LoggerFactory.getLogger(MetadataManager.class);

    private final CoordinatorZkManager zk;
    private final ClusterManager clusterManager;
    private final int replicationFactor;

    public MetadataManager(CoordinatorZkManager zk, ClusterManager clusterManager, int replicationFactor) {
        this.zk = zk;
        this.clusterManager = clusterManager;
        this.replicationFactor = replicationFactor;
    }

    // ------------------------------------------------------------------
    // Table registration
    // ------------------------------------------------------------------

    /**
     * Parses {@code sql} (a CREATE TABLE statement), builds TableMeta and
     * ShardInfo objects, and persists them in ZooKeeper.
     */
    public TableMeta registerTable(String dbName, String sql) throws Exception {
        Statement stmt = CCJSqlParserUtil.parse(sql);
        if (!(stmt instanceof CreateTable)) {
            throw new IllegalArgumentException("Expected CREATE TABLE statement");
        }
        CreateTable ct = (CreateTable) stmt;
        String tableName = ct.getTable().getName().toLowerCase();

        List<String> columns = ct.getColumnDefinitions().stream()
                .map(c -> c.getColumnName().toLowerCase())
                .collect(Collectors.toList());

        // Find primary key column
        String shardKey = findPrimaryKey(ct);
        int shardKeyIndex = shardKey != null ? columns.indexOf(shardKey.toLowerCase()) : 0;
        if (shardKeyIndex < 0) shardKeyIndex = 0;

        List<String> nodeIds = clusterManager.getLiveNodeIds();
        int numShards = Math.max(1, nodeIds.size());

        TableMeta meta = new TableMeta();
        meta.setDbName(dbName);
        meta.setTableName(tableName);
        meta.setShardKey(shardKey != null ? shardKey.toLowerCase() : columns.get(0));
        meta.setShardKeyIndex(shardKeyIndex);
        meta.setNumShards(numShards);
        meta.setColumns(columns);
        meta.setCreateSql(sql);

        zk.saveTableMeta(meta);

        // Create shard assignments
        for (int shardId = 0; shardId < numShards; shardId++) {
            String primaryId = nodeIds.get(shardId % nodeIds.size());
            List<String> replicas = buildReplicaList(nodeIds, shardId);
            ShardInfo shard = new ShardInfo(shardId, dbName, tableName, primaryId, replicas);
            zk.saveShardInfo(shard);
        }

        log.info("Registered table {}.{} with {} shards, shard key={}",
                dbName, tableName, numShards, meta.getShardKey());
        return meta;
    }

    public void deregisterTable(String dbName, String tableName) throws Exception {
        zk.deleteTableMeta(dbName, tableName);
        log.info("Deregistered table {}.{}", dbName, tableName);
    }

    public void registerDatabase(String dbName) throws Exception {
        zk.registerDatabase(dbName);
    }

    public void deregisterDatabase(String dbName) throws Exception {
        zk.deleteDatabase(dbName);
    }

    // ------------------------------------------------------------------
    // Metadata retrieval
    // ------------------------------------------------------------------

    public TableMeta getTableMeta(String dbName, String tableName) {
        return zk.getTableMeta(dbName, tableName);
    }

    public List<ShardInfo> getAllShards(String dbName, String tableName) {
        return zk.getAllShards(dbName, tableName);
    }

    public ShardInfo getShardForKey(String dbName, String tableName, String keyValue) {
        TableMeta meta = zk.getTableMeta(dbName, tableName);
        if (meta == null) return null;
        int shardId = Math.abs(keyValue.hashCode()) % meta.getNumShards();
        return zk.getShardInfo(dbName, tableName, shardId);
    }

    public List<String> listDatabases() throws Exception {
        return zk.listDatabases();
    }

    public List<String> listTables(String dbName) throws Exception {
        return zk.listTables(dbName);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private String findPrimaryKey(CreateTable ct) {
        // Check column-level PRIMARY KEY spec
        if (ct.getColumnDefinitions() != null) {
            for (ColumnDefinition col : ct.getColumnDefinitions()) {
                List<String> specs = col.getColumnSpecs();
                if (specs != null) {
                    String joined = String.join(" ", specs).toUpperCase();
                    if (joined.contains("PRIMARY") && joined.contains("KEY")) {
                        return col.getColumnName();
                    }
                }
            }
        }
        // Check table-level PRIMARY KEY constraint
        if (ct.getIndexes() != null) {
            for (Index idx : ct.getIndexes()) {
                if ("PRIMARY KEY".equalsIgnoreCase(idx.getType()) && !idx.getColumnsNames().isEmpty()) {
                    return idx.getColumnsNames().get(0);
                }
            }
        }
        return null;
    }

    private List<String> buildReplicaList(List<String> nodeIds, int shardId) {
        List<String> replicas = new ArrayList<>();
        int n = nodeIds.size();
        for (int r = 1; r < replicationFactor && r < n; r++) {
            replicas.add(nodeIds.get((shardId + r) % n));
        }
        return replicas;
    }
}
