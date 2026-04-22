package com.minisql.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Shard assignment for a table partition.
 * Stored at ZkPaths.shard(dbName, tableName, shardId).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardInfo {

    private int shardId;
    private String dbName;
    private String tableName;
    /** ID of the primary (write) node. */
    private String primaryNodeId;
    /** IDs of replica nodes. */
    private List<String> replicaNodeIds;

    public ShardInfo() {}

    public ShardInfo(int shardId, String dbName, String tableName,
                     String primaryNodeId, List<String> replicaNodeIds) {
        this.shardId = shardId;
        this.dbName = dbName;
        this.tableName = tableName;
        this.primaryNodeId = primaryNodeId;
        this.replicaNodeIds = replicaNodeIds;
    }

    public int getShardId() { return shardId; }
    public void setShardId(int shardId) { this.shardId = shardId; }

    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getPrimaryNodeId() { return primaryNodeId; }
    public void setPrimaryNodeId(String primaryNodeId) { this.primaryNodeId = primaryNodeId; }

    public List<String> getReplicaNodeIds() { return replicaNodeIds; }
    public void setReplicaNodeIds(List<String> replicaNodeIds) { this.replicaNodeIds = replicaNodeIds; }
}
