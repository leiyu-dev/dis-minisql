package com.minisql.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Table-level metadata stored in ZooKeeper.
 * Includes schema info needed for shard key resolution.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMeta {

    private String dbName;
    private String tableName;
    /** Column used for hash-based sharding (typically the primary key). */
    private String shardKey;
    /** Index (0-based) of the shard key in the column list. */
    private int shardKeyIndex;
    /** Number of shards (= number of nodes at table-creation time). */
    private int numShards;
    /** Ordered list of column names (for JOIN and row parsing). */
    private List<String> columns;
    /** Original CREATE TABLE SQL, replicated to every data node. */
    private String createSql;

    public TableMeta() {}

    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getShardKey() { return shardKey; }
    public void setShardKey(String shardKey) { this.shardKey = shardKey; }

    public int getShardKeyIndex() { return shardKeyIndex; }
    public void setShardKeyIndex(int shardKeyIndex) { this.shardKeyIndex = shardKeyIndex; }

    public int getNumShards() { return numShards; }
    public void setNumShards(int numShards) { this.numShards = numShards; }

    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }

    public String getCreateSql() { return createSql; }
    public void setCreateSql(String createSql) { this.createSql = createSql; }
}
