package com.minisql.common.zk;

/**
 * Central definition of all ZooKeeper paths used by the distributed MiniSQL cluster.
 *
 * Layout:
 *   /dis-minisql/
 *     nodes/{nodeId}                        - ephemeral; NodeInfo JSON
 *     coordinator                           - ephemeral; host:port of active coordinator
 *     databases/{dbName}                    - persistent; marks DB exists
 *     tables/{dbName}/{tableName}           - persistent; TableMeta JSON
 *     shards/{dbName}/{tableName}/{shardId} - persistent; ShardInfo JSON
 */
public final class ZkPaths {

    private ZkPaths() {}

    public static final String ROOT              = "/dis-minisql";
    public static final String NODES             = ROOT + "/nodes";
    public static final String COORDINATOR       = ROOT + "/coordinator";
    public static final String DATABASES         = ROOT + "/databases";
    public static final String TABLES            = ROOT + "/tables";
    public static final String SHARDS            = ROOT + "/shards";

    public static String node(String nodeId) {
        return NODES + "/" + nodeId;
    }

    public static String database(String dbName) {
        return DATABASES + "/" + dbName;
    }

    public static String tableRoot(String dbName) {
        return TABLES + "/" + dbName;
    }

    public static String table(String dbName, String tableName) {
        return TABLES + "/" + dbName + "/" + tableName;
    }

    public static String shardRoot(String dbName, String tableName) {
        return SHARDS + "/" + dbName + "/" + tableName;
    }

    public static String shard(String dbName, String tableName, int shardId) {
        return SHARDS + "/" + dbName + "/" + tableName + "/" + shardId;
    }
}
