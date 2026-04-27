package edu.minisql.distributed.zk;

public final class ZkPaths {
    private final String root;

    public ZkPaths(String clusterName) {
        this.root = "/" + clusterName;
    }

    public String root() {
        return root;
    }

    public String nodes() {
        return root + "/nodes";
    }

    public String node(String nodeId) {
        return nodes() + "/" + nodeId;
    }

    public String shards() {
        return root + "/shards";
    }

    public String shard(int shardId) {
        return shards() + "/" + shardId;
    }
}
