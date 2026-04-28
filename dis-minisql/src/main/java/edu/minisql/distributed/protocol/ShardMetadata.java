package edu.minisql.distributed.protocol;

import java.util.ArrayList;
import java.util.List;

public class ShardMetadata {
    public int shardId;
    public List<String> replicas = new ArrayList<>();
    public String primary;
    public long term;
    public long commitIndex;

    public ShardMetadata() {
    }

    public ShardMetadata(int shardId, List<String> replicas, String primary) {
        this.shardId = shardId;
        this.replicas = replicas;
        this.primary = primary;
        this.term = 1;
        this.commitIndex = 0;
    }
}
