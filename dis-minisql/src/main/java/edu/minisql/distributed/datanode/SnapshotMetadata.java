package edu.minisql.distributed.datanode;

import java.util.HashMap;
import java.util.Map;

public class SnapshotMetadata {
    public long createdAtMillis;
    public Map<Integer, Long> shardLogIndexes = new HashMap<>();
    public int statementCount;

    public SnapshotMetadata() {
    }

    public SnapshotMetadata(long createdAtMillis, Map<Integer, Long> shardLogIndexes, int statementCount) {
        this.createdAtMillis = createdAtMillis;
        this.shardLogIndexes = shardLogIndexes;
        this.statementCount = statementCount;
    }
}
