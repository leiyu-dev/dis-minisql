package edu.minisql.distributed.datanode;

public class WalEntry {
    public long sequence;
    public String requestId;
    public int shardId;
    public long shardLogIndex;
    public String sql;
    public long timestampMillis;

    public WalEntry() {
    }

    public WalEntry(long sequence, String requestId, int shardId, long shardLogIndex, String sql, long timestampMillis) {
        this.sequence = sequence;
        this.requestId = requestId;
        this.shardId = shardId;
        this.shardLogIndex = shardLogIndex;
        this.sql = sql;
        this.timestampMillis = timestampMillis;
    }
}
