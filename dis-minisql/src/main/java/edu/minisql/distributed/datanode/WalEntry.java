package edu.minisql.distributed.datanode;

public class WalEntry {
    public long sequence;
    public String requestId;
    public int shardId;
    public String sql;
    public long timestampMillis;

    public WalEntry() {
    }

    public WalEntry(long sequence, String requestId, int shardId, String sql, long timestampMillis) {
        this.sequence = sequence;
        this.requestId = requestId;
        this.shardId = shardId;
        this.sql = sql;
        this.timestampMillis = timestampMillis;
    }
}
