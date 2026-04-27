package edu.minisql.distributed.protocol;

public class ExecuteRequest {
    public String requestId;
    public int shardId;
    public String sql;
    public long walSequence;
    public boolean replay;

    public ExecuteRequest() {
    }

    public ExecuteRequest(String requestId, int shardId, String sql, long walSequence, boolean replay) {
        this.requestId = requestId;
        this.shardId = shardId;
        this.sql = sql;
        this.walSequence = walSequence;
        this.replay = replay;
    }
}
