package edu.minisql.distributed.protocol;

public class ExecuteRequest {
    public String requestId;
    public int shardId;
    public long shardLogIndex;
    public long raftTerm;
    public String sql;
    public long walSequence;
    public boolean replay;

    public ExecuteRequest() {
    }

    public ExecuteRequest(String requestId, int shardId, long shardLogIndex, String sql, long walSequence, boolean replay) {
        this.requestId = requestId;
        this.shardId = shardId;
        this.shardLogIndex = shardLogIndex;
        this.sql = sql;
        this.walSequence = walSequence;
        this.replay = replay;
    }

    public ExecuteRequest(String requestId, int shardId, long shardLogIndex, long raftTerm,
                          String sql, long walSequence, boolean replay) {
        this(requestId, shardId, shardLogIndex, sql, walSequence, replay);
        this.raftTerm = raftTerm;
    }
}
