package edu.minisql.distributed.protocol;

public class ExecuteResponse {
    public boolean ok;
    public String nodeId;
    public long walSequence;
    public long shardLogIndex;
    public long raftTerm;
    public String output;
    public String error;

    public ExecuteResponse() {
    }

    public static ExecuteResponse ok(String nodeId, long walSequence, String output) {
        ExecuteResponse response = new ExecuteResponse();
        response.ok = true;
        response.nodeId = nodeId;
        response.walSequence = walSequence;
        response.output = output;
        return response;
    }

    public static ExecuteResponse error(String nodeId, String error) {
        ExecuteResponse response = new ExecuteResponse();
        response.ok = false;
        response.nodeId = nodeId;
        response.error = error;
        return response;
    }
}
