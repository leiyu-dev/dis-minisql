package edu.minisql.distributed.protocol;

import java.util.HashSet;
import java.util.Set;

public class NodeInfo {
    public String nodeId;
    public String host;
    public int port;
    public Set<Integer> shards = new HashSet<>();
    public long lastWalSequence;

    public NodeInfo() {
    }

    public NodeInfo(String nodeId, String host, int port, Set<Integer> shards, long lastWalSequence) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.shards = shards;
        this.lastWalSequence = lastWalSequence;
    }

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}
