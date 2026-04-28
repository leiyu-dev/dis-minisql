package edu.minisql.distributed.protocol;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class NodeInfo {
    public String nodeId;
    public String host;
    public int port;
    public Set<Integer> shards = new HashSet<>();
    public long lastWalSequence;
    public String replicaState = "SERVING";
    public Map<Integer, Long> shardLogIndexes = new TreeMap<>();

    public NodeInfo() {
    }

    public NodeInfo(String nodeId, String host, int port, Set<Integer> shards, long lastWalSequence,
                    String replicaState, Map<Integer, Long> shardLogIndexes) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.shards = shards;
        this.lastWalSequence = lastWalSequence;
        this.replicaState = replicaState;
        this.shardLogIndexes = shardLogIndexes;
    }

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}
