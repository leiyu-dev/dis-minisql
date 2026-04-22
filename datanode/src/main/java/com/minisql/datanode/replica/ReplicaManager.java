package com.minisql.datanode.replica;

import com.minisql.common.model.NodeInfo;
import com.minisql.common.model.ShardInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages outbound replication: when this node is the primary for a shard,
 * it forwards write statements to all replica nodes for that shard.
 */
public class ReplicaManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);

    private final String myNodeId;
    /** nodeId -> ReplicaClient */
    private final Map<String, ReplicaClient> clients = new ConcurrentHashMap<>();
    /** nodeId -> NodeInfo */
    private final Map<String, NodeInfo> knownNodes = new ConcurrentHashMap<>();

    public ReplicaManager(String myNodeId) {
        this.myNodeId = myNodeId;
    }

    /** Called by ClusterManager whenever node list changes. */
    public void updateNodes(List<NodeInfo> nodes) {
        for (NodeInfo node : nodes) {
            if (!node.getNodeId().equals(myNodeId)) {
                knownNodes.put(node.getNodeId(), node);
                clients.computeIfAbsent(node.getNodeId(),
                        id -> new ReplicaClient(node.getHost(), node.getPort()));
            }
        }
        // Remove stale entries
        knownNodes.keySet().retainAll(
                nodes.stream().map(NodeInfo::getNodeId)
                        .filter(id -> !id.equals(myNodeId))
                        .collect(java.util.stream.Collectors.toSet()));
        clients.keySet().retainAll(knownNodes.keySet());
    }

    /**
     * Forwards a write statement to all replica nodes listed in {@code shardInfo}.
     * Replication is asynchronous (fire-and-forget). If a replica is unavailable,
     * the write to the primary has already succeeded; the replica will catch up
     * when it recovers (or be replaced via ZooKeeper failover).
     */
    public void replicateWrite(ShardInfo shardInfo, String database, String sql) {
        if (shardInfo == null || shardInfo.getReplicaNodeIds() == null) return;

        for (String replicaId : shardInfo.getReplicaNodeIds()) {
            ReplicaClient client = clients.get(replicaId);
            if (client != null) {
                log.debug("Replicating to {} [shard={}]: {}", replicaId, shardInfo.getShardId(), sql);
                client.replicateAsync(database, sql);
            } else {
                log.warn("No client for replica node {}", replicaId);
            }
        }
    }

    /**
     * Replicates a write to every known peer node.
     * Used for DDL statements that must be present on all nodes.
     */
    public void broadcastWrite(String database, String sql) {
        List<ReplicaClient> all = new ArrayList<>(clients.values());
        for (ReplicaClient c : all) {
            c.replicateAsync(database, sql);
        }
    }
}
