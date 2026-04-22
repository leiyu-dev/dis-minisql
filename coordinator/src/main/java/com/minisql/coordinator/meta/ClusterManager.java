package com.minisql.coordinator.meta;

import com.minisql.common.model.NodeInfo;
import com.minisql.common.model.ShardInfo;
import com.minisql.common.model.TableMeta;
import com.minisql.coordinator.zk.CoordinatorZkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages cluster membership and detects/handles node failures.
 *
 * When a node disappears from ZooKeeper, this class:
 *   1. Marks the node as dead in the live-node set.
 *   2. Promotes the first available replica to primary for every shard
 *      that was served by the failed node.
 */
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final CoordinatorZkManager zk;
    /** nodeId -> NodeInfo (volatile copy of ZK state). */
    private final Map<String, NodeInfo> liveNodes = new ConcurrentHashMap<>();
    /** Ordered list of live node IDs (stable ordering for consistent shard assignment). */
    private final List<String> liveNodeIds = new CopyOnWriteArrayList<>();

    public ClusterManager(CoordinatorZkManager zk) {
        this.zk = zk;
    }

    /** Called once during startup and then by ZK watch callbacks. */
    public synchronized void updateNodes(List<NodeInfo> nodes) {
        Set<String> incoming = new HashSet<>();
        for (NodeInfo n : nodes) incoming.add(n.getNodeId());

        // Detect new nodes
        for (NodeInfo n : nodes) {
            if (!liveNodes.containsKey(n.getNodeId())) {
                log.info("Node joined: {}", n);
            }
            liveNodes.put(n.getNodeId(), n);
        }

        // Detect failed nodes
        for (String id : new ArrayList<>(liveNodes.keySet())) {
            if (!incoming.contains(id)) {
                log.warn("Node left the cluster: {}", id);
                liveNodes.remove(id);
                handleNodeFailure(id);
            }
        }

        // Rebuild ordered list deterministically
        liveNodeIds.clear();
        List<String> sorted = new ArrayList<>(liveNodes.keySet());
        Collections.sort(sorted);
        liveNodeIds.addAll(sorted);
    }

    /** Returns a snapshot of all live nodes. */
    public List<NodeInfo> getLiveNodes() {
        return new ArrayList<>(liveNodes.values());
    }

    /** Returns live node count. */
    public int getLiveNodeCount() {
        return liveNodes.size();
    }

    /** Returns the ordered list of live node IDs. */
    public List<String> getLiveNodeIds() {
        return Collections.unmodifiableList(liveNodeIds);
    }

    /** Returns NodeInfo for the given ID, or null if not alive. */
    public NodeInfo getNode(String nodeId) {
        return liveNodes.get(nodeId);
    }

    public boolean isAlive(String nodeId) {
        return liveNodes.containsKey(nodeId);
    }

    // ------------------------------------------------------------------
    // Failover
    // ------------------------------------------------------------------

    /**
     * For every shard whose primary was the failed node, promote the first
     * available replica.
     */
    private void handleNodeFailure(String failedNodeId) {
        try {
            // Scan all databases and tables for shards affected by this failure
            for (String dbName : zk.listDatabases()) {
                for (String tableName : zk.listTables(dbName)) {
                    for (ShardInfo shard : zk.getAllShards(dbName, tableName)) {
                        if (failedNodeId.equals(shard.getPrimaryNodeId())) {
                            promoteReplica(shard, failedNodeId);
                        }
                        // Also remove from replica list silently
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error during failover for node {}", failedNodeId, e);
        }
    }

    private void promoteReplica(ShardInfo shard, String failedNodeId) throws Exception {
        for (String replicaId : shard.getReplicaNodeIds()) {
            if (isAlive(replicaId)) {
                zk.promoteToPrimary(shard.getDbName(), shard.getTableName(),
                        shard.getShardId(), replicaId);
                log.info("Failover: promoted {} for shard {}/{}/{}", replicaId,
                        shard.getDbName(), shard.getTableName(), shard.getShardId());
                return;
            }
        }
        log.error("No alive replica available for shard {}/{}/{} after failure of {}",
                shard.getDbName(), shard.getTableName(), shard.getShardId(), failedNodeId);
    }
}
