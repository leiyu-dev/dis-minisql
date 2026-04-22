package com.minisql.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Metadata about a registered data node, stored in ZooKeeper as JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeInfo {

    private String nodeId;
    private String host;
    private int port;
    /** Millisecond timestamp of last heartbeat. */
    private long lastHeartbeat;
    private NodeStatus status;

    public enum NodeStatus { ALIVE, DEAD }

    public NodeInfo() {}

    public NodeInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.status = NodeStatus.ALIVE;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }

    @Override
    public String toString() {
        return nodeId + "@" + host + ":" + port + " [" + status + "]";
    }
}
