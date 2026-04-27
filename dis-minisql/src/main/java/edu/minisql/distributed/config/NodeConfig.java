package edu.minisql.distributed.config;

public class NodeConfig {
    public String nodeId;
    public String host = "127.0.0.1";
    public int port;
    public String dataDir;

    public NodeConfig() {
    }

    public NodeConfig(String nodeId, String host, int port, String dataDir) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.dataDir = dataDir;
    }

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}
