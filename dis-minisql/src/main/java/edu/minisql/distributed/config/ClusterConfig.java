package edu.minisql.distributed.config;

import edu.minisql.distributed.common.Jsons;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ClusterConfig {
    public String clusterName = "dis-minisql";
    public String zkConnect = "127.0.0.1:2181";
    public int sessionTimeoutMs = 15000;
    public int shardCount = 3;
    public int replicationFactor = 2;
    public String coordinatorHost = "0.0.0.0";
    public int coordinatorPort = 8080;
    public String minisqlBinary = "../minisql/build/bin/main";
    public List<NodeConfig> nodes = new ArrayList<>();

    public static ClusterConfig load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return Jsons.MAPPER.readValue(in, ClusterConfig.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load config: " + path, e);
        }
    }

    public NodeConfig requireNode(String nodeId) {
        return nodes.stream()
                .filter(node -> node.nodeId.equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown nodeId in config: " + nodeId));
    }
}
