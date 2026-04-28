package edu.minisql.distributed.datanode;

import com.sun.net.httpserver.HttpServer;
import edu.minisql.distributed.common.HttpUtil;
import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.common.SqlUtils;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.config.NodeConfig;
import edu.minisql.distributed.minisql.MiniSqlCli;
import edu.minisql.distributed.protocol.ExecuteRequest;
import edu.minisql.distributed.protocol.ExecuteResponse;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.zk.ZkMetadataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DataNodeServer {
    private final ClusterConfig clusterConfig;
    private final NodeConfig nodeConfig;
    private final ZkMetadataStore metadataStore;
    private final WalLog walLog;
    private final SnapshotManager snapshotManager;
    private final MiniSqlCli miniSql;
    private final Set<Integer> shards;
    private volatile String replicaState = "RECOVERING";
    private SnapshotManifest snapshotManifest;

    public DataNodeServer(ClusterConfig clusterConfig, String nodeId) {
        this.clusterConfig = clusterConfig;
        this.nodeConfig = clusterConfig.requireNode(nodeId);
        this.metadataStore = new ZkMetadataStore(clusterConfig);
        this.metadataStore.initializeShards(clusterConfig);
        this.shards = metadataStore.shardsForNode(nodeId);
        Path dataDir = Path.of(nodeConfig.dataDir == null ? "data/" + nodeId : nodeConfig.dataDir);
        this.walLog = new WalLog(dataDir);
        this.snapshotManager = new SnapshotManager(dataDir);
        this.miniSql = new MiniSqlCli(Path.of(clusterConfig.minisqlBinary), dataDir, Duration.ofSeconds(30));
    }

    public void start() throws IOException {
        snapshotManifest = snapshotManager.loadManifest();
        snapshotManager.restoreIfNeeded();
        register();
        recoverFromPeers();
        applyPendingWal();
        maybeSnapshot();
        replicaState = "SERVING";
        register();
        HttpServer server = HttpServer.create(new InetSocketAddress(nodeConfig.host, nodeConfig.port), 0);
        server.createContext("/execute", exchange -> {
            try {
                ExecuteRequest request = Jsons.parse(HttpUtil.body(exchange), ExecuteRequest.class);
                ExecuteResponse response = execute(request);
                HttpUtil.json(exchange, response.ok ? 200 : 500, response);
            } catch (Exception e) {
                HttpUtil.json(exchange, 500, ExecuteResponse.error(nodeConfig.nodeId, e.getMessage()));
            }
        });
        server.createContext("/wal", exchange -> HttpUtil.json(exchange, 200, walLog.readAll()));
        server.createContext("/health", exchange -> HttpUtil.json(exchange, 200, currentNodeInfo()));
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        System.out.printf("DataNode %s serving %s shards %s at %s:%d%n",
                nodeConfig.nodeId, clusterConfig.clusterName, shards, nodeConfig.host, nodeConfig.port);
    }

    private synchronized ExecuteResponse execute(ExecuteRequest request) {
        String sql = SqlUtils.normalize(request.sql);
        WalEntry written = null;
        if (!SqlUtils.isReadOnly(sql) && !request.replay) {
            if (!ownsShard(request.shardId)) {
                return ExecuteResponse.error(nodeConfig.nodeId, "node does not own shard " + request.shardId);
            }
            written = walLog.append(request.requestId, request.shardId, request.shardLogIndex, sql);
            metadataStore.updateNode(currentNodeInfo());
        } else if (request.replay && walLog.containsRequestId(request.requestId)) {
            return ExecuteResponse.ok(nodeConfig.nodeId, walLog.lastSequence(), "duplicate replay ignored");
        } else if (request.replay) {
            written = walLog.appendRecovered(new WalEntry(0, request.requestId, request.shardId,
                    request.shardLogIndex, sql, System.currentTimeMillis()));
        }

        long sequence = written == null ? walLog.lastSequence() : written.sequence;
        String output = miniSql.execute(List.of(), sql);
        if (written != null) {
            snapshotManifest = snapshotManager.markApplied(snapshotManifest, sequence, currentShardLogIndexes());
        }
        maybeSnapshot();
        metadataStore.updateNode(currentNodeInfo());
        ExecuteResponse response = ExecuteResponse.ok(nodeConfig.nodeId, sequence, output);
        response.shardLogIndex = written == null ? request.shardLogIndex : written.shardLogIndex;
        return response;
    }

    private void recoverFromPeers() {
        for (NodeInfo peer : metadataStore.liveNodes()) {
            if (peer.nodeId.equals(nodeConfig.nodeId) || java.util.Collections.disjoint(peer.shards, shards)) {
                continue;
            }
            try {
                WalEntry[] peerWal = HttpUtil.getJson(peer.baseUrl() + "/wal", WalEntry[].class);
                Arrays.stream(peerWal)
                        .sorted(Comparator.comparingLong(entry -> entry.sequence))
                        .filter(entry -> ownsShard(entry.shardId))
                        .filter(entry -> !walLog.containsRequestId(entry.requestId))
                        .forEach(walLog::appendRecovered);
            } catch (Exception e) {
                System.err.printf("Skip WAL recovery from %s: %s%n", peer.nodeId, e.getMessage());
            }
        }
    }

    private boolean ownsShard(int shardId) {
        return shardId < 0 || shards.contains(shardId);
    }

    private void applyPendingWal() {
        List<String> pendingSql = walLog.pendingAfter(snapshotManifest.lastWalSequence).stream()
                .map(entry -> entry.sql)
                .collect(Collectors.toList());
        if (!pendingSql.isEmpty()) {
            miniSql.execute(pendingSql, null);
            snapshotManifest = snapshotManager.markApplied(snapshotManifest, walLog.lastSequence(), currentShardLogIndexes());
        }
    }

    private void maybeSnapshot() {
        long compactThrough = walLog.lastSequence();
        if (snapshotManifest != null && compactThrough - snapshotManifest.lastCompactedWalSequence < 20) {
            return;
        }
        snapshotManifest = snapshotManager.createSnapshot(compactThrough, currentShardLogIndexes());
        walLog.compactThrough(compactThrough);
    }

    private void register() {
        metadataStore.registerNode(currentNodeInfo());
    }

    private NodeInfo currentNodeInfo() {
        return new NodeInfo(nodeConfig.nodeId, nodeConfig.host, nodeConfig.port, shards, walLog.lastSequence(),
                replicaState, currentShardLogIndexes());
    }

    private Map<Integer, Long> currentShardLogIndexes() {
        Map<Integer, Long> indexes = new TreeMap<>();
        if (snapshotManifest != null) {
            indexes.putAll(snapshotManifest.shardLogIndexes);
        }
        for (Map.Entry<Integer, Long> entry : walLog.maxShardLogIndexes().entrySet()) {
            indexes.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return indexes;
    }
}
